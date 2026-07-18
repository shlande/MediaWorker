package ingest

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestDashIngester_ContentType(t *testing.T) {
	d := NewDashIngester("ffmpeg", t.TempDir())
	if ct := d.ContentType(); ct != "dash_video" {
		t.Errorf("ContentType() = %s, want dash_video", ct)
	}
}

func TestDashIngester_Process_WithFFmpeg(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg not in PATH")
	}

	tmpDir := t.TempDir()
	workDir := tmpDir + "/work"
	if err := os.MkdirAll(workDir, 0755); err != nil {
		t.Fatal(err)
	}

	sampleMP4 := generateTestMP4(t)
	d := NewDashIngester("ffmpeg", workDir)

	input, err := os.Open(sampleMP4)
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()

	result, err := d.Process(context.Background(), input, ProcessOptions{
		ContentID: "test-dash-content",
	})
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	if result.ContentType != "dash_video" {
		t.Errorf("ContentType = %s, want dash_video", result.ContentType)
	}

	if len(result.Blobs) == 0 {
		t.Fatal("expected at least one blob (init segments)")
	}
	if len(result.Roles) == 0 {
		t.Fatal("expected at least one role")
	}
	if len(result.Blobs) != len(result.Roles) {
		t.Errorf("len(Blobs)=%d != len(Roles)=%d", len(result.Blobs), len(result.Roles))
	}

	for i, blob := range result.Blobs {
		if !strings.HasPrefix(blob.BlobHash, "sha256:") {
			t.Errorf("blob[%d].BlobHash = %s, want sha256: prefix", i, blob.BlobHash)
		}

		switch blob.BlobType {
		case "mp4_init_segment", "m4s_media_segment":
		default:
			t.Errorf("blob[%d].BlobType = %s, want mp4_init_segment or m4s_media_segment", i, blob.BlobType)
		}

		role := result.Roles[i]
		if role.Role != "init" && role.Role != "media" {
			t.Errorf("role[%d].Role = %s, want init or media", i, role.Role)
		}

		if role.BlobHash != blob.BlobHash {
			t.Errorf("role[%d].BlobHash != blob[%d].BlobHash", i, i)
		}

		if role.Role == "media" {
			if _, ok := role.BusinessMeta["segment_number"]; !ok {
				t.Errorf("role[%d] media role missing segment_number in BusinessMeta", i)
			}
			if _, ok := role.BusinessMeta["representation_id"]; !ok {
				t.Errorf("role[%d] media role missing representation_id in BusinessMeta", i)
			}
		}

		if role.Role == "init" {
			if _, ok := role.BusinessMeta["representation_id"]; !ok {
				t.Errorf("role[%d] init role missing representation_id in BusinessMeta", i)
			}
		}
	}

	for _, blob := range result.Blobs {
		if _, ok := result.BlobFiles[blob.BlobHash]; !ok {
			t.Errorf("BlobFiles missing entry for hash %s", blob.BlobHash)
		}
	}

	var typeMeta map[string]any
	if err := json.Unmarshal(result.TypeMetadata, &typeMeta); err != nil {
		t.Fatalf("TypeMetadata is not valid JSON: %v\nraw: %s", err, string(result.TypeMetadata))
	}
	if _, ok := typeMeta["mpd_xml"]; !ok {
		t.Error("TypeMetadata missing mpd_xml key")
	}
}

func TestDashIngester_scanDashOutput_RegexParsing(t *testing.T) {
	tmpDir := t.TempDir()
	d := NewDashIngester("ffmpeg", tmpDir)

	mustWriteFile(t, filepath.Join(tmpDir, "init_720p.m4s"), []byte("init data 720p"))
	mustWriteFile(t, filepath.Join(tmpDir, "seg_720p_1.m4s"), []byte("seg data 720p-1"))
	mustWriteFile(t, filepath.Join(tmpDir, "seg_720p_2.m4s"), []byte("seg data 720p-2"))
	mustWriteFile(t, filepath.Join(tmpDir, "init_1080p.m4s"), []byte("init data 1080p"))

	blobs, roles, blobFiles, durationS, err := d.scanDashOutput(tmpDir)
	if err != nil {
		t.Fatalf("scanDashOutput failed: %v", err)
	}

	if len(blobs) != 4 {
		t.Errorf("len(blobs) = %d, want 4", len(blobs))
	}
	if len(roles) != 4 {
		t.Errorf("len(roles) = %d, want 4", len(roles))
	}
	if len(blobFiles) != 4 {
		t.Errorf("len(blobFiles) = %d, want 4", len(blobFiles))
	}

	initCount, mediaCount := 0, 0
	for _, r := range roles {
		switch r.Role {
		case "init":
			initCount++
			if r.SortOrder != 0 {
				t.Errorf("init role SortOrder = %d, want 0", r.SortOrder)
			}
			if _, ok := r.BusinessMeta["representation_id"]; !ok {
				t.Error("init role missing representation_id")
			}
		case "media":
			mediaCount++
			if r.SortOrder <= 0 {
				t.Errorf("media role SortOrder = %d, want > 0", r.SortOrder)
			}
			if _, ok := r.BusinessMeta["segment_number"]; !ok {
				t.Error("media role missing segment_number")
			}
			if _, ok := r.BusinessMeta["representation_id"]; !ok {
				t.Error("media role missing representation_id")
			}
		default:
			t.Errorf("unexpected role: %s", r.Role)
		}
	}

	if initCount != 2 {
		t.Errorf("init roles = %d, want 2", initCount)
	}
	if mediaCount != 2 {
		t.Errorf("media roles = %d, want 2", mediaCount)
	}

	if durationS <= 0 {
		t.Errorf("durationS = %v, want > 0", durationS)
	}

	for _, b := range blobs {
		if !strings.HasPrefix(b.BlobHash, "sha256:") {
			t.Errorf("BlobHash = %s, want sha256: prefix", b.BlobHash)
		}
	}
}

// ─── helpers ────────────────────────────────────────────────────────────

func mustWriteFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func generateTestMP4(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "sample.mp4")

	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg not in PATH — cannot generate test MP4")
	}

	cmd := exec.Command("ffmpeg",
		"-f", "lavfi",
		"-i", "testsrc=duration=1:size=160x120:rate=1",
		"-f", "mp4",
		"-y",
		path,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("generate test MP4 failed: %v\n%s", err, string(out))
	}
	return path
}
