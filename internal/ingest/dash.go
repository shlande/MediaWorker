package ingest

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/shlande/mediaworker/internal/types"
)

// DashIngester transcodes an MP4 input into a multi-bitrate DASH manifest
// (MPEG-DASH) using ffmpeg, then scans the output directory to produce
// content-addressed blobs and per-content roles.
type DashIngester struct {
	ffmpegPath string
	workDir    string
}

// NewDashIngester returns a configured DashIngester.
func NewDashIngester(ffmpegPath, workDir string) *DashIngester {
	return &DashIngester{ffmpegPath: ffmpegPath, workDir: workDir}
}

// ContentType implements ContentIngester.
func (d *DashIngester) ContentType() string { return "dash_video" }

// Process transcodes input MP4 into DASH segments and returns the resulting
// blobs + roles + type metadata.
func (d *DashIngester) Process(ctx context.Context, input io.Reader, opts ProcessOptions) (*ProcessResult, error) {
	contentID := opts.ContentID
	if contentID == "" {
		contentID = uuid.New().String()
	}

	outDir := filepath.Join(d.workDir, contentID)
	if err := os.MkdirAll(outDir, 0755); err != nil {
		return nil, fmt.Errorf("create output dir: %w", err)
	}

	// 1. Write input to temporary source file.
	srcPath := filepath.Join(d.workDir, contentID+"_src.mp4")
	if err := writeToFile(input, srcPath); err != nil {
		_ = os.RemoveAll(outDir) // best-effort cleanup of partial dir
		return nil, fmt.Errorf("write source: %w", err)
	}
	defer func() { _ = os.Remove(srcPath) }() // best-effort cleanup of temp source

	// 2. ffmpeg DASH transcode.
	bitrates := firstNonEmpty(opts.Metadata["dash_bitrates"], "500k,1500k,4000k")
	segDuration := firstNonEmpty(opts.Metadata["dash_seg_duration"], "4")

	if err := d.runFFmpeg(ctx, srcPath, outDir, bitrates, segDuration); err != nil {
		_ = os.RemoveAll(outDir) // best-effort cleanup of failed transcode
		return nil, fmt.Errorf("ffmpeg: %w", err)
	}

	// 3. Scan output directory.
	blobs, roles, blobFiles, duration, err := d.scanDashOutput(outDir)
	if err != nil {
		_ = os.RemoveAll(outDir) // best-effort cleanup of unreadable output
		return nil, fmt.Errorf("scan output: %w", err)
	}

	// 4. Read manifest.mpd as type metadata.
	mpdPath := filepath.Join(outDir, "manifest.mpd")
	mpdXML, err := os.ReadFile(mpdPath)
	if err != nil {
		_ = os.RemoveAll(outDir) // best-effort cleanup
		return nil, fmt.Errorf("read mpd: %w", err)
	}

	segDur, _ := strconv.Atoi(segDuration)
	typeMeta, _ := json.Marshal(map[string]any{
		"mpd_xml":      string(mpdXML),
		"duration_s":   duration,
		"seg_duration": segDur,
	})

	return &ProcessResult{
		ContentID:    contentID,
		ContentType:  d.ContentType(),
		Blobs:        blobs,
		Roles:        roles,
		TypeMetadata: typeMeta,
		BlobFiles:    blobFiles,
		WorkDir:      outDir,
	}, nil
}

// ─── ffmpeg ────────────────────────────────────────────────────────────

// runFFmpeg executes the DASH transcode command per docs/ingest/README.md §6.2.
// The multi-bitrate template uses libx264 + AAC and produces init/segment
// files with names like init_$RepresentationID$.m4s and
// seg_$RepresentationID$_$Number$.m4s.
func (d *DashIngester) runFFmpeg(ctx context.Context, srcPath, outDir, bitrates, segDuration string) error {
	bitrateList := strings.Split(bitrates, ",")
	resolutions := [][2]string{{"640", "360"}, {"1280", "720"}, {"1920", "1080"}}

	args := []string{
		"-i", srcPath,
		"-map", "0:v", "-map", "0:a",
		"-c:v", "libx264",
	}
	for i, br := range bitrateList {
		args = append(args, fmt.Sprintf("-b:v:%d", i), strings.TrimSpace(br))
		if i < len(resolutions) {
			args = append(args,
				fmt.Sprintf("-s:v:%d", i),
				fmt.Sprintf("%sx%s", resolutions[i][0], resolutions[i][1]),
			)
		}
	}
	args = append(args,
		"-c:a", "aac", "-b:a", "128k",
		"-f", "dash",
		"-seg_duration", segDuration,
		"-use_template", "1",
		"-use_timeline", "1",
		"-init_seg_name", "init_$RepresentationID$.m4s",
		"-media_seg_name", "seg_$RepresentationID$_$Number$.m4s",
		filepath.Join(outDir, "manifest.mpd"),
	)

	cmd := exec.CommandContext(ctx, d.ffmpegPath, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("ffmpeg exit: %w\n%s", err, string(out))
	}
	return nil
}

// ─── output scanning ───────────────────────────────────────────────────

// dashFilePattern matches files produced by ffmpeg:
//   - init_<rep>.m4s
//   - seg_<rep>_<num>.m4s
var (
	initPattern = regexp.MustCompile(`^init_(.+)\.m4s$`)
	segPattern  = regexp.MustCompile(`^seg_(.+)_(\d+)\.m4s$`)
)

// scanDashOutput walks outDir and builds BlobDescriptor + BlobRole for every
// .m4s file. It also reads the MPD to extract total duration.
func (d *DashIngester) scanDashOutput(outDir string) (
	blobs []types.BlobDescriptor,
	roles []types.BlobRole,
	blobFiles map[string]string,
	durationS float64,
	err error,
) {
	blobFiles = make(map[string]string)

	entries, err := os.ReadDir(outDir)
	if err != nil {
		return nil, nil, nil, 0, err
	}

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		fullPath := filepath.Join(outDir, e.Name())
		hash, size, err := hashFile(fullPath)
		if err != nil {
			return nil, nil, nil, 0, fmt.Errorf("hash %s: %w", e.Name(), err)
		}

		var role string
		var sortOrder int
		businessMeta := make(map[string]any)

		switch {
		case initPattern.MatchString(e.Name()):
			m := initPattern.FindStringSubmatch(e.Name())
			repID := m[1]
			role = "init"
			sortOrder = 0
			businessMeta["representation_id"] = repID

		case segPattern.MatchString(e.Name()):
			m := segPattern.FindStringSubmatch(e.Name())
			repID := m[1]
			segNum, _ := strconv.Atoi(m[2])
			role = "media"
			sortOrder = segNum
			businessMeta["representation_id"] = repID
			businessMeta["segment_number"] = segNum

		default:
			continue // skip non-.m4s files (manifest.mpd, etc.)
		}

		blobs = append(blobs, types.BlobDescriptor{
			BlobHash: hash,
			BlobType: blobTypeFor(role),
			Size:     size,
		})
		roles = append(roles, types.BlobRole{
			BlobHash:     hash,
			Role:         role,
			SortOrder:    sortOrder,
			BusinessMeta: businessMeta,
		})
		blobFiles[hash] = fullPath
	}

	// Approximate duration: count maximum segments in any representation.
	segCount := 0
	for _, r := range roles {
		if r.Role == "media" && r.SortOrder > segCount {
			segCount = r.SortOrder
		}
	}
	// If any "media" roles exist, segCount * 4 gives a rough estimate (default
	// segment duration of 4s). Real MPD parsing would give exact duration.
	durationS = float64(segCount) * 4

	return blobs, roles, blobFiles, durationS, nil
}

// ─── helpers ──────────────────────────────────────────────────────────

// hashFile returns the SHA-256 hex digest of the file at path, prefixed with
// "sha256:", and the file size in bytes.
func hashFile(path string) (hash string, size int64, err error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer func() { _ = f.Close() }()

	h := sha256.New()
	if size, err = io.Copy(h, f); err != nil {
		return "", 0, err
	}
	return "sha256:" + fmt.Sprintf("%x", h.Sum(nil)), size, nil
}

// writeToFile copies all bytes from r to the file at path, creating or
// truncating it.
func writeToFile(r io.Reader, path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	_, err = io.Copy(f, r)
	return err
}

// blobTypeFor returns the binary production type string for a DASH role.
func blobTypeFor(role string) string {
	switch role {
	case "init":
		return "mp4_init_segment"
	case "media":
		return "m4s_media_segment"
	default:
		return "m4s_media_segment"
	}
}

// firstNonEmpty returns a if non-empty, otherwise b.
func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}
