package ingest

import (
	"context"
	"encoding/json"
	"image"
	"image/color"
	"image/jpeg"
	"os"
	"strings"
	"testing"
)

func TestImageIngester_ContentType(t *testing.T) {
	i := NewImageIngester(t.TempDir())
	if ct := i.ContentType(); ct != "image" {
		t.Errorf("ContentType() = %s, want image", ct)
	}
}

func TestImageIngester_Process_JPEG(t *testing.T) {
	tmpDir := t.TempDir()
	workDir := tmpDir + "/work"
	if err := os.MkdirAll(workDir, 0755); err != nil {
		t.Fatal(err)
	}

	sampleJPEGData := generateTestJPEG(t, 100, 100, color.RGBA{255, 0, 0, 255})

	i := NewImageIngester(workDir)
	result, err := i.Process(context.Background(), strings.NewReader(string(sampleJPEGData)), ProcessOptions{
		ContentID: "test-image-content",
	})
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	if result.ContentType != "image" {
		t.Errorf("ContentType = %s, want image", result.ContentType)
	}

	if len(result.Blobs) < 2 {
		t.Fatalf("expected >= 2 blobs (original + thumbnails), got %d", len(result.Blobs))
	}
	if len(result.Blobs) != len(result.Roles) {
		t.Errorf("len(Blobs)=%d != len(Roles)=%d", len(result.Blobs), len(result.Roles))
	}

	// First blob should be original.
	origBlob := result.Blobs[0]
	origRole := result.Roles[0]
	if origBlob.BlobType != "jpeg_original" {
		t.Errorf("first blob type = %s, want jpeg_original", origBlob.BlobType)
	}
	if origRole.Role != "original" {
		t.Errorf("first role = %s, want original", origRole.Role)
	}
	w := origRole.BusinessMeta["width"]
	h := origRole.BusinessMeta["height"]
	if w == nil || h == nil {
		t.Fatal("original role missing width/height in BusinessMeta")
	}
	if w.(int) <= 0 || h.(int) <= 0 {
		t.Errorf("original dimensions: %dx%d, want >0", w, h)
	}

	// Remaining blobs should be thumbnails.
	for i := 1; i < len(result.Blobs); i++ {
		blob := result.Blobs[i]
		role := result.Roles[i]
		if blob.BlobType != "jpeg_thumbnail" {
			t.Errorf("blob[%d].BlobType = %s, want jpeg_thumbnail", i, blob.BlobType)
		}
		if role.Role != "thumbnail" {
			t.Errorf("role[%d].Role = %s, want thumbnail", i, role.Role)
		}
		tw := role.BusinessMeta["width"]
		th := role.BusinessMeta["height"]
		if tw == nil || th == nil {
			t.Fatalf("thumbnail role[%d] missing width/height in BusinessMeta", i)
		}
		if tw.(int) <= 0 || th.(int) <= 0 {
			t.Errorf("thumbnail[%d] dimensions: %dx%d, want >0", i, tw, th)
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
	for _, key := range []string{"format", "width", "height", "thumbnail_sizes"} {
		if _, ok := typeMeta[key]; !ok {
			t.Errorf("TypeMetadata missing key %q", key)
		}
	}
}

func TestResizeNearestNeighbor(t *testing.T) {
	src := image.NewRGBA(image.Rect(0, 0, 100, 100))
	fillColor := color.RGBA{128, 128, 128, 255}
	for y := 0; y < 100; y++ {
		for x := 0; x < 100; x++ {
			src.Set(x, y, fillColor)
		}
	}

	dst := resizeNearestNeighbor(src, 50)
	bounds := dst.Bounds()

	if bounds.Dx() != 50 {
		t.Errorf("resized width = %d, want 50", bounds.Dx())
	}
	if bounds.Dy() != 50 {
		t.Errorf("resized height = %d, want 50 (square input, 50 target width)", bounds.Dy())
	}

	r, g, b, a := dst.At(0, 0).RGBA()
	if r != 0x8080 || g != 0x8080 || b != 0x8080 || a != 0xffff {
		t.Errorf("pixel color mismatch: got (%04x, %04x, %04x, %04x), want (8080, 8080, 8080, ffff)", r, g, b, a)
	}
}

func TestResizeNearestNeighbor_NoUpscale(t *testing.T) {
	src := image.NewRGBA(image.Rect(0, 0, 50, 50))
	dst := resizeNearestNeighbor(src, 100)

	if dst.Bounds().Dx() != 50 {
		t.Errorf("expect no upscale: width = %d, want 50", dst.Bounds().Dx())
	}
}

func TestExtractOrientation_NoJPEG(t *testing.T) {
	result := extractOrientation([]byte{0x00, 0x01, 0x02, 0x03})
	if result != 0 {
		t.Errorf("expected 0 for non-JPEG data, got %d", result)
	}

	result = extractOrientation([]byte{})
	if result != 0 {
		t.Errorf("expected 0 for empty data, got %d", result)
	}
}

func TestExtractOrientation_JPEGNoExif(t *testing.T) {
	img := image.NewRGBA(image.Rect(0, 0, 10, 10))
	var buf strings.Builder
	if err := jpeg.Encode(&buf, img, nil); err != nil {
		t.Fatal(err)
	}
	data := []byte(buf.String())

	result := extractOrientation(data)
	if result != 0 {
		t.Errorf("expected 0 for JPEG without EXIF, got %d", result)
	}
}

func TestExtractOrientation_JPEGWithExif(t *testing.T) {
	img := image.NewRGBA(image.Rect(0, 0, 10, 10))
	var buf strings.Builder
	jpeg.Encode(&buf, img, nil)
	data := []byte(buf.String())

	// Insert a minimal APP1 Exif marker after SOI (0xFFD8) and before the first APP0.
	// JPEG structure: FF D8 [segments...]
	// We insert: FF E1 <length_big_endian> "Exif\x00\x00" <pad>
	data = injectExifSegment(t, data)

	result := extractOrientation(data)
	if result != 1 {
		t.Errorf("expected 1 for JPEG with EXIF header, got %d", result)
	}
}

func injectExifSegment(t *testing.T, jpegData []byte) []byte {
	t.Helper()
	if len(jpegData) < 4 {
		t.Fatal("jpegData too short")
	}
	soi := jpegData[:2]
	rest := jpegData[2:]

	exifPayload := []byte("Exif\x00\x00\x00\x00\x00\x00")
	segLen := len(exifPayload) + 2 // length includes itself
	seg := make([]byte, 0, 2+2+len(exifPayload))
	seg = append(seg, 0xFF, 0xE1)
	seg = append(seg, byte(segLen>>8), byte(segLen&0xFF))
	seg = append(seg, exifPayload...)

	out := make([]byte, 0, len(soi)+len(seg)+len(rest))
	out = append(out, soi...)
	out = append(out, seg...)
	out = append(out, rest...)
	return out
}

func generateTestJPEG(t *testing.T, w, h int, c color.Color) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			img.Set(x, y, c)
		}
	}
	var buf strings.Builder
	if err := jpeg.Encode(&buf, img, &jpeg.Options{Quality: 90}); err != nil {
		t.Fatal(err)
	}
	return []byte(buf.String())
}
