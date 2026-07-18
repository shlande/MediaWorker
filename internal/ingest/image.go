package ingest

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"image"
	"image/jpeg"
	"image/png"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/shlande/mediaworker/internal/types"
)

// ImageIngester processes raw image input into an original blob plus a set of
// JPEG thumbnails at configurable sizes.
type ImageIngester struct {
	workDir string
}

// NewImageIngester returns a configured ImageIngester.
func NewImageIngester(workDir string) *ImageIngester {
	return &ImageIngester{workDir: workDir}
}

// ContentType implements ContentIngester.
func (i *ImageIngester) ContentType() string { return "image" }

// Process decodes the input image, saves the original, generates thumbnails,
// and extracts a simplified EXIF orientation.
func (i *ImageIngester) Process(ctx context.Context, input io.Reader, opts ProcessOptions) (*ProcessResult, error) {
	contentID := opts.ContentID
	if contentID == "" {
		contentID = uuid.New().String()
	}

	workDir := filepath.Join(i.workDir, contentID)
	if err := os.MkdirAll(workDir, 0755); err != nil {
		return nil, fmt.Errorf("create work dir: %w", err)
	}

	// 1. Slurp all input bytes (we need them for both decode and orientation).
	raw, err := io.ReadAll(input)
	if err != nil {
		os.RemoveAll(workDir)
		return nil, fmt.Errorf("read input: %w", err)
	}

	// 2. Save original file.
	origPath := filepath.Join(workDir, "original")
	if err := os.WriteFile(origPath, raw, 0644); err != nil {
		os.RemoveAll(workDir)
		return nil, fmt.Errorf("write original: %w", err)
	}

	// 3. Decode to get dimensions and format.
	img, format, err := image.Decode(strings.NewReader(string(raw)))
	if err != nil {
		os.RemoveAll(workDir)
		return nil, fmt.Errorf("decode image: %w", err)
	}
	bounds := img.Bounds()
	width, height := bounds.Dx(), bounds.Dy()

	// 4. Original blob.
	origHash, origSize, err := hashFile(origPath)
	if err != nil {
		os.RemoveAll(workDir)
		return nil, fmt.Errorf("hash original: %w", err)
	}

	blobs := []types.BlobDescriptor{{
		BlobHash: origHash,
		BlobType: blobTypeForImage(format),
		Size:     origSize,
	}}
	roles := []types.BlobRole{{
		BlobHash: origHash,
		Role:     "original",
		SortOrder: 0,
		BusinessMeta: map[string]any{
			"width":  width,
			"height": height,
		},
	}}
	blobFiles := map[string]string{origHash: origPath}

	// 5. Thumbnails.
	thumbSizes := parseThumbSizes(opts.Metadata["image_thumbnail_sizes"])
	for _, tw := range thumbSizes {
		thumb := resizeNearestNeighbor(img, tw)
		thumbPath := filepath.Join(workDir, fmt.Sprintf("thumb_%d.jpg", tw))
		if err := encodeImage(thumb, thumbPath, "jpeg"); err != nil {
			os.RemoveAll(workDir)
			return nil, fmt.Errorf("encode thumbnail %d: %w", tw, err)
		}

		thumbHash, thumbSize, err := hashFile(thumbPath)
		if err != nil {
			os.RemoveAll(workDir)
			return nil, fmt.Errorf("hash thumbnail %d: %w", tw, err)
		}

		th := height * tw / width
		blobs = append(blobs, types.BlobDescriptor{
			BlobHash: thumbHash,
			BlobType: "jpeg_thumbnail",
			Size:     thumbSize,
		})
		roles = append(roles, types.BlobRole{
			BlobHash:  thumbHash,
			Role:      "thumbnail",
			SortOrder: tw,
			BusinessMeta: map[string]any{
				"width":  tw,
				"height": th,
			},
		})
		blobFiles[thumbHash] = thumbPath
	}

	// 6. Orientation.
	orientation := extractOrientation(raw)

	// 7. Type metadata.
	typeMeta, _ := json.Marshal(map[string]any{
		"format":           format,
		"width":            width,
		"height":           height,
		"thumbnail_sizes":  thumbSizes,
		"exif_orientation": orientation,
	})

	return &ProcessResult{
		ContentID:    contentID,
		ContentType:  i.ContentType(),
		Blobs:        blobs,
		Roles:        roles,
		TypeMetadata: typeMeta,
		BlobFiles:    blobFiles,
		WorkDir:      workDir,
	}, nil
}

// ─── thumbnail helpers (stdlib only) ─────────────────────────────────

// resizeNearestNeighbor returns a new image scaled to targetWidth using
// nearest-neighbor interpolation. Height is proportional to the source aspect
// ratio.
func resizeNearestNeighbor(src image.Image, targetWidth int) image.Image {
	b := src.Bounds()
	srcW, srcH := b.Dx(), b.Dy()
	if targetWidth >= srcW {
		return src // no upscale
	}
	targetHeight := int(math.Round(float64(srcH) * float64(targetWidth) / float64(srcW)))
	dst := image.NewRGBA(image.Rect(0, 0, targetWidth, targetHeight))

	xRatio := float64(srcW) / float64(targetWidth)
	yRatio := float64(srcH) / float64(targetHeight)

	for dy := 0; dy < targetHeight; dy++ {
		srcY := int(math.Floor(float64(dy) * yRatio))
		for dx := 0; dx < targetWidth; dx++ {
			srcX := int(math.Floor(float64(dx) * xRatio))
			dst.Set(dx, dy, src.At(srcX, srcY))
		}
	}
	return dst
}

// encodeImage writes img to path in the specified format ("jpeg" or "png").
func encodeImage(img image.Image, path string, format string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	switch format {
	case "jpeg":
		return jpeg.Encode(f, img, &jpeg.Options{Quality: 85})
	case "png":
		return png.Encode(f, img)
	default:
		return jpeg.Encode(f, img, &jpeg.Options{Quality: 85})
	}
}

// parseThumbSizes parses a comma-separated list of thumbnail widths. Returns
// [200, 800] if the input is empty or unparseable.
func parseThumbSizes(s string) []int {
	if s == "" {
		return []int{200, 800}
	}
	parts := strings.Split(s, ",")
	sizes := make([]int, 0, len(parts))
	for _, p := range parts {
		n, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil || n <= 0 {
			continue
		}
		sizes = append(sizes, n)
	}
	if len(sizes) == 0 {
		return []int{200, 800}
	}
	return sizes
}

// blobTypeForImage returns the BlobType string for a given image format.
func blobTypeForImage(format string) string {
	switch strings.ToLower(format) {
	case "jpeg":
		return "jpeg_original"
	case "png":
		return "png_original"
	default:
		return format + "_original"
	}
}

// ─── EXIF orientation (simplified) ───────────────────────────────────

// extractOrientation scans JPEG data for an APP1 (0xFFE1) segment containing
// Exif data. Returns 1 (normal) if the EXIF header is found, 0 otherwise.
// This is a simplified check — a full EXIF parser would extract the actual
// Orientation tag value.
func extractOrientation(data []byte) int {
	if len(data) < 4 || data[0] != 0xFF || data[1] != 0xD8 {
		return 0 // not JPEG
	}

	pos := 2
	for pos < len(data)-3 {
		if data[pos] != 0xFF {
			return 0
		}
		marker := data[pos+1]
		if marker == 0xE1 {
			// APP1 segment — check for "Exif\0\0" header.
			if pos+10 >= len(data) {
				return 0
			}
			segLen := int(binary.BigEndian.Uint16(data[pos+2 : pos+4]))
			if pos+2+segLen > len(data) {
				return 0
			}
			payload := data[pos+4 : pos+2+segLen]
			if len(payload) >= 6 && string(payload[:6]) == "Exif\x00\x00" {
				return 1 // Exif found (simplified: assume orientation=1)
			}
			pos += 2 + segLen
		} else if marker == 0xD8 || marker == 0xD9 || marker == 0xDA || (marker >= 0xD0 && marker <= 0xD7) {
			// Marker with no length field.
			pos += 2
		} else {
			// Other markers have a length field.
			if pos+3 >= len(data) {
				return 0
			}
			segLen := int(binary.BigEndian.Uint16(data[pos+2 : pos+4]))
			pos += 2 + segLen
		}
	}
	return 0
}
