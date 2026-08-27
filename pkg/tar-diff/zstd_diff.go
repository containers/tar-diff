package tardiff

import (
	"bytes"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
)

// zstdPatchDictID matches zstd --patch-from (raw dictionary id 0).
const zstdPatchDictID = 0

// zstdMaxCompatibleWindow is the largest window we emit so older apply
// implementations (and current klauspost) can still decode the frame.
const zstdMaxCompatibleWindow = 512 * 1024 * 1024

func zstdMaxWindow() int {
	if zstd.MaxWindowSize < zstdMaxCompatibleWindow {
		return zstd.MaxWindowSize
	}
	return zstdMaxCompatibleWindow
}

func zstdPatchFrom(oldData []byte, newData io.Reader, compressionLevel int, windowSize int) ([]byte, error) {
	level := zstd.EncoderLevelFromZstd(compressionLevel)
	opts := []zstd.EOption{
		zstd.WithEncoderLevel(level),
		zstd.WithEncoderDictRaw(zstdPatchDictID, oldData),
		zstd.WithEncoderConcurrency(1),
		zstd.WithSingleSegment(true),
	}
	if windowSize > 0 {
		opts = append(opts, zstd.WithWindowSize(windowSize))
	}

	var buf bytes.Buffer
	enc, err := zstd.NewWriter(&buf, opts...)
	if err != nil {
		return nil, fmt.Errorf("create zstd patch encoder: %w", err)
	}
	if _, err := io.Copy(enc, newData); err != nil {
		_ = enc.Close()
		return nil, fmt.Errorf("encode zstd patch: %w", err)
	}
	if err := enc.Close(); err != nil {
		return nil, fmt.Errorf("close zstd patch encoder: %w", err)
	}
	return buf.Bytes(), nil
}

func zstdFitsLimits(fileSize, sourceSize, maxZstd int64) bool {
	maxWin := int64(zstdMaxWindow())
	if fileSize > maxWin || sourceSize > maxWin {
		return false
	}
	if maxZstd == 0 {
		return true
	}
	return fileSize < maxZstd && sourceSize < maxZstd
}

func sizeWithinLimit(size, max int64) bool {
	if max == 0 {
		return true
	}
	return size < max
}

// zstdWindowSize picks a power-of-two window large enough for the source
// dictionary (old file), capped at min(512MiB, zstd.MaxWindowSize).
// configuredBytes 0 means auto.
func zstdWindowSize(oldLen, configuredBytes int) (int, error) {
	maxWin := zstdMaxWindow()
	if configuredBytes > 0 {
		if configuredBytes < zstd.MinWindowSize || configuredBytes > maxWin {
			return 0, fmt.Errorf("zstd diff window %d out of range [%d, %d]", configuredBytes, zstd.MinWindowSize, maxWin)
		}
		if configuredBytes&(configuredBytes-1) != 0 {
			return 0, fmt.Errorf("zstd diff window %d must be a power of two", configuredBytes)
		}
		return configuredBytes, nil
	}

	need := oldLen
	if need < zstd.MinWindowSize {
		return zstd.MinWindowSize, nil
	}
	if need > maxWin {
		return maxWin, nil
	}

	w := zstd.MinWindowSize
	for w < need {
		w <<= 1
	}
	return w, nil
}
