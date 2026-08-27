package tardiff

import (
	"archive/tar"
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/containers/tar-diff/pkg/protocol"
	tarpatch "github.com/containers/tar-diff/pkg/tar-patch"
	"github.com/klauspost/compress/zstd"
)

func TestZstdPatchFromRoundTrip(t *testing.T) {
	oldData := []byte("The quick brown fox jumps over the lazy dog")
	newData := []byte("The quick red fox runs over the lazy cat")

	window, err := zstdWindowSize(len(oldData), 0)
	if err != nil {
		t.Fatalf("zstdWindowSize failed: %v", err)
	}
	patch, err := zstdPatchFrom(oldData, bytes.NewReader(newData), 3, window)
	if err != nil {
		t.Fatalf("zstdPatchFrom failed: %v", err)
	}
	if len(patch) == 0 {
		t.Fatal("expected non-empty patch")
	}

	dec, err := zstd.NewReader(nil, zstd.WithDecoderDictRaw(zstdPatchDictID, oldData))
	if err != nil {
		t.Fatalf("create decoder: %v", err)
	}
	defer dec.Close()

	got, err := dec.DecodeAll(patch, nil)
	if err != nil {
		t.Fatalf("DecodeAll failed: %v", err)
	}
	if !bytes.Equal(got, newData) {
		t.Fatalf("round-trip mismatch:\n got %q\nwant %q", got, newData)
	}
}

func TestZstdPatchFromBestCompressionUsesDict(t *testing.T) {
	oldData := bytes.Repeat([]byte("abcdefghijklmnop"), 12800)
	newData := append([]byte{}, oldData...)
	newData[100] ^= 0xff
	newData[101] ^= 0xff

	window, err := zstdWindowSize(len(oldData), 0)
	if err != nil {
		t.Fatalf("zstdWindowSize failed: %v", err)
	}
	patch, err := zstdPatchFrom(oldData, bytes.NewReader(newData), 22, window)
	if err != nil {
		t.Fatalf("zstdPatchFrom failed: %v", err)
	}
	if len(patch) > 200 {
		t.Fatalf("expected small dict patch at best compression, got %d bytes", len(patch))
	}

	dec, err := zstd.NewReader(nil, zstd.WithDecoderDictRaw(zstdPatchDictID, oldData))
	if err != nil {
		t.Fatalf("create decoder: %v", err)
	}
	defer dec.Close()

	got, err := dec.DecodeAll(patch, nil)
	if err != nil {
		t.Fatalf("DecodeAll failed: %v", err)
	}
	if !bytes.Equal(got, newData) {
		t.Fatal("round-trip mismatch at SpeedBestCompression")
	}
}

func TestZstdFitsLimits(t *testing.T) {
	if !zstdFitsLimits(100, 100, defaultMaxZstdDiffSize) {
		t.Fatal("expected small files to fit")
	}
	if zstdFitsLimits(int64(zstdMaxWindow())+1, 100, 0) {
		t.Fatal("expected size over max window to be rejected")
	}
	if zstdFitsLimits(1000, 1000, 500) {
		t.Fatal("expected size over maxZstd to be rejected")
	}
	if !zstdFitsLimits(1000, 1000, 0) {
		t.Fatal("expected maxZstd 0 to mean no extra cap")
	}
}

func diffApplyZstd(t *testing.T, method BinaryDiffMethod, maxZstd, maxBsdiff int64, oldData, newData []byte) (delta bytes.Buffer, wantNew []byte) {
	t.Helper()
	oldTar, err := createTestTar([]tarEntry{{name: "file.txt", typeflag: tar.TypeReg, data: oldData}})
	if err != nil {
		t.Fatalf("create old tar: %v", err)
	}
	newTar, err := createTestTar([]tarEntry{{name: "file.txt", typeflag: tar.TypeReg, data: newData}})
	if err != nil {
		t.Fatalf("create new tar: %v", err)
	}
	wantNew, err = io.ReadAll(newTar)
	if err != nil {
		t.Fatalf("read new tar: %v", err)
	}
	if _, err := newTar.Seek(0, 0); err != nil {
		t.Fatalf("seek new tar: %v", err)
	}

	options := NewOptions()
	options.SetBinaryDiffMethod(method)
	options.SetMaxZstdDiffFileSize(maxZstd)
	options.SetMaxBsdiffFileSize(maxBsdiff)
	if err := Diff([]io.ReadSeeker{oldTar}, newTar, &delta, options); err != nil {
		t.Fatalf("Diff failed: %v", err)
	}
	if delta.Len() == 0 {
		t.Fatal("expected non-empty delta")
	}
	return delta, wantNew
}

func applyDelta(t *testing.T, delta *bytes.Buffer, oldData, wantNew []byte) {
	t.Helper()
	tmpDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(tmpDir, "file.txt"), oldData, 0o644); err != nil {
		t.Fatalf("write source file: %v", err)
	}
	var reconstructed bytes.Buffer
	ds := tarpatch.NewFilesystemDataSource(tmpDir)
	defer func() { _ = ds.Close() }()
	if err := tarpatch.Apply(delta, ds, &reconstructed); err != nil {
		t.Fatalf("Apply failed: %v", err)
	}
	if !bytes.Equal(reconstructed.Bytes(), wantNew) {
		t.Fatalf("reconstructed tar mismatch (%d vs %d bytes)", reconstructed.Len(), len(wantNew))
	}
}

func TestDiffApplyZstdBinaryDiff(t *testing.T) {
	oldData := []byte("The quick brown fox jumps over the lazy dog")
	newData := []byte("The quick red fox runs over the lazy cat")
	delta, wantNew := diffApplyZstd(t, BinaryDiffZstd, defaultMaxZstdDiffSize, defaultMaxBsdiffSize, oldData, newData)
	if !bytes.Equal(delta.Bytes()[:len(protocol.DeltaHeaderv2)], protocol.DeltaHeaderv2[:]) {
		t.Fatalf("expected tardf2 header, got %q", delta.Bytes()[:8])
	}
	applyDelta(t, &delta, oldData, wantNew)
}

func TestDiffAutoUsesZstdUnderCap(t *testing.T) {
	oldData := []byte("The quick brown fox jumps over the lazy dog")
	newData := []byte("The quick red fox runs over the lazy cat")
	delta, wantNew := diffApplyZstd(t, BinaryDiffAuto, defaultMaxZstdDiffSize, defaultMaxBsdiffSize, oldData, newData)
	if !bytes.Equal(delta.Bytes()[:len(protocol.DeltaHeaderv2)], protocol.DeltaHeaderv2[:]) {
		t.Fatalf("expected tardf2 header for auto zstd, got %q", delta.Bytes()[:8])
	}
	applyDelta(t, &delta, oldData, wantNew)
}

func TestDiffAutoUsesBsdiffOverZstdCap(t *testing.T) {
	oldData := bytes.Repeat([]byte("abcdefghijklmnop"), 4096)
	newData := append([]byte{}, oldData...)
	newData[100] ^= 0xff

	maxZstd := int64(len(oldData) / 2)
	delta, wantNew := diffApplyZstd(t, BinaryDiffAuto, maxZstd, defaultMaxBsdiffSize, oldData, newData)
	if !bytes.Equal(delta.Bytes()[:len(protocol.DeltaHeaderv2)], protocol.DeltaHeaderv2[:]) {
		t.Fatalf("expected tardf2 header for auto even when only bsdiff is used, got %q", delta.Bytes()[:8])
	}
	applyDelta(t, &delta, oldData, wantNew)
}

func TestDiffZstdDoesNotFallThroughToBsdiff(t *testing.T) {
	oldData := bytes.Repeat([]byte("abcdefghijklmnop"), 4096)
	newData := append([]byte{}, oldData...)
	newData[100] ^= 0xff

	maxZstd := int64(len(oldData) / 2)
	delta, wantNew := diffApplyZstd(t, BinaryDiffZstd, maxZstd, defaultMaxBsdiffSize, oldData, newData)
	if !bytes.Equal(delta.Bytes()[:len(protocol.DeltaHeaderv2)], protocol.DeltaHeaderv2[:]) {
		t.Fatalf("expected tardf2 header for zstd mode even when zstd is skipped, got %q", delta.Bytes()[:8])
	}
	applyDelta(t, &delta, oldData, wantNew)
}

func TestDiffAutoMixesZstdAndBsdiff(t *testing.T) {
	smallOld := []byte("The quick brown fox jumps over the lazy dog")
	smallNew := []byte("The quick red fox runs over the lazy cat")
	largeOld := bytes.Repeat([]byte("abcdefghijklmnop"), 4096)
	largeNew := append([]byte{}, largeOld...)
	largeNew[100] ^= 0xff

	oldTar, err := createTestTar([]tarEntry{
		{name: "small.txt", typeflag: tar.TypeReg, data: smallOld},
		{name: "large.txt", typeflag: tar.TypeReg, data: largeOld},
	})
	if err != nil {
		t.Fatalf("create old tar: %v", err)
	}
	newTar, err := createTestTar([]tarEntry{
		{name: "small.txt", typeflag: tar.TypeReg, data: smallNew},
		{name: "large.txt", typeflag: tar.TypeReg, data: largeNew},
	})
	if err != nil {
		t.Fatalf("create new tar: %v", err)
	}
	wantNew, err := io.ReadAll(newTar)
	if err != nil {
		t.Fatalf("read new tar: %v", err)
	}
	if _, err := newTar.Seek(0, 0); err != nil {
		t.Fatalf("seek new tar: %v", err)
	}

	var delta bytes.Buffer
	options := NewOptions()
	options.SetBinaryDiffMethod(BinaryDiffAuto)
	options.SetMaxZstdDiffFileSize(int64(len(largeOld) / 2))
	options.SetMaxBsdiffFileSize(defaultMaxBsdiffSize)
	if err := Diff([]io.ReadSeeker{oldTar}, newTar, &delta, options); err != nil {
		t.Fatalf("Diff failed: %v", err)
	}
	if !bytes.Equal(delta.Bytes()[:len(protocol.DeltaHeaderv2)], protocol.DeltaHeaderv2[:]) {
		t.Fatalf("expected tardf2 header when mix includes zstd, got %q", delta.Bytes()[:8])
	}

	tmpDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(tmpDir, "small.txt"), smallOld, 0o644); err != nil {
		t.Fatalf("write small source: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "large.txt"), largeOld, 0o644); err != nil {
		t.Fatalf("write large source: %v", err)
	}
	var reconstructed bytes.Buffer
	ds := tarpatch.NewFilesystemDataSource(tmpDir)
	defer func() { _ = ds.Close() }()
	if err := tarpatch.Apply(&delta, ds, &reconstructed); err != nil {
		t.Fatalf("Apply failed: %v", err)
	}
	if !bytes.Equal(reconstructed.Bytes(), wantNew) {
		t.Fatalf("reconstructed tar mismatch (%d vs %d bytes)", reconstructed.Len(), len(wantNew))
	}
}

func TestDiffApplyZstdThenCopySameSource(t *testing.T) {
	shared := bytes.Repeat([]byte("The quick brown fox jumps over the lazy dog\n"), 40)
	changed := append([]byte{}, shared...)
	changed[10] ^= 0xff

	oldTar, err := createTestTar([]tarEntry{
		{name: "shared.txt", typeflag: tar.TypeReg, data: shared},
	})
	if err != nil {
		t.Fatalf("create old tar: %v", err)
	}
	newTar, err := createTestTar([]tarEntry{
		{name: "shared.txt", typeflag: tar.TypeReg, data: changed},
		{name: "copy.txt", typeflag: tar.TypeReg, data: shared},
	})
	if err != nil {
		t.Fatalf("create new tar: %v", err)
	}
	wantNew, err := io.ReadAll(newTar)
	if err != nil {
		t.Fatalf("read new tar: %v", err)
	}
	if _, err := newTar.Seek(0, 0); err != nil {
		t.Fatalf("seek new tar: %v", err)
	}

	var delta bytes.Buffer
	options := NewOptions()
	options.SetBinaryDiffMethod(BinaryDiffZstd)
	if err := Diff([]io.ReadSeeker{oldTar}, newTar, &delta, options); err != nil {
		t.Fatalf("Diff failed: %v", err)
	}

	tmpDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(tmpDir, "shared.txt"), shared, 0o644); err != nil {
		t.Fatalf("write source file: %v", err)
	}
	var reconstructed bytes.Buffer
	ds := tarpatch.NewFilesystemDataSource(tmpDir)
	defer func() { _ = ds.Close() }()
	if err := tarpatch.Apply(&delta, ds, &reconstructed); err != nil {
		t.Fatalf("Apply failed: %v", err)
	}
	if !bytes.Equal(reconstructed.Bytes(), wantNew) {
		t.Fatalf("reconstructed tar mismatch (%d vs %d bytes)", reconstructed.Len(), len(wantNew))
	}
}

func TestOptionsSetBinaryDiffMethod(t *testing.T) {
	options := NewOptions()
	if options.binaryDiffMethod != BinaryDiffBsdiff {
		t.Fatalf("expected default BinaryDiffBsdiff, got %v", options.binaryDiffMethod)
	}

	options.SetBinaryDiffMethod(BinaryDiffZstd)
	if options.binaryDiffMethod != BinaryDiffZstd {
		t.Fatalf("expected BinaryDiffZstd, got %v", options.binaryDiffMethod)
	}

	options.SetBinaryDiffMethod(BinaryDiffAuto)
	if options.binaryDiffMethod != BinaryDiffAuto {
		t.Fatalf("expected BinaryDiffAuto, got %v", options.binaryDiffMethod)
	}
}

func TestZstdDiffOptionDefaults(t *testing.T) {
	options := NewOptions()
	if options.zstdDiffLevel != -1 {
		t.Fatalf("expected zstdDiffLevel -1, got %d", options.zstdDiffLevel)
	}
	if options.effectiveZstdDiffLevel() != options.compressionLevel {
		t.Fatalf("expected effective level %d, got %d", options.compressionLevel, options.effectiveZstdDiffLevel())
	}
	if options.maxZstdDiffSize != defaultMaxZstdDiffSize {
		t.Fatalf("expected maxZstdDiffSize %d, got %d", defaultMaxZstdDiffSize, options.maxZstdDiffSize)
	}

	options.SetZstdDiffLevel(9)
	if options.effectiveZstdDiffLevel() != 9 {
		t.Fatalf("expected effective level 9, got %d", options.effectiveZstdDiffLevel())
	}
}

func TestZstdWindowSize(t *testing.T) {
	w, err := zstdWindowSize(100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if w < 100 {
		t.Fatalf("auto window %d smaller than source", w)
	}
	if w&(w-1) != 0 {
		t.Fatalf("auto window %d not power of two", w)
	}

	w, err = zstdWindowSize(100, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if w != 1<<20 {
		t.Fatalf("got window %d, want %d", w, 1<<20)
	}

	if zstdMaxWindow() > zstdMaxCompatibleWindow {
		t.Fatalf("max window %d exceeds compatible cap %d", zstdMaxWindow(), zstdMaxCompatibleWindow)
	}
	w, err = zstdWindowSize(zstdMaxCompatibleWindow*2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if w != zstdMaxWindow() {
		t.Fatalf("auto window %d, want cap %d", w, zstdMaxWindow())
	}
	if _, err := zstdWindowSize(100, zstdMaxCompatibleWindow*2); err == nil {
		t.Fatal("expected error for configured window over 512MiB")
	}
}
