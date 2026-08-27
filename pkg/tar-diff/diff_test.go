package tardiff

import (
	"archive/tar"
	"bytes"
	"io"
	"testing"
)

func TestGenerateDelta_Hardlinks(t *testing.T) {
	// Create a new tar with a file and hardlink
	entries := []tarEntry{
		{name: "file.txt", typeflag: tar.TypeReg, data: []byte("content")},
		{name: "link.txt", typeflag: tar.TypeLink, linkname: "file.txt"},
	}

	newTar, err := createTestTar(entries)
	if err != nil {
		t.Fatalf("Failed to create test tar: %v", err)
	}

	// Create old tar (empty for simplicity)
	oldEntries := []tarEntry{}
	oldTar, err := createTestTar(oldEntries)
	if err != nil {
		t.Fatalf("Failed to create old tar: %v", err)
	}

	// Analyze both tars
	if _, err := newTar.Seek(0, 0); err != nil {
		t.Fatalf("newTar: %v", err)
	}
	if _, err := oldTar.Seek(0, 0); err != nil {
		t.Fatalf("oldTar.Seek: %v", err)
	}

	newInfo, err := analyzeTar(newTar, false)
	if err != nil {
		t.Fatalf("analyzeTar (new) failed: %v", err)
	}

	oldInfo, err := analyzeTar(oldTar, false)
	if err != nil {
		t.Fatalf("analyzeTar (old) failed: %v", err)
	}

	// Create delta analysis
	if _, err := oldTar.Seek(0, 0); err != nil {
		t.Fatalf("oldTar.Seek: %v", err)
	}
	analysis, err := analyzeForDelta(buildSourceAnalysis([]*tarInfo{oldInfo}, 1, nil), newInfo, []io.ReadSeeker{oldTar}, nil)
	if err != nil {
		t.Fatalf("analyzeForDelta failed: %v", err)
	}
	defer func() { _ = analysis.Close() }()

	// Verify hardlink is in the analysis
	hlInfo, exists := analysis.targetInfoByIndex[1]
	if !exists {
		t.Fatal("Hardlink not found in targetInfoByIndex")
	}
	if hlInfo.hardlink == nil {
		t.Fatal("targetInfo.hardlink is nil")
	}

	// Generate delta
	if _, err := newTar.Seek(0, 0); err != nil {
		t.Fatalf("newTar.Seek: %v", err)
	}
	var deltaBuf bytes.Buffer
	options := NewOptions()

	err = generateDelta(newTar, &deltaBuf, analysis, options)
	if err != nil {
		t.Fatalf("generateDelta failed: %v", err)
	}

	// The delta should have been generated successfully
	// (We can't easily verify the exact content without applying it,
	// but we can check that it didn't error)
	if deltaBuf.Len() == 0 {
		t.Error("Delta is empty")
	}
}

func TestGenerateDelta_MixedHardlinksAndDuplicates(t *testing.T) {
	// Test both features together
	entries := []tarEntry{
		{name: "file.txt", typeflag: tar.TypeReg, data: []byte("first")},
		{name: "file.txt", typeflag: tar.TypeReg, data: []byte("second")},
		{name: "link1.txt", typeflag: tar.TypeLink, linkname: "file.txt"},
		{name: "link2.txt", typeflag: tar.TypeLink, linkname: "file.txt"},
	}

	newTar, err := createTestTar(entries)
	if err != nil {
		t.Fatalf("Failed to create test tar: %v", err)
	}

	oldEntries := []tarEntry{
		{name: "file2.txt", typeflag: tar.TypeReg, data: []byte("third")},
		{name: "link3", typeflag: tar.TypeLink, linkname: "file2.txt"},
	}
	oldTar, err := createTestTar(oldEntries)
	if err != nil {
		t.Fatalf("Failed to create old tar: %v", err)
	}

	// Analyze
	if _, err := newTar.Seek(0, 0); err != nil {
		t.Fatalf("newTar.Seek: %v", err)
	}
	if _, err := oldTar.Seek(0, 0); err != nil {
		t.Fatalf("oldTar.Seek: %v", err)
	}

	newInfo, err := analyzeTar(newTar, false)
	if err != nil {
		t.Fatalf("analyzeTar (new) failed: %v", err)
	}

	oldInfo, err := analyzeTar(oldTar, false)
	if err != nil {
		t.Fatalf("analyzeTar (old) failed: %v", err)
	}

	// Verify tracking
	if len(newInfo.files) != 2 {
		t.Errorf("Expected 2 files, got %d", len(newInfo.files))
	}
	if len(newInfo.hardlinks) != 2 {
		t.Errorf("Expected 2 hardlinks, got %d", len(newInfo.hardlinks))
	}

	// Create delta analysis
	if _, err := oldTar.Seek(0, 0); err != nil {
		t.Fatalf("oldTar.Seek: %v", err)
	}
	analysis, err := analyzeForDelta(buildSourceAnalysis([]*tarInfo{oldInfo}, 1, nil), newInfo, []io.ReadSeeker{oldTar}, nil)
	if err != nil {
		t.Fatalf("analyzeForDelta failed: %v", err)
	}
	defer func() { _ = analysis.Close() }()

	// Old tar has link3 (not in new); targetInforByIndex must only contain new-tar entries
	wantLen := len(newInfo.files) + len(newInfo.hardlinks)
	if len(analysis.targetInfoByIndex) != wantLen {
		t.Errorf("targetInfoByIndex should have %d entries (new tar only), got %d", wantLen, len(analysis.targetInfoByIndex))
	}

	// Verify all entries are in targetInfoByIndex
	// Indices 0, 1 are files, 2, 3 are hardlinks
	for i := 0; i < 4; i++ {
		info, exists := analysis.targetInfoByIndex[i]
		if !exists {
			t.Errorf("No targetInfo found for index %d", i)
			continue
		}
		if i < 2 {
			// Files
			if info.file == nil {
				t.Errorf("targetInfo.file is nil for file index %d", i)
			}
		} else {
			// Hardlinks
			if info.hardlink == nil {
				t.Errorf("targetInfo.hardlink is nil for hardlink index %d", i)
			}
		}
	}

	// Generate delta
	if _, err := newTar.Seek(0, 0); err != nil {
		t.Fatalf("newTar.Seek: %v", err)
	}
	var deltaBuf bytes.Buffer
	options := NewOptions()

	err = generateDelta(newTar, &deltaBuf, analysis, options)
	if err != nil {
		t.Fatalf("generateDelta failed: %v", err)
	}

	if deltaBuf.Len() == 0 {
		t.Error("Delta is empty")
	}
}

// Additional unit tests for comprehensive coverage

func TestDiffEmptyOldTars(t *testing.T) {
	newEntries := []tarEntry{
		{name: "file.txt", typeflag: tar.TypeReg, data: []byte("new content")},
	}
	newTar, err := createTestTar(newEntries)
	if err != nil {
		t.Fatalf("Failed to create new tar: %v", err)
	}

	var diffOutput bytes.Buffer
	options := NewOptions()

	err = Diff([]io.ReadSeeker{}, newTar, &diffOutput, options)
	if err == nil {
		t.Fatal("Expected Diff to fail with empty old tar files, but it succeeded")
	}
}

func TestDiffMultipleSources(t *testing.T) {
	// Create multiple old tars
	oldTar1Entries := []tarEntry{
		{name: "file1.txt", typeflag: tar.TypeReg, data: []byte("content1")},
	}
	oldTar1, err := createTestTar(oldTar1Entries)
	if err != nil {
		t.Fatalf("Failed to create old tar 1: %v", err)
	}

	oldTar2Entries := []tarEntry{
		{name: "file2.txt", typeflag: tar.TypeReg, data: []byte("content2")},
	}
	oldTar2, err := createTestTar(oldTar2Entries)
	if err != nil {
		t.Fatalf("Failed to create old tar 2: %v", err)
	}

	// Create new tar
	newEntries := []tarEntry{
		{name: "file1.txt", typeflag: tar.TypeReg, data: []byte("updated content1")},
		{name: "file2.txt", typeflag: tar.TypeReg, data: []byte("updated content2")},
		{name: "newfile.txt", typeflag: tar.TypeReg, data: []byte("completely new")},
	}
	newTar, err := createTestTar(newEntries)
	if err != nil {
		t.Fatalf("Failed to create new tar: %v", err)
	}

	var diffOutput bytes.Buffer
	options := NewOptions()

	err = Diff([]io.ReadSeeker{oldTar1, oldTar2}, newTar, &diffOutput, options)
	if err != nil {
		t.Fatalf("Diff failed: %v", err)
	}

	if diffOutput.Len() == 0 {
		t.Error("Diff output is empty")
	}
}

func TestDiff(t *testing.T) {
	oldEntries := []tarEntry{
		{name: "file.txt", typeflag: tar.TypeReg, data: []byte("old content")},
	}
	oldTar, err := createTestTar(oldEntries)
	if err != nil {
		t.Fatalf("Failed to create old tar: %v", err)
	}

	newEntries := []tarEntry{
		{name: "file.txt", typeflag: tar.TypeReg, data: []byte("new content")},
		{name: "newfile.txt", typeflag: tar.TypeReg, data: []byte("additional content")},
	}
	newTar, err := createTestTar(newEntries)
	if err != nil {
		t.Fatalf("Failed to create new tar: %v", err)
	}

	var diffOutput bytes.Buffer
	options := NewOptions()

	err = Diff([]io.ReadSeeker{oldTar}, newTar, &diffOutput, options)
	if err != nil {
		t.Fatalf("Diff failed: %v", err)
	}

	if diffOutput.Len() == 0 {
		t.Error("Diff output is empty")
	}
}

func TestNewOptions(t *testing.T) {
	options := NewOptions()

	if options == nil {
		t.Fatal("NewOptions() returned nil")
	}
	if options.compressionLevel != 3 {
		t.Errorf("Expected default compression level 3, got %d", options.compressionLevel)
	}
	if options.binaryDiffMethod != BinaryDiffBsdiff {
		t.Errorf("Expected default BinaryDiffBsdiff, got %v", options.binaryDiffMethod)
	}
	if options.maxZstdDiffSize != defaultMaxZstdDiffSize {
		t.Errorf("Expected default max zstd diff size %d, got %d", defaultMaxZstdDiffSize, options.maxZstdDiffSize)
	}
}

func TestOptionsSetCompressionLevel(t *testing.T) {
	options := NewOptions()

	options.SetCompressionLevel(9)
	if options.compressionLevel != 9 {
		t.Errorf("Expected compression level 9, got %d", options.compressionLevel)
	}
}

func TestOptionsSetMaxBsdiffFileSize(t *testing.T) {
	options := NewOptions()

	newSize := int64(100 * 1024 * 1024)
	options.SetMaxBsdiffFileSize(newSize)
	if options.maxBsdiffSize != newSize {
		t.Errorf("Expected max bsdiff file size %d, got %d", newSize, options.maxBsdiffSize)
	}
}

func TestOptionsSetMaxZstdDiffFileSize(t *testing.T) {
	options := NewOptions()

	newSize := int64(64 * 1024 * 1024)
	options.SetMaxZstdDiffFileSize(newSize)
	if options.maxZstdDiffSize != newSize {
		t.Errorf("Expected max zstd diff file size %d, got %d", newSize, options.maxZstdDiffSize)
	}
}

func TestOptionsSetSourcePrefixes(t *testing.T) {
	options := NewOptions()

	prefixes := []string{"prefix1/", "prefix2/"}
	options.SetSourcePrefixes(prefixes)

	if len(options.sourcePrefixes) != len(prefixes) {
		t.Errorf("Expected %d source prefixes, got %d", len(prefixes), len(options.sourcePrefixes))
	}

	for i, prefix := range prefixes {
		if options.sourcePrefixes[i] != prefix {
			t.Errorf("Expected source prefix %d to be %q, got %q", i, prefix, options.sourcePrefixes[i])
		}
	}
}

func TestOptionsSetIgnoreSourcePrefixes(t *testing.T) {
	options := NewOptions()

	prefixes := []string{"ignore1/", "ignore2/"}
	options.SetIgnoreSourcePrefixes(prefixes)

	if len(options.ignoreSourcePrefixes) != len(prefixes) {
		t.Errorf("Expected %d ignore source prefixes, got %d", len(prefixes), len(options.ignoreSourcePrefixes))
	}

	for i, prefix := range prefixes {
		if options.ignoreSourcePrefixes[i] != prefix {
			t.Errorf("Expected ignore source prefix %d to be %q, got %q", i, prefix, options.ignoreSourcePrefixes[i])
		}
	}
}

func TestOptionsSetTmpDir(t *testing.T) {
	options := NewOptions()

	tempDir := "/custom/tmp"
	options.SetTmpDir(tempDir)

	if options.tmpDir != tempDir {
		t.Errorf("Expected temp dir %q, got %q", tempDir, options.tmpDir)
	}
}

func TestOptionsSetApplyWhiteouts(t *testing.T) {
	options := NewOptions()

	options.SetApplyWhiteouts(true)

	if !options.applyWhiteouts {
		t.Error("Expected applyWhiteouts to be true")
	}

	options.SetApplyWhiteouts(false)

	if options.applyWhiteouts {
		t.Error("Expected applyWhiteouts to be false")
	}
}

// Test DiffWithSources public API
func TestDiffWithSources(t *testing.T) {
	// Create old tar
	oldEntries := []tarEntry{
		{name: "file1.txt", typeflag: tar.TypeReg, data: []byte("old content 1")},
		{name: "file2.txt", typeflag: tar.TypeReg, data: []byte("old content 2")},
	}
	oldTar, err := createTestTar(oldEntries)
	if err != nil {
		t.Fatalf("Failed to create old tar: %v", err)
	}

	// Create new tar
	newEntries := []tarEntry{
		{name: "file1.txt", typeflag: tar.TypeReg, data: []byte("new content 1")},
		{name: "file2.txt", typeflag: tar.TypeReg, data: []byte("new content 2")},
	}
	newTar, err := createTestTar(newEntries)
	if err != nil {
		t.Fatalf("Failed to create new tar: %v", err)
	}

	// First, analyze sources separately
	sources, err := AnalyzeSources([]io.ReadSeeker{oldTar}, nil)
	if err != nil {
		t.Fatalf("AnalyzeSources failed: %v", err)
	}

	// Reset old tar after analysis
	if _, err := oldTar.Seek(0, 0); err != nil {
		t.Fatalf("oldTar.Seek: %v", err)
	}

	// Now use DiffWithSources
	var diffOutput bytes.Buffer
	err = DiffWithSources(sources, []io.ReadSeeker{oldTar}, newTar, &diffOutput, nil)
	if err != nil {
		t.Fatalf("DiffWithSources failed: %v", err)
	}

	if diffOutput.Len() == 0 {
		t.Error("DiffWithSources output is empty")
	}
}

func TestDiffWithSources_NilSources(t *testing.T) {
	newEntries := []tarEntry{
		{name: "file.txt", typeflag: tar.TypeReg, data: []byte("content")},
	}
	newTar, err := createTestTar(newEntries)
	if err != nil {
		t.Fatalf("Failed to create new tar: %v", err)
	}

	var diffOutput bytes.Buffer
	err = DiffWithSources(nil, []io.ReadSeeker{}, newTar, &diffOutput, nil)
	if err == nil {
		t.Fatal("Expected error for nil sources, got nil")
	}
	if err.Error() != "sources cannot be nil" {
		t.Errorf("Expected 'sources cannot be nil' error, got: %v", err)
	}
}

func TestDiffWithSources_MismatchedOldTarCount(t *testing.T) {
	oldEntries := []tarEntry{
		{name: "file.txt", typeflag: tar.TypeReg, data: []byte("content")},
	}
	oldTar, err := createTestTar(oldEntries)
	if err != nil {
		t.Fatalf("Failed to create old tar: %v", err)
	}

	newEntries := []tarEntry{
		{name: "file.txt", typeflag: tar.TypeReg, data: []byte("new content")},
	}
	newTar, err := createTestTar(newEntries)
	if err != nil {
		t.Fatalf("Failed to create new tar: %v", err)
	}

	// Analyze with 1 source
	sources, err := AnalyzeSources([]io.ReadSeeker{oldTar}, nil)
	if err != nil {
		t.Fatalf("AnalyzeSources failed: %v", err)
	}

	// Try to use with 0 old tars (mismatch)
	var diffOutput bytes.Buffer
	err = DiffWithSources(sources, []io.ReadSeeker{}, newTar, &diffOutput, nil)
	if err == nil {
		t.Fatal("Expected error for mismatched old tar count, got nil")
	}
}

// Test the rollsum code path (for large files with matches)
func TestGenerateForFile_RollsumPath(t *testing.T) {
	// Create large content with some repeating patterns
	// This ensures rollsum will find matches
	pattern := []byte("This is a repeating pattern that will be used for rollsum matching. ")
	largeContent := bytes.Repeat(pattern, 3000) // ~200KB

	// Modify some parts to create a delta scenario
	modifiedContent := make([]byte, len(largeContent))
	copy(modifiedContent, largeContent)
	copy(modifiedContent[10000:10100], []byte(bytes.Repeat([]byte("CHANGED"), 14)))

	oldEntries := []tarEntry{
		{name: "largefile.bin", typeflag: tar.TypeReg, data: largeContent},
	}
	oldTar, err := createTestTar(oldEntries)
	if err != nil {
		t.Fatalf("Failed to create old tar: %v", err)
	}

	newEntries := []tarEntry{
		{name: "largefile.bin", typeflag: tar.TypeReg, data: modifiedContent},
	}
	newTar, err := createTestTar(newEntries)
	if err != nil {
		t.Fatalf("Failed to create new tar: %v", err)
	}

	var diffOutput bytes.Buffer
	options := NewOptions()
	// Set bsdiff size very small (1 byte) to force rollsum path for large files
	// (Setting to 0 would force bsdiff always)
	options.SetMaxBsdiffFileSize(1)

	err = Diff([]io.ReadSeeker{oldTar}, newTar, &diffOutput, options)
	if err != nil {
		t.Fatalf("Diff with rollsum failed: %v", err)
	}

	if diffOutput.Len() == 0 {
		t.Error("Diff output is empty")
	}

	// The diff should be much smaller than the full file since we used rollsums
	// (not a strict requirement, but generally true)
	t.Logf("Original file size: %d, Diff size: %d", len(largeContent), diffOutput.Len())
}

// Test exact file reuse (same SHA1)
func TestGenerateForFile_ExactMatch(t *testing.T) {
	// Create identical content
	content := []byte("This is identical content in both tars")

	oldEntries := []tarEntry{
		{name: "identical.txt", typeflag: tar.TypeReg, data: content},
	}
	oldTar, err := createTestTar(oldEntries)
	if err != nil {
		t.Fatalf("Failed to create old tar: %v", err)
	}

	newEntries := []tarEntry{
		{name: "identical.txt", typeflag: tar.TypeReg, data: content},
	}
	newTar, err := createTestTar(newEntries)
	if err != nil {
		t.Fatalf("Failed to create new tar: %v", err)
	}

	var diffOutput bytes.Buffer
	options := NewOptions()

	err = Diff([]io.ReadSeeker{oldTar}, newTar, &diffOutput, options)
	if err != nil {
		t.Fatalf("Diff failed: %v", err)
	}

	if diffOutput.Len() == 0 {
		t.Error("Diff output is empty")
	}

	// For identical files, the delta should be very small (just metadata)
	t.Logf("Identical file diff size: %d bytes", diffOutput.Len())
}

// Test with very small file (too small for delta)
func TestGenerateForFile_TooSmallForDelta(t *testing.T) {
	// Tiny file smaller than the path length + header
	tinyContent := []byte("x")

	oldEntries := []tarEntry{
		{name: "tiny.txt", typeflag: tar.TypeReg, data: tinyContent},
	}
	oldTar, err := createTestTar(oldEntries)
	if err != nil {
		t.Fatalf("Failed to create old tar: %v", err)
	}

	newEntries := []tarEntry{
		{name: "tiny.txt", typeflag: tar.TypeReg, data: []byte("y")},
	}
	newTar, err := createTestTar(newEntries)
	if err != nil {
		t.Fatalf("Failed to create new tar: %v", err)
	}

	var diffOutput bytes.Buffer
	err = Diff([]io.ReadSeeker{oldTar}, newTar, &diffOutput, nil)
	if err != nil {
		t.Fatalf("Diff failed: %v", err)
	}

	// Should still produce some output even for tiny files
	if diffOutput.Len() == 0 {
		t.Error("Diff output is empty")
	}
}

// Test AnalyzeSources can be reused for multiple diffs
func TestAnalyzeSources_Reusable(t *testing.T) {
	oldEntries := []tarEntry{
		{name: "source.txt", typeflag: tar.TypeReg, data: []byte("source content")},
	}
	oldTar, err := createTestTar(oldEntries)
	if err != nil {
		t.Fatalf("Failed to create old tar: %v", err)
	}

	// Analyze sources once
	sources, err := AnalyzeSources([]io.ReadSeeker{oldTar}, nil)
	if err != nil {
		t.Fatalf("AnalyzeSources failed: %v", err)
	}

	// Use the same sources for two different diffs
	newEntries1 := []tarEntry{
		{name: "source.txt", typeflag: tar.TypeReg, data: []byte("modified content 1")},
	}
	newTar1, err := createTestTar(newEntries1)
	if err != nil {
		t.Fatalf("Failed to create new tar 1: %v", err)
	}

	newEntries2 := []tarEntry{
		{name: "source.txt", typeflag: tar.TypeReg, data: []byte("modified content 2")},
	}
	newTar2, err := createTestTar(newEntries2)
	if err != nil {
		t.Fatalf("Failed to create new tar 2: %v", err)
	}

	// First diff
	if _, err := oldTar.Seek(0, 0); err != nil {
		t.Fatalf("oldTar.Seek: %v", err)
	}
	var diff1 bytes.Buffer
	err = DiffWithSources(sources, []io.ReadSeeker{oldTar}, newTar1, &diff1, nil)
	if err != nil {
		t.Fatalf("First DiffWithSources failed: %v", err)
	}

	// Second diff with same sources
	if _, err := oldTar.Seek(0, 0); err != nil {
		t.Fatalf("oldTar.Seek: %v", err)
	}
	var diff2 bytes.Buffer
	err = DiffWithSources(sources, []io.ReadSeeker{oldTar}, newTar2, &diff2, nil)
	if err != nil {
		t.Fatalf("Second DiffWithSources failed: %v", err)
	}

	if diff1.Len() == 0 || diff2.Len() == 0 {
		t.Error("One or both diffs are empty")
	}
}
