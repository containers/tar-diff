package tardiff

import (
	"archive/tar"
	"bytes"
	"io"
	"testing"
)

// Helper function to create test tar files in memory (for hardlink tests)
func createTestTar(entries []tarEntry) (io.ReadSeeker, error) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	for _, entry := range entries {
		hdr := &tar.Header{
			Name:     entry.name,
			Typeflag: entry.typeflag,
			Size:     int64(len(entry.data)),
			Mode:     0644,
		}
		if entry.linkname != "" {
			hdr.Linkname = entry.linkname
		}
		if entry.mode != 0 {
			hdr.Mode = entry.mode
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return nil, err
		}
		if len(entry.data) > 0 {
			if _, err := tw.Write(entry.data); err != nil {
				return nil, err
			}
		}
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}

	return bytes.NewReader(buf.Bytes()), nil
}

type tarEntry struct {
	name     string
	typeflag byte
	data     []byte
	linkname string
	mode     int64
}

func TestBuildSourceAnalysis_DirectoryWhiteout(t *testing.T) {
	oldTar, err := createTestTar([]tarEntry{
		{name: "boot/.kernel/version/.vmlinuz.hmac", typeflag: tar.TypeReg, data: []byte("hmac")},
		{name: "boot/.kernel-old/keep", typeflag: tar.TypeReg, data: []byte("sibling")},
		{name: "boot/EFI/Linux/image.efi", typeflag: tar.TypeReg, data: []byte("uki")},
	})
	if err != nil {
		t.Fatal(err)
	}
	whiteoutTar, err := createTestTar([]tarEntry{
		{name: "boot/.wh..kernel", typeflag: tar.TypeReg},
	})
	if err != nil {
		t.Fatal(err)
	}

	options := NewOptions()
	options.SetApplyWhiteouts(true)
	sources, err := AnalyzeSources([]io.ReadSeeker{oldTar, whiteoutTar}, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := sources.sourceByPath["boot/.kernel/version/.vmlinuz.hmac"]; ok {
		t.Error("file beneath a whiteouted directory remains a delta source")
	}
	for _, path := range []string{"boot/.kernel-old/keep", "boot/EFI/Linux/image.efi"} {
		if _, ok := sources.sourceByPath[path]; !ok {
			t.Errorf("unaffected file %q is missing from delta sources", path)
		}
	}
}

func TestAnalyzeTar_Hardlinks(t *testing.T) {
	entries := []tarEntry{
		{name: "original.txt", typeflag: tar.TypeReg, data: []byte("content")},
		{name: "hardlink.txt", typeflag: tar.TypeLink, linkname: "original.txt"},
	}
	tarFile, err := createTestTar(entries)
	if err != nil {
		t.Fatalf("Failed to create test tar: %v", err)
	}

	info, err := analyzeTar(tarFile, false)
	if err != nil {
		t.Fatalf("analyzeTar failed: %v", err)
	}

	// Should have 1 file (the original)
	if len(info.files) != 1 {
		t.Errorf("Expected 1 file, got %d", len(info.files))
	}

	// Should have 1 hardlink
	if len(info.hardlinks) != 1 {
		t.Errorf("Expected 1 hardlink, got %d", len(info.hardlinks))
	}

	// Check hardlink details
	hl := info.hardlinks[0]
	if hl.path != "hardlink.txt" {
		t.Errorf("Expected hardlink path 'hardlink.txt', got %q", hl.path)
	}
	if hl.linkname != "original.txt" {
		t.Errorf("Expected linkname 'original.txt', got %q", hl.linkname)
	}
	if hl.index != 1 {
		t.Errorf("Expected hardlink index 1, got %d", hl.index)
	}
}

func TestAnalyzeTar_MultipleHardlinks(t *testing.T) {
	entries := []tarEntry{
		{name: "file.txt", typeflag: tar.TypeReg, data: []byte("content")},
		{name: "link1.txt", typeflag: tar.TypeLink, linkname: "file.txt"},
		{name: "link2.txt", typeflag: tar.TypeLink, linkname: "file.txt"},
		{name: "link3.txt", typeflag: tar.TypeLink, linkname: "file.txt"},
	}
	tarFile, err := createTestTar(entries)
	if err != nil {
		t.Fatalf("Failed to create test tar: %v", err)
	}

	info, err := analyzeTar(tarFile, false)
	if err != nil {
		t.Fatalf("analyzeTar failed: %v", err)
	}

	if len(info.files) != 1 {
		t.Errorf("Expected 1 file, got %d", len(info.files))
	}
	if len(info.hardlinks) != 3 {
		t.Errorf("Expected 3 hardlinks, got %d", len(info.hardlinks))
	}

	// Verify all hardlinks point to the same file
	for i, hl := range info.hardlinks {
		if hl.linkname != "file.txt" {
			t.Errorf("Hardlink %d: expected linkname 'file.txt', got %q", i, hl.linkname)
		}
	}
}

func TestAnalyzeTar_DuplicateFilesAndHardlinks(t *testing.T) {
	entries := []tarEntry{
		{name: "file.txt", typeflag: tar.TypeReg, data: []byte("first")},
		{name: "file.txt", typeflag: tar.TypeReg, data: []byte("second")},
		{name: "file.txt", typeflag: tar.TypeReg, data: []byte("third")},
		{name: "link1.txt", typeflag: tar.TypeLink, linkname: "file.txt"},
		{name: "link2.txt", typeflag: tar.TypeLink, linkname: "file.txt"},
	}

	tarFile, err := createTestTar(entries)
	if err != nil {
		t.Fatalf("Failed to create test tar: %v", err)
	}

	info, err := analyzeTar(tarFile, false)
	if err != nil {
		t.Fatalf("analyzeTar failed: %v", err)
	}

	if len(info.files) != 3 {
		t.Errorf("Should have 3 files (all duplicates), got %d", len(info.files))
	}
	if len(info.hardlinks) != 2 {
		t.Errorf("Should have 2 hardlinks, got %d", len(info.hardlinks))
	}
}

func TestAnalyzeForDelta_HardlinksInTargetInfo(t *testing.T) {
	// Create old tar with a file
	oldEntries := []tarEntry{
		{name: "file.txt", typeflag: tar.TypeReg, data: []byte("old content")},
	}
	oldTar, err := createTestTar(oldEntries)
	if err != nil {
		t.Fatalf("Failed to create old tar: %v", err)
	}

	// Create new tar with the same file and a hardlink
	newEntries := []tarEntry{
		{name: "file.txt", typeflag: tar.TypeReg, data: []byte("old content")},
		{name: "link.txt", typeflag: tar.TypeLink, linkname: "file.txt"},
	}
	newTar, err := createTestTar(newEntries)
	if err != nil {
		t.Fatalf("Failed to create new tar: %v", err)
	}

	// Reset both tars
	if _, err := oldTar.Seek(0, 0); err != nil {
		t.Fatalf("oldTar.Seek: %v", err)
	}
	if _, err := newTar.Seek(0, 0); err != nil {
		t.Fatalf("newTar.Seek: %v", err)
	}

	oldInfo, err := analyzeTar(oldTar, false)
	if err != nil {
		t.Fatalf("analyzeTar (old) failed: %v", err)
	}

	newInfo, err := analyzeTar(newTar, false)
	if err != nil {
		t.Fatalf("analyzeTar (new) failed: %v", err)
	}

	// Reset old tar for analyzeForDelta
	if _, err := oldTar.Seek(0, 0); err != nil {
		t.Fatalf("oldTar.Seek: %v", err)
	}

	analysis, err := analyzeForDelta(buildSourceAnalysis([]*tarInfo{oldInfo}, 1, nil), newInfo, []io.ReadSeeker{oldTar}, nil)
	if err != nil {
		t.Fatalf("analyzeForDelta failed: %v", err)
	}
	defer func() {
		if err := analysis.Close(); err != nil {
			t.Fatalf("analysis.Close failed: %v", err)
		}
	}()

	// Check that hardlink is in targetInfoByIndex
	hlInfo, exists := analysis.targetInfoByIndex[1]
	if !exists {
		t.Fatal("Hardlink should be found in targetInfoByIndex at index 1")
	}
	if hlInfo.hardlink == nil {
		t.Fatal("targetInfo.hardlink should not be nil")
	}
	if hlInfo.hardlink.path != "link.txt" {
		t.Errorf("Expected hardlink path 'link.txt', got %q", hlInfo.hardlink.path)
	}
	if hlInfo.hardlink.linkname != "file.txt" {
		t.Errorf("Expected linkname 'file.txt', got %q", hlInfo.hardlink.linkname)
	}
}

func TestAnalyzeTar_HardlinksAddMultiplePaths(t *testing.T) {
	entries := []tarEntry{
		{name: "blobs/sha256/abc123", typeflag: tar.TypeReg, data: []byte("content")},
		{name: "real/file.txt", typeflag: tar.TypeLink, linkname: "blobs/sha256/abc123"},
		{name: "other/link.txt", typeflag: tar.TypeLink, linkname: "blobs/sha256/abc123"},
	}
	tarFile, err := createTestTar(entries)
	if err != nil {
		t.Fatalf("Failed to create test tar: %v", err)
	}

	info, err := analyzeTar(tarFile, false)
	if err != nil {
		t.Fatalf("analyzeTar failed: %v", err)
	}

	if len(info.files) != 1 {
		t.Fatalf("Expected 1 file, got %d", len(info.files))
	}

	file := &info.files[0]

	expectedPaths := []string{"blobs/sha256/abc123", "real/file.txt", "other/link.txt"}
	if len(file.paths) != len(expectedPaths) {
		t.Fatalf("Expected %d paths, got %d", len(expectedPaths), len(file.paths))
	}
	for i, expected := range expectedPaths {
		if file.paths[i] != expected {
			t.Errorf("Path %d: expected %q, got %q", i, expected, file.paths[i])
		}
	}

	expectedBasenames := []string{"abc123", "file.txt", "link.txt"}
	if len(file.basenames) != len(expectedBasenames) {
		t.Fatalf("Expected %d basenames, got %d", len(expectedBasenames), len(file.basenames))
	}
	for i, expected := range expectedBasenames {
		if file.basenames[i] != expected {
			t.Errorf("Basename %d: expected %q, got %q", i, expected, file.basenames[i])
		}
	}
}

func TestAnalyzeForDelta_MatchViaHardlinkPath(t *testing.T) {
	// Old tar: file with sha256 name and real name hardlink
	oldEntries := []tarEntry{
		{name: "blobs/sha256/abc123", typeflag: tar.TypeReg, data: []byte("version 1 content")},
		{name: "real/file.txt", typeflag: tar.TypeLink, linkname: "blobs/sha256/abc123"},
	}
	oldTar, err := createTestTar(oldEntries)
	if err != nil {
		t.Fatalf("Failed to create old tar: %v", err)
	}

	// New tar: file with different sha256 name but same real name
	newEntries := []tarEntry{
		{name: "blobs/sha256/def456", typeflag: tar.TypeReg, data: []byte("version 2 content")},
		{name: "real/file.txt", typeflag: tar.TypeLink, linkname: "blobs/sha256/def456"},
	}
	newTar, err := createTestTar(newEntries)
	if err != nil {
		t.Fatalf("Failed to create new tar: %v", err)
	}

	if _, err := oldTar.Seek(0, 0); err != nil {
		t.Fatalf("oldTar.Seek: %v", err)
	}
	if _, err := newTar.Seek(0, 0); err != nil {
		t.Fatalf("newTar.Seek: %v", err)
	}

	oldInfo, err := analyzeTar(oldTar, false)
	if err != nil {
		t.Fatalf("analyzeTar (old) failed: %v", err)
	}

	newInfo, err := analyzeTar(newTar, false)
	if err != nil {
		t.Fatalf("analyzeTar (new) failed: %v", err)
	}

	if _, err := oldTar.Seek(0, 0); err != nil {
		t.Fatalf("oldTar.Seek: %v", err)
	}

	analysis, err := analyzeForDelta(buildSourceAnalysis([]*tarInfo{oldInfo}, 1, nil), newInfo, []io.ReadSeeker{oldTar}, nil)
	if err != nil {
		t.Fatalf("analyzeForDelta failed: %v", err)
	}
	defer func() {
		if err := analysis.Close(); err != nil {
			t.Fatalf("analysis.Close failed: %v", err)
		}
	}()

	// The new file should have matched the old file via the "real/file.txt" path
	targetInfo := &analysis.targetInfos[0]
	if targetInfo.source == nil {
		t.Fatal("Expected target file to find a source match")
	}

	// The source should be the old file (which has "real/file.txt" as one of its paths)
	if len(targetInfo.source.file.paths) < 2 {
		t.Fatal("Expected source file to have multiple paths")
	}
	foundRealPath := false
	for _, p := range targetInfo.source.file.paths {
		if p == "real/file.txt" {
			foundRealPath = true
			break
		}
	}
	if !foundRealPath {
		t.Error("Expected source file to have 'real/file.txt' in its paths")
	}

	// The primary path (paths[0]) should be the sha256 path (first regular file entry)
	if targetInfo.source.file.paths[0] != "blobs/sha256/abc123" {
		t.Errorf("Expected primary source path to be 'blobs/sha256/abc123', got %q", targetInfo.source.file.paths[0])
	}
}

// Additional unit tests for comprehensive coverage

func TestAbs(t *testing.T) {
	tests := []struct {
		input    int64
		expected int64
	}{
		{10, 10},
		{-10, 10},
		{0, 0},
		{-1, 1},
		{1, 1},
	}

	for _, test := range tests {
		result := abs(test.input)
		if result != test.expected {
			t.Errorf("abs(%d) = %d, expected %d", test.input, result, test.expected)
		}
	}
}

func TestAnalyzeForDeltaHardlinkChains(t *testing.T) {
	// Test complex hardlink scenarios in delta analysis
	oldEntries := []tarEntry{
		{name: "base.txt", typeflag: tar.TypeReg, data: []byte("base content")},
		{name: "old-link.txt", typeflag: tar.TypeLink, linkname: "base.txt"},
	}
	oldTar, err := createTestTar(oldEntries)
	if err != nil {
		t.Fatalf("Failed to create old tar: %v", err)
	}

	newEntries := []tarEntry{
		{name: "base.txt", typeflag: tar.TypeReg, data: []byte("base content")},
		{name: "old-link.txt", typeflag: tar.TypeLink, linkname: "base.txt"},
		{name: "new-link1.txt", typeflag: tar.TypeLink, linkname: "base.txt"},
		{name: "new-link2.txt", typeflag: tar.TypeLink, linkname: "base.txt"},
	}
	newTar, err := createTestTar(newEntries)
	if err != nil {
		t.Fatalf("Failed to create new tar: %v", err)
	}

	// Reset tar readers
	if _, err := oldTar.Seek(0, 0); err != nil {
		t.Fatalf("oldTar.Seek: %v", err)
	}
	if _, err := newTar.Seek(0, 0); err != nil {
		t.Fatalf("newTar.Seek: %v", err)
	}

	oldInfo, err := analyzeTar(oldTar, false)
	if err != nil {
		t.Fatalf("analyzeTar (old) failed: %v", err)
	}

	newInfo, err := analyzeTar(newTar, false)
	if err != nil {
		t.Fatalf("analyzeTar (new) failed: %v", err)
	}

	if _, err := oldTar.Seek(0, 0); err != nil {
		t.Fatalf("oldTar.Seek: %v", err)
	}

	analysis, err := analyzeForDelta(buildSourceAnalysis([]*tarInfo{oldInfo}, 1, nil), newInfo, []io.ReadSeeker{oldTar}, nil)
	if err != nil {
		t.Fatalf("analyzeForDelta failed: %v", err)
	}
	defer func() {
		if err := analysis.Close(); err != nil {
			t.Logf("Failed to close analysis: %v", err)
		}
	}()

	// Should find hardlink info for the new hardlinks (indices 2 and 3)
	for index := 2; index <= 3; index++ {
		hlInfo, exists := analysis.targetInfoByIndex[index]
		if !exists {
			t.Errorf("Hardlink should be found in targetInfoByIndex at index %d", index)
			continue
		}
		if hlInfo.hardlink == nil {
			t.Errorf("targetInfo.hardlink should not be nil for index %d", index)
		}
	}
}

func TestAnalyzeTarDirectoriesAndSymlinks(t *testing.T) {
	entries := []tarEntry{
		{name: "dir/", typeflag: tar.TypeDir},
		{name: "symlink", typeflag: tar.TypeSymlink, linkname: "target"},
		{name: "file.txt", typeflag: tar.TypeReg, data: []byte("content")},
	}
	tarFile, err := createTestTar(entries)
	if err != nil {
		t.Fatalf("Failed to create test tar: %v", err)
	}

	info, err := analyzeTar(tarFile, false)
	if err != nil {
		t.Fatalf("analyzeTar failed: %v", err)
	}

	// Should only have 1 file (directories and symlinks ignored)
	if len(info.files) != 1 {
		t.Errorf("Expected 1 file, got %d", len(info.files))
	}

	if len(info.hardlinks) != 0 {
		t.Errorf("Expected 0 hardlinks, got %d", len(info.hardlinks))
	}
}

func TestAnalyzeTarDuplicateContent(t *testing.T) {
	content := []byte("duplicate content")
	entries := []tarEntry{
		{name: "file1.txt", typeflag: tar.TypeReg, data: content},
		{name: "file2.txt", typeflag: tar.TypeReg, data: content},
		{name: "different.txt", typeflag: tar.TypeReg, data: []byte("different")},
	}
	tarFile, err := createTestTar(entries)
	if err != nil {
		t.Fatalf("Failed to create test tar: %v", err)
	}

	info, err := analyzeTar(tarFile, false)
	if err != nil {
		t.Fatalf("analyzeTar failed: %v", err)
	}

	// Should have 3 files, 0 hardlinks
	if len(info.files) != 3 || len(info.hardlinks) != 0 {
		t.Errorf("Expected 3 files and 0 hardlinks, got %d files and %d hardlinks",
			len(info.files), len(info.hardlinks))
	}

	// Verify duplicate files have same SHA1
	if info.files[0].sha1 != info.files[1].sha1 {
		t.Error("Duplicate content files should have same SHA1")
	}
	if info.files[0].sha1 == info.files[2].sha1 {
		t.Error("Different content file should have different SHA1")
	}
}

func TestAnalyzeTarEmpty(t *testing.T) {
	entries := []tarEntry{}
	tarFile, err := createTestTar(entries)
	if err != nil {
		t.Fatalf("Failed to create test tar: %v", err)
	}

	info, err := analyzeTar(tarFile, false)
	if err != nil {
		t.Fatalf("analyzeTar failed: %v", err)
	}

	if len(info.files) != 0 {
		t.Errorf("Expected 0 files, got %d", len(info.files))
	}

	if len(info.hardlinks) != 0 {
		t.Errorf("Expected 0 hardlinks, got %d", len(info.hardlinks))
	}
}

func TestAnalyzeTarMultipleHardlinks(t *testing.T) {
	entries := []tarEntry{
		{name: "original.txt", typeflag: tar.TypeReg, data: []byte("content")},
		{name: "link1.txt", typeflag: tar.TypeLink, linkname: "original.txt"},
		{name: "link2.txt", typeflag: tar.TypeLink, linkname: "original.txt"},
	}
	tarFile, err := createTestTar(entries)
	if err != nil {
		t.Fatalf("Failed to create test tar: %v", err)
	}

	info, err := analyzeTar(tarFile, false)
	if err != nil {
		t.Fatalf("analyzeTar failed: %v", err)
	}

	if len(info.files) != 1 || len(info.hardlinks) != 2 {
		t.Errorf("Expected 1 file and 2 hardlinks, got %d files and %d hardlinks",
			len(info.files), len(info.hardlinks))
	}

	// Verify hardlink targets point to original
	for _, hl := range info.hardlinks {
		if hl.linkname != "original.txt" {
			t.Errorf("Hardlink %s should point to original.txt, got %s", hl.path, hl.linkname)
		}
	}
}

func TestIsDeltaCandidate(t *testing.T) {
	tests := []struct {
		name     string
		file     *tarFileInfo
		expected bool
	}{
		{
			name:     "File with good size for delta",
			file:     &tarFileInfo{size: 1024, basenames: []string{"file.txt"}},
			expected: true,
		},
		{
			name:     "File with xz extension",
			file:     &tarFileInfo{size: 1024, basenames: []string{"archive.xz"}},
			expected: false,
		},
		{
			name:     "Large file suitable for delta",
			file:     &tarFileInfo{size: 1024 * 1024, basenames: []string{"large.bin"}},
			expected: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := isDeltaCandidate(test.file)
			if result != test.expected {
				t.Errorf("isDeltaCandidate() = %v, expected %v", result, test.expected)
			}
		})
	}
}

func TestIsDeltaCandidateWithBz2(t *testing.T) {
	file := &tarFileInfo{size: 1024, basenames: []string{"archive.bz2"}}

	result := isDeltaCandidate(file)
	if result {
		t.Error("Expected isDeltaCandidate to return false for .bz2 file")
	}
}

func TestIsSparseFile(t *testing.T) {
	tests := []struct {
		name     string
		hdr      *tar.Header
		expected bool
	}{
		{
			name:     "GNU sparse file",
			hdr:      &tar.Header{Typeflag: tar.TypeGNUSparse},
			expected: true,
		},
		{
			name:     "Regular file with GNU sparse PAX records",
			hdr:      &tar.Header{Typeflag: tar.TypeReg, PAXRecords: map[string]string{"GNU.sparse.major": "1"}},
			expected: true,
		},
		{
			name:     "Regular file without sparse records",
			hdr:      &tar.Header{Typeflag: tar.TypeReg},
			expected: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := isSparseFile(test.hdr)
			if result != test.expected {
				t.Errorf("isSparseFile() = %v, expected %v", result, test.expected)
			}
		})
	}
}

func TestNameIsSimilar(t *testing.T) {
	tests := []struct {
		name     string
		fileA    *tarFileInfo
		fileB    *tarFileInfo
		expected bool
	}{
		{
			name:     "Exact basename match",
			fileA:    &tarFileInfo{basenames: []string{"file.txt"}},
			fileB:    &tarFileInfo{basenames: []string{"file.txt"}},
			expected: true,
		},
		{
			name:     "No basename match",
			fileA:    &tarFileInfo{basenames: []string{"file1.txt"}},
			fileB:    &tarFileInfo{basenames: []string{"file2.txt"}},
			expected: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := nameIsSimilar(test.fileA, test.fileB, 0)
			if result != test.expected {
				t.Errorf("nameIsSimilar() = %v, expected %v", result, test.expected)
			}
		})
	}
}

func TestNameIsSimilarWithFuzzy(t *testing.T) {
	fileA := &tarFileInfo{basenames: []string{"file1.txt"}}
	fileB := &tarFileInfo{basenames: []string{"file2.txt"}}

	result := nameIsSimilar(fileA, fileB, 1)
	// Verify that files with different prefixes before first "." don't match
	if result {
		t.Errorf("nameIsSimilar(%s, %s, 1) should be false: different prefixes", fileA.basenames[0], fileB.basenames[0])
	}

	// Verify symmetry - order shouldn't matter
	if nameIsSimilar(fileB, fileA, 1) != result {
		t.Error("nameIsSimilar should be symmetric")
	}

	// Test a case that should actually match with fuzzy -
	// same prefix before first "." means similar file across versions
	fileC := &tarFileInfo{basenames: []string{"libcurl.so.4.7.0"}}
	fileD := &tarFileInfo{basenames: []string{"libcurl.so.4.8.0"}}
	if !nameIsSimilar(fileC, fileD, 1) {
		t.Errorf("nameIsSimilar(%s, %s, 1) should be true: same prefix 'libcurl.'", fileC.basenames[0], fileD.basenames[0])
	}
}

func TestSizeIsSimilar(t *testing.T) {
	tests := []struct {
		name     string
		fileA    *tarFileInfo
		fileB    *tarFileInfo
		expected bool
	}{
		{
			name:     "Identical sizes",
			fileA:    &tarFileInfo{size: 1000},
			fileB:    &tarFileInfo{size: 1000},
			expected: true,
		},
		{
			name:     "Small files (always similar)",
			fileA:    &tarFileInfo{size: 1000},
			fileB:    &tarFileInfo{size: 50 * 1024},
			expected: true,
		},
		{
			name:     "Large files exceeding factor of 10",
			fileA:    &tarFileInfo{size: 100 * 1024},
			fileB:    &tarFileInfo{size: 1100 * 1024},
			expected: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := sizeIsSimilar(test.fileA, test.fileB)
			if result != test.expected {
				t.Errorf("sizeIsSimilar() = %v, expected %v", result, test.expected)
			}
		})
	}
}

func TestUseTarFile(t *testing.T) {
	tests := []struct {
		name      string
		hdr       *tar.Header
		cleanPath string
		expected  bool
	}{
		{
			name:      "Regular file with valid path",
			hdr:       &tar.Header{Typeflag: tar.TypeReg, Size: 100, Mode: 0644},
			cleanPath: "valid/path.txt",
			expected:  true,
		},
		{
			name:      "Regular file with empty path",
			hdr:       &tar.Header{Typeflag: tar.TypeReg, Size: 100},
			cleanPath: "",
			expected:  false,
		},
		{
			name:      "Directory",
			hdr:       &tar.Header{Typeflag: tar.TypeDir, Size: 0},
			cleanPath: "valid/path",
			expected:  false,
		},
		{
			name:      "Empty regular file",
			hdr:       &tar.Header{Typeflag: tar.TypeReg, Size: 0},
			cleanPath: "valid/path.txt",
			expected:  false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := useTarFile(test.hdr, test.cleanPath)
			if result != test.expected {
				t.Errorf("useTarFile() = %v, expected %v", result, test.expected)
			}
		})
	}
}

func TestUseTarFileWithoutWorldReadPermission(t *testing.T) {
	// Mode 0600 = owner read+write, but not world-readable (no other-read bit set)
	// useTarFile checks (hdr.Mode & 00004) == 0, which tests the world-readable bit
	hdr := &tar.Header{Typeflag: tar.TypeReg, Size: 100, Mode: 0600}
	cleanPath := "valid/path.txt"

	result := useTarFile(hdr, cleanPath)
	if result {
		t.Error("Expected useTarFile to return false for file without world-read permission (other-readable)")
	}
}
