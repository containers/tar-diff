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
