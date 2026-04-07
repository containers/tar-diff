package tardiff

import (
	"archive/tar"
	"testing"
)

func TestBuildSourceInfos(t *testing.T) {
	// Create two tar infos
	tar1Entries := []tarEntry{
		{name: "file1.txt", typeflag: tar.TypeReg, data: []byte("content1")},
		{name: "file2.txt", typeflag: tar.TypeReg, data: []byte("content2")},
	}
	tar1, err := createTestTar(tar1Entries)
	if err != nil {
		t.Fatalf("Failed to create tar1: %v", err)
	}

	tar2Entries := []tarEntry{
		{name: "file2.txt", typeflag: tar.TypeReg, data: []byte("content2-override")},
		{name: "file3.txt", typeflag: tar.TypeReg, data: []byte("content3")},
	}
	tar2, err := createTestTar(tar2Entries)
	if err != nil {
		t.Fatalf("Failed to create tar2: %v", err)
	}

	info1, err := analyzeTar(tar1, false)
	if err != nil {
		t.Fatalf("Failed to analyze tar1: %v", err)
	}

	info2, err := analyzeTar(tar2, false)
	if err != nil {
		t.Fatalf("Failed to analyze tar2: %v", err)
	}

	sourceInfos := buildSourceAnalysis([]*tarInfo{info1, info2}, 2, NewOptions()).sourceInfos

	// Should have 3 files total (file1, file2-orig, file2-override, file3)
	// But file2-orig should be marked as overwritten
	if len(sourceInfos) != 4 {
		t.Fatalf("Expected 4 source infos, got %d", len(sourceInfos))
	}

	// Check that first file2 is marked as overwritten
	var file2FromTar1 *sourceInfo
	var file2FromTar2 *sourceInfo
	for i := range sourceInfos {
		s := &sourceInfos[i]
		if s.file.paths[0] == "file2.txt" {
			switch s.sourceTarFileIndex {
			case 0:
				file2FromTar1 = s
			case 1:
				file2FromTar2 = s
			}
		}
	}

	if file2FromTar1 == nil {
		t.Fatal("file2.txt from tar1 not found")
	}
	if file2FromTar2 == nil {
		t.Fatal("file2.txt from tar2 not found")
	}

	if !file2FromTar1.file.overwritten {
		t.Error("file2.txt from tar1 should be marked as overwritten")
	}
	if file2FromTar2.file.overwritten {
		t.Error("file2.txt from tar2 should NOT be marked as overwritten")
	}
}

func TestBuildSourceInfos_HardlinkConflicts(t *testing.T) {
	// Layer 1:
	//   * sha256 file with hardlink to real name,
	//   * a file that will be overwritten via a hardlink
	tar1Entries := []tarEntry{
		{name: "blobs/sha256/abc123", typeflag: tar.TypeReg, data: []byte("version1")},
		{name: "files/app.bin", typeflag: tar.TypeLink, linkname: "blobs/sha256/abc123"},
		{name: "files/replace-me", typeflag: tar.TypeReg, data: []byte("version1")},
	}
	tar1, err := createTestTar(tar1Entries)
	if err != nil {
		t.Fatalf("Failed to create tar1: %v", err)
	}

	// Layer 2:
	// * different sha256 file with same hardlink name, will not overwrite old blob
	// * a hardlink that overwrites replace-me
	tar2Entries := []tarEntry{
		{name: "blobs/sha256/def456", typeflag: tar.TypeReg, data: []byte("version2")},
		{name: "files/app.bin", typeflag: tar.TypeLink, linkname: "blobs/sha256/def456"},
		{name: "files/other-file", typeflag: tar.TypeReg, data: []byte("version1")},
		{name: "files/replace-me", typeflag: tar.TypeLink, linkname: "files/other-file"},
	}
	tar2, err := createTestTar(tar2Entries)
	if err != nil {
		t.Fatalf("Failed to create tar2: %v", err)
	}

	info1, err := analyzeTar(tar1, false)
	if err != nil {
		t.Fatalf("Failed to analyze tar1: %v", err)
	}

	info2, err := analyzeTar(tar2, false)
	if err != nil {
		t.Fatalf("Failed to analyze tar2: %v", err)
	}

	sourceInfos := buildSourceAnalysis([]*tarInfo{info1, info2}, 2, NewOptions()).sourceInfos

	// Should have 4 files (two from each layer)
	if len(sourceInfos) != 4 {
		t.Fatalf("Expected 4 source infos, got %d", len(sourceInfos))
	}

	// The layer 1 blob file should not be marked as overwritten, even though its hardlink path
	// "files/app.bin" conflicts with layer 2's hardlink path.
	if sourceInfos[0].file.overwritten {
		t.Error("Layer 1 file should not be marked as overwritten due to hardlink path conflict")
	}

	// But the replace-me file should be overwritten, by the repalce-me hardlink
	if !sourceInfos[1].file.overwritten {
		t.Error("Layer 1 file should not be marked as overwritten due to hardlink path conflict")
	}
}
