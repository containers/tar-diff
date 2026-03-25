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

	newInfo, err := analyzeTar(newTar)
	if err != nil {
		t.Fatalf("analyzeTar (new) failed: %v", err)
	}

	oldInfo, err := analyzeTar(oldTar)
	if err != nil {
		t.Fatalf("analyzeTar (old) failed: %v", err)
	}

	// Create delta analysis
	if _, err := oldTar.Seek(0, 0); err != nil {
		t.Fatalf("oldTar.Seek: %v", err)
	}
	analysis, err := analyzeForDelta([]*tarInfo{oldInfo}, newInfo, []io.ReadSeeker{oldTar})
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

	newInfo, err := analyzeTar(newTar)
	if err != nil {
		t.Fatalf("analyzeTar (new) failed: %v", err)
	}

	oldInfo, err := analyzeTar(oldTar)
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
	analysis, err := analyzeForDelta([]*tarInfo{oldInfo}, newInfo, []io.ReadSeeker{oldTar})
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
