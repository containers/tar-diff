package tardiff

import (
	"archive/tar"
	"io"
	"testing"
)

func TestMatchesAnyPrefix(t *testing.T) {
	tests := []struct {
		path     string
		prefixes []string
		want     bool
	}{
		{"blobs/sha256/abc123", []string{"blobs/"}, true},
		{"config/app.conf", []string{"blobs/"}, false},
		{"data/file.txt", []string{"blobs/"}, false},
		{"blobs/sha256/abc123", []string{"blobs/", "config/"}, true},
		{"config/app.conf", []string{"blobs/", "config/"}, true},
		{"data/file.txt", []string{"blobs/", "config/"}, false},
		{"anything", []string{}, true}, // empty prefixes means match all
		{"anything", nil, true},        // nil prefixes means match all
	}

	for _, tt := range tests {
		got := matchesAnyPrefix(tt.path, tt.prefixes)
		if got != tt.want {
			t.Errorf("matchesAnyPrefix(%q, %v) = %v, want %v", tt.path, tt.prefixes, got, tt.want)
		}
	}
}

func setupPrefixFilterTestData(t *testing.T) (oldTar io.ReadSeeker, oldTarInfo *tarInfo, newTar io.ReadSeeker, newInfo *tarInfo) {
	oldEntries := []tarEntry{
		{name: "blobs/sha256/abc123", typeflag: tar.TypeReg, data: []byte("blob-content")},
		{name: "config/app.conf", typeflag: tar.TypeReg, data: []byte("config-v1")},
		{name: "data/file.txt", typeflag: tar.TypeReg, data: []byte("data-v1")},
	}
	oldTar, err := createTestTar(oldEntries)
	if err != nil {
		t.Fatalf("Failed to create oldTar: %v", err)
	}

	// New tar: same names, but all files modified
	newEntries := []tarEntry{
		{name: "blobs/sha256/abc123", typeflag: tar.TypeReg, data: []byte("blob-content-modified")},
		{name: "config/app.conf", typeflag: tar.TypeReg, data: []byte("config-v2")},
		{name: "data/file.txt", typeflag: tar.TypeReg, data: []byte("data-v2")},
	}
	newTar, err = createTestTar(newEntries)
	if err != nil {
		t.Fatalf("Failed to create new tar: %v", err)
	}

	oldTarInfo, err = analyzeTar(oldTar, false)
	if err != nil {
		t.Fatalf("Failed to analyze oldTar: %v", err)
	}
	if _, err := oldTar.Seek(0, 0); err != nil {
		t.Fatalf("oldTar.Seek: %v", err)
	}

	newInfo, err = analyzeTar(newTar, false)
	if err != nil {
		t.Fatalf("Failed to analyze new tar: %v", err)
	}
	if _, err := newTar.Seek(0, 0); err != nil {
		t.Fatalf("newTar.Seek: %v", err)
	}

	return oldTar, oldTarInfo, newTar, newInfo
}

func TestDiff_SourcePrefix(t *testing.T) {
	old, oldInfo, _, newInfo := setupPrefixFilterTestData(t)

	options := NewOptions()
	options.SetSourcePrefixes([]string{"blobs/"})

	analysis, err := analyzeForDelta(buildSourceAnalysis([]*tarInfo{oldInfo}, 1, options), newInfo, []io.ReadSeeker{old}, nil)
	if err != nil {
		t.Fatalf("analyzeForDelta failed: %v", err)
	}
	defer func() { _ = analysis.Close() }()

	// Verify that only files with blobs/ prefix can be used as delta sources
	if len(analysis.targetInfos) != 3 {
		t.Fatalf("Expected 3 target infos, got %d", len(analysis.targetInfos))
	}

	for i := range analysis.targetInfos {
		target := &analysis.targetInfos[i]
		if target.file == nil {
			continue
		}

		fileName := target.file.paths[0]
		source := target.source

		switch fileName {
		case "blobs/sha256/abc123":
			// Should have a source (matches prefix)
			if source == nil {
				t.Error("blobs/sha256/abc123 should have a source (matches prefix)")
			} else if sdi := analysis.sourceDataInfos[source]; sdi == nil || !sdi.usedForDelta {
				t.Error("blobs/sha256/abc123 source should be usedForDelta")
			}

		default:
			// Should NOT have a source (doesn't match prefix)
			if source != nil {
				t.Errorf("%s should NOT have a source (doesn't match prefix)", fileName)
			}
		}
	}
}

func TestDiff_SourceMultiplePrefixes(t *testing.T) {
	old, oldInfo, _, newInfo := setupPrefixFilterTestData(t)

	options := NewOptions()
	options.SetSourcePrefixes([]string{"blobs/", "config/"})

	analysis, err := analyzeForDelta(buildSourceAnalysis([]*tarInfo{oldInfo}, 1, options), newInfo, []io.ReadSeeker{old}, nil)
	if err != nil {
		t.Fatalf("analyzeForDelta failed: %v", err)
	}
	defer func() { _ = analysis.Close() }()

	// Verify correct filtering
	for i := range analysis.targetInfos {
		target := &analysis.targetInfos[i]
		if target.file == nil {
			continue
		}

		fileName := target.file.paths[0]
		source := target.source

		switch fileName {
		case "blobs/sha256/abc123", "config/app.conf":
			// Should have a source (matches one of the prefixes)
			if source == nil {
				t.Errorf("%s should have a source (matches prefix)", fileName)
			}

		default:
			// Should NOT have a source (doesn't match any prefix)
			if source != nil {
				t.Error("data/file.txt should NOT have a source (doesn't match any prefix)")
			}
		}
	}
}

func TestDiff_NoPrefixFilter(t *testing.T) {
	old, oldInfo, _, newInfo := setupPrefixFilterTestData(t)

	// No prefix filter (default) - pass nil for default options
	analysis, err := analyzeForDelta(buildSourceAnalysis([]*tarInfo{oldInfo}, 1, nil), newInfo, []io.ReadSeeker{old}, nil)
	if err != nil {
		t.Fatalf("analyzeForDelta failed: %v", err)
	}
	defer func() { _ = analysis.Close() }()

	// All files should have sources
	sourcesFound := 0
	for i := range analysis.targetInfos {
		target := &analysis.targetInfos[i]
		if target.file != nil && target.source != nil {
			sourcesFound++
		}
	}

	if sourcesFound != 3 {
		t.Errorf("Expected 3 files to have sources, got %d", sourcesFound)
	}
}
