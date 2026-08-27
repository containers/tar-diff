package tardiff

import (
	"bytes"
	"testing"
)

func TestMinInt(t *testing.T) {
	tests := []struct {
		a, b, expected int
	}{
		{1, 2, 1},
		{5, 3, 3},
		{10, 10, 10},
	}

	for _, test := range tests {
		result := minInt(test.a, test.b)
		if result != test.expected {
			t.Errorf("minInt(%d, %d) = %d, expected %d", test.a, test.b, result, test.expected)
		}
	}
}

func TestMatchlen(t *testing.T) {
	tests := []struct {
		oldbin   []byte
		newbin   []byte
		expected int
	}{
		{[]byte("hello"), []byte("hello"), 5},
		{[]byte("hello"), []byte("help"), 3},
		{[]byte(""), []byte("test"), 0},
		{[]byte("test"), []byte(""), 0},
		{[]byte("abc"), []byte("xyz"), 0},
	}

	for _, test := range tests {
		result := matchlen(test.oldbin, test.newbin)
		if result != test.expected {
			t.Errorf("matchlen(%q, %q) = %d, expected %d", test.oldbin, test.newbin, result, test.expected)
		}
	}
}

func TestQsufsort(t *testing.T) {
	buf := []byte("banana")
	iii := make([]int, len(buf)+1)

	qsufsort(iii, buf)

	if len(iii) != len(buf)+1 {
		t.Errorf("Expected iii length %d, got %d", len(buf)+1, len(iii))
	}

	// Verify all indices are within valid range
	for i, idx := range iii {
		if idx < -1 || idx > len(buf) {
			t.Errorf("iii[%d] = %d is out of range [-1, %d]", i, idx, len(buf))
		}
	}
}

func TestSearch(t *testing.T) {
	oldbin := []byte("abcdef")
	iii := make([]int, len(oldbin)+1)
	qsufsort(iii, oldbin)

	// Test exact prefix match
	var pos int
	length := search(iii, oldbin, []byte("abc"), 0, len(oldbin), &pos)
	if length != 3 || pos != 0 {
		t.Errorf("Expected match length 3 at position 0, got length %d at position %d", length, pos)
	}

	// Verify actual match content
	if pos >= 0 && pos+length <= len(oldbin) {
		actualMatch := oldbin[pos : pos+length]
		if string(actualMatch) != "abc" {
			t.Errorf("Match content mismatch: got %q, expected %q", actualMatch, "abc")
		}
	}

	// Test no match case
	length = search(iii, oldbin, []byte("xyz"), 0, len(oldbin), &pos)
	if length != 0 {
		t.Errorf("Expected no match (length 0), got length %d", length)
	}
}

func TestBsdiffBasic(t *testing.T) {
	// Note: These bsdiff tests verify that delta generation completes without error
	// and produces output, but cannot verify correctness without a bspatch implementation.
	// The bsdiff algorithm is well-tested upstream; these tests ensure integration works.
	var output bytes.Buffer
	deltaWriter, err := newDeltaWriter(&output, 1, deltaFormatV1)
	if err != nil {
		t.Fatalf("Failed to create delta writer: %v", err)
	}
	defer func() {
		if err := deltaWriter.Close(); err != nil {
			t.Logf("Failed to close deltaWriter: %v", err)
		}
	}()

	oldbin := []byte("hello world")
	newbin := []byte("hello mars!")

	err = bsdiff(oldbin, newbin, deltaWriter)
	if err != nil {
		t.Fatalf("bsdiff failed: %v", err)
	}

	// Close to flush any buffered data
	if err := deltaWriter.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify delta was generated
	if output.Len() == 0 {
		t.Error("Expected non-empty delta output")
	}

	t.Logf("Generated %d byte delta for %d byte change", output.Len(), len(newbin))
}

func TestBsdiffIdentical(t *testing.T) {
	var output bytes.Buffer
	deltaWriter, err := newDeltaWriter(&output, 1, deltaFormatV1)
	if err != nil {
		t.Fatalf("Failed to create delta writer: %v", err)
	}
	defer func() {
		if err := deltaWriter.Close(); err != nil {
			t.Logf("Failed to close deltaWriter: %v", err)
		}
	}()

	data := []byte("identical data")

	err = bsdiff(data, data, deltaWriter)
	if err != nil {
		t.Fatalf("bsdiff failed with identical data: %v", err)
	}

	// Close to flush
	if err := deltaWriter.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// For identical data, delta should be generated
	if output.Len() == 0 {
		t.Error("Expected non-empty delta even for identical data")
	}

	t.Logf("Generated %d byte delta for identical %d byte file", output.Len(), len(data))
}

func TestBsdiffEmpty(t *testing.T) {
	var output bytes.Buffer
	deltaWriter, err := newDeltaWriter(&output, 1, deltaFormatV1)
	if err != nil {
		t.Fatalf("Failed to create delta writer: %v", err)
	}
	defer func() {
		if err := deltaWriter.Close(); err != nil {
			t.Logf("Failed to close deltaWriter: %v", err)
		}
	}()

	oldbin := []byte("")
	newbin := []byte("new content")

	err = bsdiff(oldbin, newbin, deltaWriter)
	if err != nil {
		t.Fatalf("bsdiff failed with empty oldbin: %v", err)
	}

	// Close to flush
	if err := deltaWriter.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify delta was generated
	if output.Len() == 0 {
		t.Error("Expected non-empty delta output")
	}

	// With empty old, delta should contain the new data
	// It should be at least as large as the new content (plus some overhead)
	if output.Len() < len(newbin) {
		t.Errorf("Delta too small: %d bytes for %d byte new file", output.Len(), len(newbin))
	}

	t.Logf("Generated %d byte delta for %d byte file (from empty)", output.Len(), len(newbin))
}

func TestBsdiffLargeData(t *testing.T) {
	var output bytes.Buffer
	deltaWriter, err := newDeltaWriter(&output, 1, deltaFormatV1)
	if err != nil {
		t.Fatalf("Failed to create delta writer: %v", err)
	}
	defer func() {
		if err := deltaWriter.Close(); err != nil {
			t.Logf("Failed to close deltaWriter: %v", err)
		}
	}()

	oldbin := bytes.Repeat([]byte("old pattern "), 100)
	newbin := bytes.Repeat([]byte("new pattern "), 100)

	err = bsdiff(oldbin, newbin, deltaWriter)
	if err != nil {
		t.Fatalf("bsdiff failed with large data: %v", err)
	}

	// Close to flush
	if err := deltaWriter.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify delta was generated
	if output.Len() == 0 {
		t.Error("Expected non-empty delta output for large data")
	}

	// Delta should exist and be reasonable size
	if output.Len() > len(newbin) {
		t.Logf("Note: Delta (%d bytes) larger than new file (%d bytes) - expected for completely different patterns",
			output.Len(), len(newbin))
	}

	t.Logf("Generated %d byte delta for %d byte file", output.Len(), len(newbin))
}

func TestSplitFunction(t *testing.T) {
	// split is an internal helper for suffix array sorting in qsufsort.
	// Testing the exact sorting invariants is complex and tightly coupled to the algorithm.
	// This test serves as a smoke test to ensure split doesn't panic on valid input.
	bufsize := 10
	iii := make([]int, bufsize+1)
	vvv := make([]int, bufsize+1)

	// Initialize with sequential values
	for i := 0; i < bufsize+1; i++ {
		iii[i] = i
		vvv[i] = i
	}

	// Call split with valid parameters (start, length, h)
	// This exercises the function without asserting sorting correctness,
	// which is better tested through the full qsufsort -> bsdiff integration tests
	split(iii, vvv, 0, bufsize, 0)

	// Verify split completed without panicking
	t.Logf("split completed successfully with bufsize=%d", bufsize)
}

func TestBsdiffPartialMatch(t *testing.T) {
	var output bytes.Buffer
	deltaWriter, err := newDeltaWriter(&output, 1, deltaFormatV1)
	if err != nil {
		t.Fatalf("Failed to create delta writer: %v", err)
	}
	defer func() {
		if err := deltaWriter.Close(); err != nil {
			t.Logf("Failed to close deltaWriter: %v", err)
		}
	}()

	oldbin := []byte("The quick brown fox jumps over the lazy dog")
	newbin := []byte("The quick red fox runs over the lazy cat")

	err = bsdiff(oldbin, newbin, deltaWriter)
	if err != nil {
		t.Fatalf("bsdiff failed with partial match: %v", err)
	}

	// Close to flush
	if err := deltaWriter.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify delta was generated
	if output.Len() == 0 {
		t.Error("Expected non-empty delta output for partial match")
	}

	t.Logf("Generated %d byte delta for %d byte file (partial match)", output.Len(), len(newbin))
}
