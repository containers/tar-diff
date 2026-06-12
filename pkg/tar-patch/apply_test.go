package tarpatch

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/containers/tar-diff/pkg/protocol"
	"github.com/klauspost/compress/zstd"
)

// Helper to create delta streams for testing
func createDeltaStream(t *testing.T, ops []deltaOp) *bytes.Buffer {
	t.Helper()

	var buf bytes.Buffer

	// Write header
	_, err := buf.Write(protocol.DeltaHeader[:])
	if err != nil {
		t.Fatalf("failed to write delta header: %v", err)
	}

	// Create zstd encoder
	encoder, err := zstd.NewWriter(&buf)
	if err != nil {
		t.Fatalf("failed to create zstd encoder: %v", err)
	}

	// Write operations
	for _, op := range ops {
		opBuf := make([]byte, 1+binary.MaxVarintLen64)
		opBuf[0] = op.code
		sizeLen := binary.PutUvarint(opBuf[1:], op.size)

		if _, err := encoder.Write(opBuf[:1+sizeLen]); err != nil {
			t.Fatalf("failed to write op code/size: %v", err)
		}

		if op.data != nil {
			if _, err := encoder.Write(op.data); err != nil {
				t.Fatalf("failed to write op data: %v", err)
			}
		}
	}

	if err := encoder.Close(); err != nil {
		t.Fatalf("failed to close encoder: %v", err)
	}

	return &buf
}

type deltaOp struct {
	code byte
	size uint64
	data []byte
}

// Mock data source for testing
type mockDataSource struct {
	files       map[string][]byte
	currentFile string
	currentPos  int64
	openErr     error
	readErr     error
	seekErr     error
}

func newMockDataSource() *mockDataSource {
	return &mockDataSource{
		files: make(map[string][]byte),
	}
}

func (m *mockDataSource) AddFile(name string, content []byte) {
	m.files[name] = content
}

func (m *mockDataSource) SetCurrentFile(file string) error {
	if m.openErr != nil {
		return m.openErr
	}
	if _, exists := m.files[file]; !exists {
		return fmt.Errorf("file not found: %s", file)
	}
	m.currentFile = file
	m.currentPos = 0
	return nil
}

func (m *mockDataSource) Read(p []byte) (int, error) {
	if m.readErr != nil {
		return 0, m.readErr
	}
	if m.currentFile == "" {
		return 0, fmt.Errorf("no current file")
	}

	content := m.files[m.currentFile]
	if m.currentPos >= int64(len(content)) {
		return 0, io.EOF
	}

	n := copy(p, content[m.currentPos:])
	m.currentPos += int64(n)
	return n, nil
}

func (m *mockDataSource) Seek(offset int64, whence int) (int64, error) {
	if m.seekErr != nil {
		return 0, m.seekErr
	}
	if m.currentFile == "" {
		return 0, fmt.Errorf("no current file")
	}

	content := m.files[m.currentFile]
	var newPos int64

	switch whence {
	case io.SeekStart:
		newPos = offset
	case io.SeekCurrent:
		newPos = m.currentPos + offset
	case io.SeekEnd:
		newPos = int64(len(content)) + offset
	default:
		return 0, fmt.Errorf("invalid whence")
	}

	if newPos < 0 {
		return 0, fmt.Errorf("negative position")
	}

	m.currentPos = newPos
	return newPos, nil
}

func (m *mockDataSource) Close() error {
	// Close is not called by Apply(), only by test cleanup
	return nil
}

// Tests for Apply function

func TestApply_DeltaOpData(t *testing.T) {
	testData := []byte("Hello, World!")
	delta := createDeltaStream(t, []deltaOp{
		{code: protocol.DeltaOpData, size: uint64(len(testData)), data: testData},
	})

	var output bytes.Buffer
	ds := newMockDataSource()

	err := Apply(delta, ds, &output)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	if !bytes.Equal(output.Bytes(), testData) {
		t.Errorf("expected output %q, got %q", testData, output.Bytes())
	}
}

func TestApply_DeltaOpOpen(t *testing.T) {
	filename := "test.txt"
	fileContent := []byte("file content")

	delta := createDeltaStream(t, []deltaOp{
		{code: protocol.DeltaOpOpen, size: uint64(len(filename)), data: []byte(filename)},
	})

	var output bytes.Buffer
	ds := newMockDataSource()
	ds.AddFile(filename, fileContent)

	err := Apply(delta, ds, &output)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	if ds.currentFile != filename {
		t.Errorf("expected current file %q, got %q", filename, ds.currentFile)
	}
}

func TestApply_DeltaOpCopy(t *testing.T) {
	filename := "source.txt"
	fileContent := []byte("content to copy")
	copySize := uint64(7) // Copy "content"

	delta := createDeltaStream(t, []deltaOp{
		{code: protocol.DeltaOpOpen, size: uint64(len(filename)), data: []byte(filename)},
		{code: protocol.DeltaOpCopy, size: copySize},
	})

	var output bytes.Buffer
	ds := newMockDataSource()
	ds.AddFile(filename, fileContent)

	err := Apply(delta, ds, &output)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	expected := fileContent[:copySize]
	if !bytes.Equal(output.Bytes(), expected) {
		t.Errorf("expected output %q, got %q", expected, output.Bytes())
	}
}

func TestApply_DeltaOpAddData(t *testing.T) {
	filename := "base.txt"
	baseData := []byte{1, 2, 3, 4, 5}
	addData := []byte{10, 20, 30, 40, 50}
	expected := []byte{11, 22, 33, 44, 55}

	delta := createDeltaStream(t, []deltaOp{
		{code: protocol.DeltaOpOpen, size: uint64(len(filename)), data: []byte(filename)},
		{code: protocol.DeltaOpAddData, size: uint64(len(addData)), data: addData},
	})

	var output bytes.Buffer
	ds := newMockDataSource()
	ds.AddFile(filename, baseData)

	err := Apply(delta, ds, &output)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	if !bytes.Equal(output.Bytes(), expected) {
		t.Errorf("expected output %v, got %v", expected, output.Bytes())
	}
}

func TestApply_DeltaOpAddData_Overflow(t *testing.T) {
	filename := "overflow.txt"
	// Test byte overflow wraps around
	baseData := []byte{255, 254, 253}
	addData := []byte{2, 3, 4}
	expected := []byte{1, 1, 1} // Wraps around at 256

	delta := createDeltaStream(t, []deltaOp{
		{code: protocol.DeltaOpOpen, size: uint64(len(filename)), data: []byte(filename)},
		{code: protocol.DeltaOpAddData, size: uint64(len(addData)), data: addData},
	})

	var output bytes.Buffer
	ds := newMockDataSource()
	ds.AddFile(filename, baseData)

	err := Apply(delta, ds, &output)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	if !bytes.Equal(output.Bytes(), expected) {
		t.Errorf("expected output %v, got %v", expected, output.Bytes())
	}
}

func TestApply_DeltaOpSeek(t *testing.T) {
	filename := "seektest.txt"
	fileContent := []byte("0123456789")
	seekPos := uint64(5)
	copySize := uint64(3)

	delta := createDeltaStream(t, []deltaOp{
		{code: protocol.DeltaOpOpen, size: uint64(len(filename)), data: []byte(filename)},
		{code: protocol.DeltaOpSeek, size: seekPos},
		{code: protocol.DeltaOpCopy, size: copySize},
	})

	var output bytes.Buffer
	ds := newMockDataSource()
	ds.AddFile(filename, fileContent)

	err := Apply(delta, ds, &output)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	expected := []byte("567")
	if !bytes.Equal(output.Bytes(), expected) {
		t.Errorf("expected output %q, got %q", expected, output.Bytes())
	}
}

func TestApply_InvalidHeader(t *testing.T) {
	delta := bytes.NewReader([]byte("invalid header data"))

	var output bytes.Buffer
	ds := newMockDataSource()

	err := Apply(delta, ds, &output)
	if err == nil {
		t.Fatal("expected error for invalid header, got nil")
	}

	if !strings.Contains(err.Error(), "invalid delta format") {
		t.Errorf("expected 'invalid delta format' error, got: %v", err)
	}
}

func TestApply_UnknownOperation(t *testing.T) {
	invalidOp := byte(99) // Not a valid operation
	delta := createDeltaStream(t, []deltaOp{
		{code: invalidOp, size: 0},
	})

	var output bytes.Buffer
	ds := newMockDataSource()

	err := Apply(delta, ds, &output)
	if err == nil {
		t.Fatal("expected error for unknown operation, got nil")
	}

	if !strings.Contains(err.Error(), "unexpected delta op") {
		t.Errorf("expected 'unexpected delta op' error, got: %v", err)
	}
}

func TestApply_FilenameSizeLimit(t *testing.T) {
	// Create a filename larger than maxFilenameSize
	largeFilename := make([]byte, maxFilenameSize+1)
	for i := range largeFilename {
		largeFilename[i] = 'a'
	}

	delta := createDeltaStream(t, []deltaOp{
		{code: protocol.DeltaOpOpen, size: uint64(len(largeFilename)), data: largeFilename},
	})

	var output bytes.Buffer
	ds := newMockDataSource()

	err := Apply(delta, ds, &output)
	if err == nil {
		t.Fatal("expected error for oversized filename, got nil")
	}

	if !strings.Contains(err.Error(), "filename size") && !strings.Contains(err.Error(), "exceeds maximum") {
		t.Errorf("expected 'filename size' error, got: %v", err)
	}
}

func TestApply_AddDataSizeLimit(t *testing.T) {
	filename := "test.txt"

	// Try to allocate more than maxAddDataSize
	excessiveSize := uint64(maxAddDataSize + 1)

	delta := createDeltaStream(t, []deltaOp{
		{code: protocol.DeltaOpOpen, size: uint64(len(filename)), data: []byte(filename)},
		{code: protocol.DeltaOpAddData, size: excessiveSize, data: []byte{1}},
	})

	var output bytes.Buffer
	ds := newMockDataSource()
	ds.AddFile(filename, []byte{1})

	err := Apply(delta, ds, &output)
	if err == nil {
		t.Fatal("expected error for oversized AddData, got nil")
	}

	if !strings.Contains(err.Error(), "AddData") && !strings.Contains(err.Error(), "exceeds maximum") {
		t.Errorf("expected 'AddData' size error, got: %v", err)
	}
}

func TestApply_PathTraversal(t *testing.T) {
	// Create a temp directory as the base
	tempDir := t.TempDir()

	// Get the parent directory and create a secret file there
	parentDir := filepath.Dir(tempDir)
	secretFile := filepath.Join(parentDir, "secret.txt")
	if err := os.WriteFile(secretFile, []byte("SECRET"), 0644); err != nil {
		t.Fatalf("failed to create secret file: %v", err)
	}
	defer func() {
		if err := os.Remove(secretFile); err != nil {
			t.Logf("failed to remove secret file: %v", err)
		}
	}()

	// Create a decoy file inside the base directory
	// CleanPath will convert "../secret.txt" to "secret.txt"
	decoyFile := filepath.Join(tempDir, "secret.txt")
	if err := os.WriteFile(decoyFile, []byte("DECOY"), 0644); err != nil {
		t.Fatalf("failed to create decoy file: %v", err)
	}

	testCases := []struct {
		name        string
		filename    string
		shouldExist bool
	}{
		{
			name:        "parent directory traversal",
			filename:    "../secret.txt",
			shouldExist: true, // CleanPath sanitizes this to "secret.txt" which exists as the decoy
		},
		{
			name:        "absolute path",
			filename:    "/etc/passwd",
			shouldExist: false, // CleanPath sanitizes to "etc/passwd" which doesn't exist
		},
		{
			name:        "complex traversal",
			filename:    "../../secret.txt",
			shouldExist: true, // CleanPath sanitizes to "secret.txt" which exists as the decoy
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			delta := createDeltaStream(t, []deltaOp{
				{code: protocol.DeltaOpOpen, size: uint64(len(tc.filename)), data: []byte(tc.filename)},
				{code: protocol.DeltaOpCopy, size: 5}, // Try to read some data
			})

			var output bytes.Buffer
			ds := NewFilesystemDataSource(tempDir)
			defer func() {
				if err := ds.Close(); err != nil {
					t.Errorf("failed to close data source: %v", err)
				}
			}()

			err := Apply(delta, ds, &output)

			if tc.shouldExist {
				// Should succeed and read from the decoy file (inside tempDir), not the secret file (in parent)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				// Verify it read "DECOY" from inside tempDir, not "SECRET" from parent
				// A broken CleanPath would read "SECRE" (first 5 bytes of "SECRET")
				if !bytes.Equal(output.Bytes(), []byte("DECOY")) {
					t.Errorf("expected to read from decoy inside tempDir, got: %q - possible path traversal vulnerability", output.Bytes())
				}
			} else if err == nil {
				// Should fail because the cleaned path doesn't exist
				t.Fatal("expected error for non-existent path, got nil")
			}
		})
	}
}

func TestApply_MissingFile(t *testing.T) {
	filename := "nonexistent.txt"

	delta := createDeltaStream(t, []deltaOp{
		{code: protocol.DeltaOpOpen, size: uint64(len(filename)), data: []byte(filename)},
	})

	var output bytes.Buffer
	ds := newMockDataSource()

	err := Apply(delta, ds, &output)
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}

func TestApply_ComplexDelta(t *testing.T) {
	file1 := "file1.txt"
	file1Content := []byte("First file content")
	file2 := "file2.txt"
	file2Content := []byte("Second file content")
	rawData := []byte(" and some raw data")

	delta := createDeltaStream(t, []deltaOp{
		{code: protocol.DeltaOpOpen, size: uint64(len(file1)), data: []byte(file1)},
		{code: protocol.DeltaOpCopy, size: 5}, // "First"
		{code: protocol.DeltaOpData, size: uint64(len(rawData)), data: rawData},
		{code: protocol.DeltaOpOpen, size: uint64(len(file2)), data: []byte(file2)},
		{code: protocol.DeltaOpSeek, size: 7}, // Skip to "file"
		{code: protocol.DeltaOpCopy, size: 4}, // "file"
	})

	var output bytes.Buffer
	ds := newMockDataSource()
	ds.AddFile(file1, file1Content)
	ds.AddFile(file2, file2Content)

	err := Apply(delta, ds, &output)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	expected := "First and some raw datafile"
	if output.String() != expected {
		t.Errorf("expected output %q, got %q", expected, output.String())
	}
}

func TestApply_EmptyDelta(t *testing.T) {
	delta := createDeltaStream(t, []deltaOp{})

	var output bytes.Buffer
	ds := newMockDataSource()

	err := Apply(delta, ds, &output)
	if err != nil {
		t.Fatalf("Apply failed on empty delta: %v", err)
	}

	if output.Len() != 0 {
		t.Errorf("expected empty output, got %d bytes", output.Len())
	}
}

// Tests for FilesystemDataSource

func TestFilesystemDataSource_New(t *testing.T) {
	basePath := "/tmp/test"
	ds := NewFilesystemDataSource(basePath)

	if ds == nil {
		t.Fatal("NewFilesystemDataSource returned nil")
	}

	if ds.basePath != basePath {
		t.Errorf("expected basePath %q, got %q", basePath, ds.basePath)
	}

	if ds.currentFile != nil {
		t.Error("expected currentFile to be nil initially")
	}
}

func TestFilesystemDataSource_SetCurrentFile(t *testing.T) {
	tempDir := t.TempDir()
	testFile := "test.txt"
	testContent := []byte("test content")

	filePath := filepath.Join(tempDir, testFile)
	if err := os.WriteFile(filePath, testContent, 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	ds := NewFilesystemDataSource(tempDir)
	defer func() {
		if err := ds.Close(); err != nil {
			t.Errorf("failed to close data source: %v", err)
		}
	}()

	err := ds.SetCurrentFile(testFile)
	if err != nil {
		t.Fatalf("SetCurrentFile failed: %v", err)
	}

	if ds.currentFile == nil {
		t.Error("expected currentFile to be non-nil after SetCurrentFile")
	}
}

func TestFilesystemDataSource_SetCurrentFile_NotFound(t *testing.T) {
	tempDir := t.TempDir()
	ds := NewFilesystemDataSource(tempDir)
	defer func() {
		if err := ds.Close(); err != nil {
			t.Errorf("failed to close data source: %v", err)
		}
	}()

	err := ds.SetCurrentFile("nonexistent.txt")
	if err == nil {
		t.Fatal("expected error for nonexistent file, got nil")
	}
}

func TestFilesystemDataSource_SetCurrentFileCloseError(t *testing.T) {
	tempDir := t.TempDir()
	testFile1 := "test1.txt"
	testFile2 := "test2.txt"

	filePath1 := filepath.Join(tempDir, testFile1)
	filePath2 := filepath.Join(tempDir, testFile2)
	if err := os.WriteFile(filePath1, []byte("content1"), 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	if err := os.WriteFile(filePath2, []byte("content2"), 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	ds := NewFilesystemDataSource(tempDir)
	if err := ds.SetCurrentFile(testFile1); err != nil {
		t.Fatalf("SetCurrentFile failed: %v", err)
	}

	// Close the file directly to force an error when SetCurrentFile tries to close it
	if err := ds.currentFile.Close(); err != nil {
		t.Fatalf("failed to pre-close file: %v", err)
	}

	// Now SetCurrentFile should return an error when trying to close the already-closed file
	err := ds.SetCurrentFile(testFile2)
	if err == nil {
		t.Fatal("expected error from SetCurrentFile when closing already-closed file, got nil")
	}

	if ds.currentFile == nil {
		t.Fatal("currentFile should not be nil after a failed Close()")
	}
}

func TestFilesystemDataSource_Read(t *testing.T) {
	tempDir := t.TempDir()
	testFile := "test.txt"
	testContent := []byte("test content")

	filePath := filepath.Join(tempDir, testFile)
	if err := os.WriteFile(filePath, testContent, 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	ds := NewFilesystemDataSource(tempDir)
	defer func() {
		if err := ds.Close(); err != nil {
			t.Errorf("failed to close data source: %v", err)
		}
	}()

	if err := ds.SetCurrentFile(testFile); err != nil {
		t.Fatalf("SetCurrentFile failed: %v", err)
	}

	buf := make([]byte, len(testContent))
	n, err := ds.Read(buf)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if n != len(testContent) {
		t.Errorf("expected to read %d bytes, got %d", len(testContent), n)
	}

	if !bytes.Equal(buf, testContent) {
		t.Errorf("expected content %q, got %q", testContent, buf)
	}
}

func TestFilesystemDataSource_Read_NoFile(t *testing.T) {
	tempDir := t.TempDir()
	ds := NewFilesystemDataSource(tempDir)
	defer func() {
		if err := ds.Close(); err != nil {
			t.Errorf("failed to close data source: %v", err)
		}
	}()

	buf := make([]byte, 10)
	_, err := ds.Read(buf)
	if err == nil {
		t.Fatal("expected error reading without current file, got nil")
	}

	if !strings.Contains(err.Error(), "no current file") {
		t.Errorf("expected 'no current file' error, got: %v", err)
	}
}

func TestFilesystemDataSource_Seek(t *testing.T) {
	tempDir := t.TempDir()
	testFile := "test.txt"
	testContent := []byte("0123456789")

	filePath := filepath.Join(tempDir, testFile)
	if err := os.WriteFile(filePath, testContent, 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	ds := NewFilesystemDataSource(tempDir)
	defer func() {
		if err := ds.Close(); err != nil {
			t.Errorf("failed to close data source: %v", err)
		}
	}()

	if err := ds.SetCurrentFile(testFile); err != nil {
		t.Fatalf("SetCurrentFile failed: %v", err)
	}

	// Seek to position 5
	pos, err := ds.Seek(5, io.SeekStart)
	if err != nil {
		t.Fatalf("Seek failed: %v", err)
	}

	if pos != 5 {
		t.Errorf("expected position 5, got %d", pos)
	}

	// Read and verify we're at the right position
	buf := make([]byte, 3)
	n, err := ds.Read(buf)
	if err != nil {
		t.Fatalf("Read after Seek failed: %v", err)
	}

	if n != 3 {
		t.Errorf("expected to read 3 bytes, got %d", n)
	}

	expected := []byte("567")
	if !bytes.Equal(buf, expected) {
		t.Errorf("expected %q after seek, got %q", expected, buf)
	}
}

func TestFilesystemDataSource_Seek_NoFile(t *testing.T) {
	tempDir := t.TempDir()
	ds := NewFilesystemDataSource(tempDir)
	defer func() {
		if err := ds.Close(); err != nil {
			t.Errorf("failed to close data source: %v", err)
		}
	}()

	_, err := ds.Seek(0, io.SeekStart)
	if err == nil {
		t.Fatal("expected error seeking without current file, got nil")
	}

	if !strings.Contains(err.Error(), "no current file") {
		t.Errorf("expected 'no current file' error, got: %v", err)
	}
}

func TestFilesystemDataSource_Close(t *testing.T) {
	tempDir := t.TempDir()
	testFile := "test.txt"
	testContent := []byte("test content")

	filePath := filepath.Join(tempDir, testFile)
	if err := os.WriteFile(filePath, testContent, 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	ds := NewFilesystemDataSource(tempDir)

	if err := ds.SetCurrentFile(testFile); err != nil {
		t.Fatalf("SetCurrentFile failed: %v", err)
	}

	err := ds.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	if ds.currentFile != nil {
		t.Error("expected currentFile to be nil after Close")
	}

	// Closing again should not error
	err = ds.Close()
	if err != nil {
		t.Errorf("second Close failed: %v", err)
	}
}

func TestFilesystemDataSource_MultipleSwitches(t *testing.T) {
	tempDir := t.TempDir()

	files := map[string][]byte{
		"file1.txt": []byte("content1"),
		"file2.txt": []byte("content2"),
		"file3.txt": []byte("content3"),
	}

	for name, content := range files {
		filePath := filepath.Join(tempDir, name)
		if err := os.WriteFile(filePath, content, 0644); err != nil {
			t.Fatalf("failed to create test file %s: %v", name, err)
		}
	}

	ds := NewFilesystemDataSource(tempDir)
	defer func() {
		if err := ds.Close(); err != nil {
			t.Errorf("failed to close data source: %v", err)
		}
	}()

	for name, expectedContent := range files {
		if err := ds.SetCurrentFile(name); err != nil {
			t.Fatalf("SetCurrentFile(%s) failed: %v", name, err)
		}

		buf := make([]byte, len(expectedContent))
		n, err := ds.Read(buf)
		if err != nil {
			t.Fatalf("Read from %s failed: %v", name, err)
		}

		if n != len(expectedContent) {
			t.Errorf("file %s: expected to read %d bytes, got %d", name, len(expectedContent), n)
		}

		if !bytes.Equal(buf, expectedContent) {
			t.Errorf("file %s: expected content %q, got %q", name, expectedContent, buf)
		}
	}
}

func TestApply_Integration(t *testing.T) {
	tempDir := t.TempDir()

	file1 := "source1.txt"
	file1Content := []byte("Hello from file 1")
	file2 := "source2.txt"
	file2Content := []byte("Hello from file 2")

	if err := os.WriteFile(filepath.Join(tempDir, file1), file1Content, 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tempDir, file2), file2Content, 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	rawData := []byte(" + ")
	delta := createDeltaStream(t, []deltaOp{
		{code: protocol.DeltaOpOpen, size: uint64(len(file1)), data: []byte(file1)},
		{code: protocol.DeltaOpCopy, size: 5}, // "Hello"
		{code: protocol.DeltaOpData, size: uint64(len(rawData)), data: rawData},
		{code: protocol.DeltaOpOpen, size: uint64(len(file2)), data: []byte(file2)},
		{code: protocol.DeltaOpSeek, size: 11}, // Skip to "file 2"
		{code: protocol.DeltaOpCopy, size: 6},  // "file 2"
	})

	var output bytes.Buffer
	ds := NewFilesystemDataSource(tempDir)
	defer func() {
		if err := ds.Close(); err != nil {
			t.Errorf("failed to close data source: %v", err)
		}
	}()

	err := Apply(delta, ds, &output)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	expected := "Hello + file 2"
	if output.String() != expected {
		t.Errorf("expected output %q, got %q", expected, output.String())
	}
}

// Tests for operations without preceding DeltaOpOpen

func TestApply_CopyWithoutOpen(t *testing.T) {
	delta := createDeltaStream(t, []deltaOp{
		{code: protocol.DeltaOpCopy, size: 5},
	})

	var output bytes.Buffer
	ds := newMockDataSource()

	err := Apply(delta, ds, &output)
	if err == nil {
		t.Fatal("expected error for DeltaOpCopy without preceding DeltaOpOpen, got nil")
	}

	if !strings.Contains(err.Error(), "no current file") {
		t.Errorf("expected 'no current file' error, got: %v", err)
	}
}

func TestApply_AddDataWithoutOpen(t *testing.T) {
	addData := []byte{1, 2, 3}
	delta := createDeltaStream(t, []deltaOp{
		{code: protocol.DeltaOpAddData, size: uint64(len(addData)), data: addData},
	})

	var output bytes.Buffer
	ds := newMockDataSource()

	err := Apply(delta, ds, &output)
	if err == nil {
		t.Fatal("expected error for DeltaOpAddData without preceding DeltaOpOpen, got nil")
	}

	if !strings.Contains(err.Error(), "no current file") {
		t.Errorf("expected 'no current file' error, got: %v", err)
	}
}

func TestApply_SeekWithoutOpen(t *testing.T) {
	delta := createDeltaStream(t, []deltaOp{
		{code: protocol.DeltaOpSeek, size: 5},
	})

	var output bytes.Buffer
	ds := newMockDataSource()

	err := Apply(delta, ds, &output)
	if err == nil {
		t.Fatal("expected error for DeltaOpSeek without preceding DeltaOpOpen, got nil")
	}

	if !strings.Contains(err.Error(), "no current file") {
		t.Errorf("expected 'no current file' error, got: %v", err)
	}
}

// Error injection tests

func TestApply_OpenError(t *testing.T) {
	filename := "test.txt"
	delta := createDeltaStream(t, []deltaOp{
		{code: protocol.DeltaOpOpen, size: uint64(len(filename)), data: []byte(filename)},
	})

	var output bytes.Buffer
	ds := newMockDataSource()
	ds.openErr = fmt.Errorf("permission denied")

	err := Apply(delta, ds, &output)
	if err == nil {
		t.Fatal("expected error from openErr injection, got nil")
	}

	if !strings.Contains(err.Error(), "permission denied") {
		t.Errorf("expected 'permission denied' error, got: %v", err)
	}
}

func TestApply_ReadError(t *testing.T) {
	filename := "test.txt"
	fileContent := []byte("test content")

	delta := createDeltaStream(t, []deltaOp{
		{code: protocol.DeltaOpOpen, size: uint64(len(filename)), data: []byte(filename)},
		{code: protocol.DeltaOpCopy, size: 5},
	})

	var output bytes.Buffer
	ds := newMockDataSource()
	ds.AddFile(filename, fileContent)
	ds.readErr = fmt.Errorf("disk read failure")

	err := Apply(delta, ds, &output)
	if err == nil {
		t.Fatal("expected error from readErr injection, got nil")
	}

	if !strings.Contains(err.Error(), "disk read failure") {
		t.Errorf("expected 'disk read failure' error, got: %v", err)
	}
}

func TestApply_SeekError(t *testing.T) {
	filename := "test.txt"
	fileContent := []byte("test content")

	delta := createDeltaStream(t, []deltaOp{
		{code: protocol.DeltaOpOpen, size: uint64(len(filename)), data: []byte(filename)},
		{code: protocol.DeltaOpSeek, size: 5},
	})

	var output bytes.Buffer
	ds := newMockDataSource()
	ds.AddFile(filename, fileContent)
	ds.seekErr = fmt.Errorf("seek failed")

	err := Apply(delta, ds, &output)
	if err == nil {
		t.Fatal("expected error from seekErr injection, got nil")
	}

	if !strings.Contains(err.Error(), "seek failed") {
		t.Errorf("expected 'seek failed' error, got: %v", err)
	}
}

func TestApply_ReadErrorDuringAddData(t *testing.T) {
	filename := "test.txt"
	fileContent := []byte{1, 2, 3}
	addData := []byte{10, 20, 30}

	delta := createDeltaStream(t, []deltaOp{
		{code: protocol.DeltaOpOpen, size: uint64(len(filename)), data: []byte(filename)},
		{code: protocol.DeltaOpAddData, size: uint64(len(addData)), data: addData},
	})

	var output bytes.Buffer
	ds := newMockDataSource()
	ds.AddFile(filename, fileContent)
	ds.readErr = fmt.Errorf("I/O error during read")

	err := Apply(delta, ds, &output)
	if err == nil {
		t.Fatal("expected error from readErr during AddData, got nil")
	}

	if !strings.Contains(err.Error(), "I/O error during read") {
		t.Errorf("expected 'I/O error during read' error, got: %v", err)
	}
}

// Edge case tests

func TestApply_DataWithZeroSize(t *testing.T) {
	delta := createDeltaStream(t, []deltaOp{
		{code: protocol.DeltaOpData, size: 0, data: []byte{}},
	})

	var output bytes.Buffer
	ds := newMockDataSource()

	err := Apply(delta, ds, &output)
	if err != nil {
		t.Fatalf("Apply failed on zero-size DeltaOpData: %v", err)
	}

	if output.Len() != 0 {
		t.Errorf("expected empty output, got %d bytes", output.Len())
	}
}

func TestApply_CopyWithZeroSize(t *testing.T) {
	filename := "test.txt"
	fileContent := []byte("test content")

	delta := createDeltaStream(t, []deltaOp{
		{code: protocol.DeltaOpOpen, size: uint64(len(filename)), data: []byte(filename)},
		{code: protocol.DeltaOpCopy, size: 0},
	})

	var output bytes.Buffer
	ds := newMockDataSource()
	ds.AddFile(filename, fileContent)

	err := Apply(delta, ds, &output)
	if err != nil {
		t.Fatalf("Apply failed on zero-size DeltaOpCopy: %v", err)
	}

	if output.Len() != 0 {
		t.Errorf("expected empty output, got %d bytes", output.Len())
	}
}

func TestApply_OpenEmptyFilename(t *testing.T) {
	delta := createDeltaStream(t, []deltaOp{
		{code: protocol.DeltaOpOpen, size: 0, data: []byte{}},
	})

	var output bytes.Buffer
	ds := newMockDataSource()

	err := Apply(delta, ds, &output)
	if err == nil {
		t.Fatal("expected error for empty filename, got nil")
	}

	if !strings.Contains(err.Error(), "invalid source name") {
		t.Errorf("expected 'invalid source name' error, got: %v", err)
	}
}

func TestApply_SeekPastEndThenCopy(t *testing.T) {
	filename := "test.txt"
	fileContent := []byte("short")

	delta := createDeltaStream(t, []deltaOp{
		{code: protocol.DeltaOpOpen, size: uint64(len(filename)), data: []byte(filename)},
		{code: protocol.DeltaOpSeek, size: 100}, // Seek past the end
		{code: protocol.DeltaOpCopy, size: 5},   // Try to copy
	})

	var output bytes.Buffer
	ds := newMockDataSource()
	ds.AddFile(filename, fileContent)

	err := Apply(delta, ds, &output)
	if err == nil {
		t.Fatal("expected error when copying after seeking past end of file, got nil")
	}

	// Should get EOF error when trying to copy from past the end
	if err != io.EOF && !strings.Contains(err.Error(), "EOF") {
		t.Errorf("expected 'EOF' error, got: %v", err)
	}
}
