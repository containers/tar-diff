package tardiff

import (
	"bytes"
	"github.com/containers/tar-diff/pkg/protocol"
	"testing"
)

func TestNewDeltaWriter(t *testing.T) {
	var output bytes.Buffer

	deltaWriter, err := newDeltaWriter(&output, 1, deltaFormatV1)
	if err != nil {
		t.Fatalf("newDeltaWriter failed: %v", err)
	}
	defer func() {
		if err := deltaWriter.Close(); err != nil {
			t.Logf("Failed to close deltaWriter: %v", err)
		}
	}()

	if deltaWriter == nil {
		t.Fatal("newDeltaWriter returned nil")
	}

	if deltaWriter.writer == nil {
		t.Error("deltaWriter.writer is nil")
	}

	if deltaWriter.buffer == nil {
		t.Error("deltaWriter.buffer is nil")
	}

	// Check that header was written
	if output.Len() < len(protocol.DeltaHeader) {
		t.Errorf("Expected at least %d bytes written (header), got %d", len(protocol.DeltaHeader), output.Len())
	}
}

func TestDeltaWriterClose(t *testing.T) {
	var output bytes.Buffer

	deltaWriter, err := newDeltaWriter(&output, 1, deltaFormatV1)
	if err != nil {
		t.Fatalf("newDeltaWriter failed: %v", err)
	}

	// Write some data to buffer
	testData := []byte("test data")
	err = deltaWriter.WriteContent(testData)
	if err != nil {
		t.Fatalf("WriteContent failed: %v", err)
	}

	initialLen := output.Len()

	// Close should automatically flush buffered data
	err = deltaWriter.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// Verify buffered data was flushed to output
	if output.Len() <= initialLen {
		t.Error("Close should have flushed buffered data to output")
	}

	// Second close should not fail
	err = deltaWriter.Close()
	if err != nil {
		t.Errorf("Second Close failed: %v", err)
	}
}

func TestDeltaWriterWriteContent(t *testing.T) {
	var output bytes.Buffer

	deltaWriter, err := newDeltaWriter(&output, 1, deltaFormatV1)
	if err != nil {
		t.Fatalf("newDeltaWriter failed: %v", err)
	}
	defer func() {
		if err := deltaWriter.Close(); err != nil {
			t.Logf("Failed to close deltaWriter: %v", err)
		}
	}()

	testData := []byte("Hello, World!")

	err = deltaWriter.WriteContent(testData)
	if err != nil {
		t.Errorf("WriteContent failed: %v", err)
	}

	if len(deltaWriter.buffer) != len(testData) {
		t.Errorf("Expected buffer length %d, got %d", len(testData), len(deltaWriter.buffer))
	}

	if !bytes.Equal(deltaWriter.buffer, testData) {
		t.Errorf("Buffer content doesn't match written data")
	}
}

func TestDeltaWriterFlushBuffer(t *testing.T) {
	var output bytes.Buffer

	deltaWriter, err := newDeltaWriter(&output, 1, deltaFormatV1)
	if err != nil {
		t.Fatalf("newDeltaWriter failed: %v", err)
	}
	defer func() {
		if err := deltaWriter.Close(); err != nil {
			t.Logf("Failed to close deltaWriter: %v", err)
		}
	}()

	testData := []byte("Test data for flushing")
	deltaWriter.buffer = append(deltaWriter.buffer, testData...)

	err = deltaWriter.FlushBuffer()
	if err != nil {
		t.Errorf("FlushBuffer failed: %v", err)
	}

	if len(deltaWriter.buffer) != 0 {
		t.Errorf("Expected buffer to be empty after flush, got length %d", len(deltaWriter.buffer))
	}
}

func TestDeltaWriterSetCurrentFile(t *testing.T) {
	var output bytes.Buffer

	deltaWriter, err := newDeltaWriter(&output, 1, deltaFormatV1)
	if err != nil {
		t.Fatalf("newDeltaWriter failed: %v", err)
	}
	defer func() {
		if err := deltaWriter.Close(); err != nil {
			t.Logf("Failed to close deltaWriter: %v", err)
		}
	}()

	filename := "test.txt"
	err = deltaWriter.SetCurrentFile(filename)
	if err != nil {
		t.Errorf("SetCurrentFile failed: %v", err)
	}

	if deltaWriter.currentFile != filename {
		t.Errorf("Expected currentFile %s, got %s", filename, deltaWriter.currentFile)
	}

	if deltaWriter.currentPos != 0 {
		t.Errorf("Expected currentPos 0 after SetCurrentFile, got %d", deltaWriter.currentPos)
	}
}

func TestDeltaWriterSeek(t *testing.T) {
	var output bytes.Buffer

	deltaWriter, err := newDeltaWriter(&output, 1, deltaFormatV1)
	if err != nil {
		t.Fatalf("newDeltaWriter failed: %v", err)
	}
	defer func() {
		if err := deltaWriter.Close(); err != nil {
			t.Logf("Failed to close deltaWriter: %v", err)
		}
	}()

	pos := uint64(1024)
	err = deltaWriter.Seek(pos)
	if err != nil {
		t.Errorf("Seek failed: %v", err)
	}

	if deltaWriter.currentPos != pos {
		t.Errorf("Expected currentPos %d, got %d", pos, deltaWriter.currentPos)
	}
}

func TestDeltaWriterSeekForward(t *testing.T) {
	var output bytes.Buffer

	deltaWriter, err := newDeltaWriter(&output, 1, deltaFormatV1)
	if err != nil {
		t.Fatalf("newDeltaWriter failed: %v", err)
	}
	defer func() {
		if err := deltaWriter.Close(); err != nil {
			t.Logf("Failed to close deltaWriter: %v", err)
		}
	}()

	initialPos := deltaWriter.currentPos
	offset := uint64(500)

	err = deltaWriter.SeekForward(offset)
	if err != nil {
		t.Errorf("SeekForward failed: %v", err)
	}

	expectedPos := initialPos + offset
	if deltaWriter.currentPos != expectedPos {
		t.Errorf("Expected currentPos %d, got %d", expectedPos, deltaWriter.currentPos)
	}
}

func TestDeltaWriterCopyFile(t *testing.T) {
	var output bytes.Buffer

	deltaWriter, err := newDeltaWriter(&output, 1, deltaFormatV1)
	if err != nil {
		t.Fatalf("newDeltaWriter failed: %v", err)
	}
	defer func() {
		if err := deltaWriter.Close(); err != nil {
			t.Logf("Failed to close deltaWriter: %v", err)
		}
	}()

	size := uint64(2048)
	initialPos := deltaWriter.currentPos

	err = deltaWriter.CopyFile(size)
	if err != nil {
		t.Errorf("CopyFile failed: %v", err)
	}

	expectedPos := initialPos + size
	if deltaWriter.currentPos != expectedPos {
		t.Errorf("Expected currentPos %d after CopyFile, got %d", expectedPos, deltaWriter.currentPos)
	}
}

func TestDeltaWriterWriteAddContent(t *testing.T) {
	var output bytes.Buffer

	deltaWriter, err := newDeltaWriter(&output, 1, deltaFormatV1)
	if err != nil {
		t.Fatalf("newDeltaWriter failed: %v", err)
	}
	defer func() {
		if err := deltaWriter.Close(); err != nil {
			t.Logf("Failed to close deltaWriter: %v", err)
		}
	}()

	data := []byte("Additional content")
	initialPos := deltaWriter.currentPos

	err = deltaWriter.WriteAddContent(data)
	if err != nil {
		t.Errorf("WriteAddContent failed: %v", err)
	}

	expectedPos := initialPos + uint64(len(data))
	if deltaWriter.currentPos != expectedPos {
		t.Errorf("Expected currentPos %d after WriteAddContent, got %d", expectedPos, deltaWriter.currentPos)
	}
}

func TestDeltaWriterWriteOldFile(t *testing.T) {
	var output bytes.Buffer

	deltaWriter, err := newDeltaWriter(&output, 1, deltaFormatV1)
	if err != nil {
		t.Fatalf("newDeltaWriter failed: %v", err)
	}
	defer func() {
		if err := deltaWriter.Close(); err != nil {
			t.Logf("Failed to close deltaWriter: %v", err)
		}
	}()

	filename := "oldfile.txt"
	size := uint64(1024)

	err = deltaWriter.WriteOldFile(filename, size)
	if err != nil {
		t.Errorf("WriteOldFile failed: %v", err)
	}

	if deltaWriter.currentFile != filename {
		t.Errorf("Expected currentFile %s, got %s", filename, deltaWriter.currentFile)
	}

	if deltaWriter.currentPos != size {
		t.Errorf("Expected currentPos %d after WriteOldFile, got %d", size, deltaWriter.currentPos)
	}
}

func TestDeltaWriterWrite(t *testing.T) {
	var output bytes.Buffer

	deltaWriter, err := newDeltaWriter(&output, 1, deltaFormatV1)
	if err != nil {
		t.Fatalf("newDeltaWriter failed: %v", err)
	}
	defer func() {
		if err := deltaWriter.Close(); err != nil {
			t.Logf("Failed to close deltaWriter: %v", err)
		}
	}()

	data := []byte("Test data for Write method")

	n, err := deltaWriter.Write(data)
	if err != nil {
		t.Errorf("Write failed: %v", err)
	}

	if n != len(data) {
		t.Errorf("Expected to write %d bytes, wrote %d", len(data), n)
	}

	if len(deltaWriter.buffer) != len(data) {
		t.Errorf("Expected buffer length %d, got %d", len(data), len(deltaWriter.buffer))
	}
}

func TestDeltaWriterCopyFileAt(t *testing.T) {
	var output bytes.Buffer

	deltaWriter, err := newDeltaWriter(&output, 1, deltaFormatV1)
	if err != nil {
		t.Fatalf("newDeltaWriter failed: %v", err)
	}
	defer func() {
		if err := deltaWriter.Close(); err != nil {
			t.Logf("Failed to close deltaWriter: %v", err)
		}
	}()

	offset := uint64(1000)
	size := uint64(512)

	err = deltaWriter.CopyFileAt(offset, size)
	if err != nil {
		t.Errorf("CopyFileAt failed: %v", err)
	}

	expectedPos := offset + size
	if deltaWriter.currentPos != expectedPos {
		t.Errorf("Expected currentPos %d after CopyFileAt, got %d", expectedPos, deltaWriter.currentPos)
	}
}

func TestDeltaWriterWriteOp(t *testing.T) {
	var output bytes.Buffer

	deltaWriter, err := newDeltaWriter(&output, 1, deltaFormatV1)
	if err != nil {
		t.Fatalf("newDeltaWriter failed: %v", err)
	}
	defer func() {
		if err := deltaWriter.Close(); err != nil {
			t.Logf("Failed to close deltaWriter: %v", err)
		}
	}()

	initialLen := output.Len()
	op := uint8(protocol.DeltaOpData)
	size := uint64(100)
	data := []byte("test data")

	err = deltaWriter.writeOp(op, size, data)
	if err != nil {
		t.Errorf("writeOp failed: %v", err)
	}

	// Flush the buffer by closing
	if err := deltaWriter.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify something was written to the output after flush
	if output.Len() <= initialLen {
		t.Errorf("writeOp did not write to output after flush: before=%d, after=%d", initialLen, output.Len())
	}

	t.Logf("writeOp wrote %d bytes (op + data + header)", output.Len()-initialLen)
}

func TestDeltaWriterV2Header(t *testing.T) {
	var output bytes.Buffer

	deltaWriter, err := newDeltaWriter(&output, 1, deltaFormatV2)
	if err != nil {
		t.Fatalf("newDeltaWriter failed: %v", err)
	}
	if err := deltaWriter.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if !bytes.Equal(output.Bytes()[:len(protocol.DeltaHeaderv2)], protocol.DeltaHeaderv2[:]) {
		t.Fatalf("expected v2 header, got %q", output.Bytes()[:8])
	}
}

func TestDeltaWriterLargeContent(t *testing.T) {
	var output bytes.Buffer

	deltaWriter, err := newDeltaWriter(&output, 1, deltaFormatV1)
	if err != nil {
		t.Fatalf("newDeltaWriter failed: %v", err)
	}
	// Create data larger than deltaDataChunkSize to test automatic flushing
	largeData := make([]byte, deltaDataChunkSize+1000)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	initialOutputLen := output.Len()

	err = deltaWriter.WriteContent(largeData)
	if err != nil {
		t.Errorf("WriteContent failed with large data: %v", err)
	}

	// Close the delta writer to ensure all compression is complete before checking output
	err = deltaWriter.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// Verify that data was actually written to output
	if output.Len() <= initialOutputLen {
		t.Error("Expected large content to trigger buffer flush and write to output")
	}
}

func TestDeltaWriterSetCurrentFileTwice(t *testing.T) {
	var output bytes.Buffer

	deltaWriter, err := newDeltaWriter(&output, 1, deltaFormatV1)
	if err != nil {
		t.Fatalf("newDeltaWriter failed: %v", err)
	}
	defer func() {
		if err := deltaWriter.Close(); err != nil {
			t.Logf("Failed to close deltaWriter: %v", err)
		}
	}()

	// Set file first time
	err = deltaWriter.SetCurrentFile("file1.txt")
	if err != nil {
		t.Errorf("First SetCurrentFile failed: %v", err)
	}

	// Set different file - should trigger flush
	err = deltaWriter.SetCurrentFile("file2.txt")
	if err != nil {
		t.Errorf("Second SetCurrentFile failed: %v", err)
	}

	if deltaWriter.currentFile != "file2.txt" {
		t.Errorf("Expected currentFile 'file2.txt', got %s", deltaWriter.currentFile)
	}
}
