package tar_diff

import (
	"encoding/binary"
	"github.com/containers/tar-diff/pkg/common"
	"github.com/klauspost/compress/zstd"
	"io"
)

const (
	deltaDataChunkSize = 4 * 1024 * 1024
)

type deltaWriter struct {
	writer      *zstd.Encoder
	buffer      []byte
	currentFile string
	currentPos  uint64
}

func newDeltaWriter(writer io.Writer, compressionLevel int) (*deltaWriter, error) {
	_, err := writer.Write(common.DeltaHeader[:])
	if err != nil {
		return nil, err
	}

	encoder, err := zstd.NewWriter(writer, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(compressionLevel)))
	if err != nil {
		return nil, err
	}
	d := deltaWriter{writer: encoder, buffer: make([]byte, 0, deltaDataChunkSize)}
	return &d, nil
}

func (d *deltaWriter) writeOp(op uint8, size uint64, data []byte) error {
	buf := make([]byte, 1+binary.MaxVarintLen64)
	buf[0] = op
	sizeLen := binary.PutUvarint(buf[1:], size)
	bufLen := 1 + sizeLen

	if _, err := d.writer.Write(buf[:bufLen]); err != nil {
		return err
	}

	if data != nil {
		if _, err := d.writer.Write(data); err != nil {
			return err
		}
	}

	return nil
}

func (d *deltaWriter) FlushBuffer() error {
	if len(d.buffer) == 0 {
		return nil
	}
	err := d.writeOp(common.DeltaOpData, uint64(len(d.buffer)), d.buffer)
	d.buffer = d.buffer[:0]
	return err
}

func (d *deltaWriter) Close() error {
	if d.writer == nil {
		return nil
	}
	err := d.writer.Close()
	d.writer = nil
	return err
}

func (d *deltaWriter) WriteContent(data []byte) error {
	d.buffer = append(d.buffer, data...)

	if len(d.buffer) >= deltaDataChunkSize {
		return d.FlushBuffer()
	} else {
		return nil
	}
}

// Switches to new file if needed and ensures we're at the start of it
func (d *deltaWriter) SetCurrentFile(filename string) error {
	if d.currentFile != filename {
		nameBytes := []byte(filename)
		err := d.FlushBuffer()
		if err != nil {
			return err
		}
		err = d.writeOp(common.DeltaOpOpen, uint64(len(nameBytes)), nameBytes)
		if err != nil {
			return err
		}

		d.currentFile = filename
		d.currentPos = 0
	}
	return nil
}

func (d *deltaWriter) Seek(pos uint64) error {
	if d.currentPos == pos {
		return nil
	}

	err := d.FlushBuffer()
	if err != nil {
		return err
	}

	err = d.writeOp(common.DeltaOpSeek, pos, nil)
	if err != nil {
		return err
	}
	d.currentPos = pos
	return nil
}

func (d *deltaWriter) SeekForward(pos uint64) error {
	d.currentPos += pos

	err := d.FlushBuffer()
	if err != nil {
		return err
	}

	err = d.writeOp(common.DeltaOpSeek, d.currentPos, nil)
	if err != nil {
		return err
	}
	return nil
}

func (d *deltaWriter) CopyFile(size uint64) error {
	err := d.FlushBuffer()
	if err != nil {
		return err
	}

	err = d.writeOp(common.DeltaOpCopy, size, nil)
	if err != nil {
		return err
	}
	d.currentPos += size
	return nil
}

func (d *deltaWriter) WriteAddContent(data []byte) error {
	err := d.FlushBuffer()
	if err != nil {
		return err
	}

	size := uint64(len(data))
	err = d.writeOp(common.DeltaOpAddData, size, data)
	if err != nil {
		return err
	}
	d.currentPos += size
	return nil
}

func (d *deltaWriter) CopyFileAt(offset uint64, size uint64) error {
	if err := d.Seek(offset); err != nil {
		return err
	}
	if err := d.CopyFile(size); err != nil {
		return err
	}
	return nil
}

func (d *deltaWriter) WriteOldFile(filename string, size uint64) error {
	err := d.SetCurrentFile(filename)
	if err != nil {
		return err
	}
	if err := d.Seek(0); err != nil {
		return err
	}
	err = d.CopyFile(size)
	if err != nil {
		return err
	}
	return nil
}

func (d *deltaWriter) Write(data []byte) (int, error) {
	n := len(data)
	err := d.WriteContent(data)
	return n, err
}
