package tar_patch

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/alexlarsson/tar-diff/pkg/common"
	"github.com/klauspost/compress/zstd"
	"io"
	"os"
)

func maybeClose(closer io.Closer) {
	closer.Close()
}

func ApplyDelta(delta io.Reader, extractedDir string, dst io.Writer) error {
	buf := make([]byte, len(common.DeltaHeader))
	_, err := io.ReadFull(delta, buf)
	if err != nil {
		return err
	}
	if !bytes.Equal(buf, common.DeltaHeader[:]) {
		return fmt.Errorf("Invalid delta format")
	}

	decoder, err := zstd.NewReader(delta)
	if err != nil {
		return err
	}
	defer decoder.Close()

	r := bufio.NewReader(decoder)

	var currentFile *os.File
	defer maybeClose(currentFile)

	for {
		op, err := r.ReadByte()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		size, err := binary.ReadUvarint(r)
		if err != nil {
			return err
		}

		switch op {
		case common.DeltaOpData:
			_, err = io.CopyN(dst, r, int64(size))
			if err != nil {
				return err
			}
		case common.DeltaOpOpen:
			nameBytes := make([]byte, size)
			_, err = io.ReadFull(r, nameBytes)
			if err != nil {
				return err
			}
			name := string(nameBytes)
			path := extractedDir + "/" + name
			if currentFile != nil {
				currentFile.Close()
			}
			currentFile, err = os.Open(path)
			if err != nil {
				return err
			}
		case common.DeltaOpCopy:
			if currentFile == nil {
				return fmt.Errorf("No current file to copy from")
			}

			_, err = io.CopyN(dst, currentFile, int64(size))
			if err != nil {
				return err
			}
		case common.DeltaOpAddData:
			if currentFile == nil {
				return fmt.Errorf("No current file to copy from")
			}

			addBytes := make([]byte, size)
			_, err = io.ReadFull(r, addBytes)
			if err != nil {
				return err
			}

			addBytes2 := make([]byte, size)
			_, err = io.ReadFull(currentFile, addBytes2)
			if err != nil {
				return err
			}

			for i := uint64(0); i < size; i++ {
				addBytes[i] = addBytes[i] + addBytes2[i]
			}
			if _, err := dst.Write(addBytes); err != nil {
				return err
			}

		case common.DeltaOpSeek:
			if currentFile == nil {
				return fmt.Errorf("No current file to seek in")
			}
			_, err = currentFile.Seek(int64(size), 0)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("Unexpected delta op %d", op)
		}
	}

	return nil
}
