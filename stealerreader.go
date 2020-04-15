package tar_diff

import (
	"io"
)

type stealerReader struct {
	source  io.Reader
	stealer io.Writer
	ignore  bool
}

// This is a wrapper for reader, everything that is read from
// it is also written to the stealer, unless this is temporary
// disabled by SetIgnore(true)

func newStealerReader(source io.Reader, stealer io.Writer) *stealerReader {
	return &stealerReader{source: source, stealer: stealer}
}

func (s *stealerReader) Read(p []byte) (int, error) {
	n, err := s.source.Read(p)
	var writeErr error = nil
	if !s.ignore && n > 0 {
		_, writeErr = s.stealer.Write(p[0:n])
	}

	if err != nil {
		return n, err
	}
	if writeErr != nil {
		return n, writeErr
	}
	return n, nil
}

func (s *stealerReader) SetIgnore(ignore bool) {
	s.ignore = ignore
}
