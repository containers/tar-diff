package tar_diff

// TODO
// * Handle same file multiple times in tarfile
// * Handle sparse files
// * Handle empty files
// * Handle hardlinks

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"io"
	"io/ioutil"
)

const (
	maxBsdiffSize = 64 * 1024 * 1024
)

type DeltaGenerator struct {
	stealingTarFile *StealerReader
	tarReader       *tar.Reader
	analysis        *DeltaAnalysis
	deltaWriter     *DeltaWriter
}

// Toggle whether reads from the source tarfile are copied into the delta, or skipped
func (g *DeltaGenerator) setSkip(ignore bool) {
	g.stealingTarFile.SetIgnore(ignore)
}

// Skip the rest of the current file from the tarfile
func (g *DeltaGenerator) skipRest() error {
	g.setSkip(true)
	_, err := io.Copy(ioutil.Discard, g.tarReader)
	return err
}

// Skip the next n bytes of data from the current file in the tarfile
func (g *DeltaGenerator) skipN(n int64) error {
	g.setSkip(true)
	_, err := io.CopyN(ioutil.Discard, g.tarReader, int64(n))
	return err
}

// Read the next n bytes of data from the current file in the tarfile, not copying it to delta
func (g *DeltaGenerator) readN(n int64) ([]byte, error) {
	g.setSkip(true)
	buf := make([]byte, n)
	_, err := io.ReadFull(g.tarReader, buf)
	return buf, err
}

// Copy the rest of the current file from the tarfile into the delta
func (g *DeltaGenerator) copyRest() error {
	g.setSkip(false)
	_, err := io.Copy(ioutil.Discard, g.tarReader)
	return err
}

// Copy the next n bytes of the current file from the tarfile into the delta
func (g *DeltaGenerator) copyN(n int64) error {
	g.setSkip(false)
	_, err := io.CopyN(ioutil.Discard, g.tarReader, int64(n))
	return err
}

// Read back part of the stored data for the source file
func (g *DeltaGenerator) readSourceData(source *SourceInfo, offset int64, size int64) ([]byte, error) {
	g.analysis.sourceData.Seek(int64(source.offset+offset), 0)
	buf := make([]byte, size)
	_, err := io.ReadFull(g.analysis.sourceData, buf)
	return buf, err
}

func (g *DeltaGenerator) generateForFileWithBsdiff(info *TargetInfo) error {
	file := info.file
	source := info.source

	err := g.deltaWriter.SetCurrentFile(source.file.path)
	if err != nil {
		return err
	}

	err = g.deltaWriter.Seek(0)
	if err != nil {
		return err
	}

	oldData, err := g.readSourceData(source, 0, source.file.size)
	if err != nil {
		return err
	}

	newData, err := g.readN(file.size)
	if err != nil {
		return err
	}

	err = bsdiff(oldData, newData, g.deltaWriter)
	if err != nil {
		return err
	}

	return nil
}

func (g *DeltaGenerator) generateForFileWithRollsums(info *TargetInfo) error {
	file := info.file
	source := info.source
	matches := info.rollsumMatches.matches
	pos := int64(0)

	err := g.deltaWriter.SetCurrentFile(source.file.path)
	if err != nil {
		return err
	}

	for i := range matches {
		match := &matches[i]
		matchStart := match.to.offset
		matchSize := match.to.size

		// Copy upto next match
		if pos < matchStart {
			if err := g.copyN(matchStart - pos); err != nil {
				return err
			}
		}
		// Before copying from old file, we have to verify we got an exact match
		dstbuf, err := g.readN(matchSize)
		if err != nil {
			return err
		}
		srcbuf, err := g.readSourceData(source, match.from.offset, matchSize)
		if err != nil {
			return err
		}
		if bytes.Equal(dstbuf, srcbuf) {
			// The chunks were actually equal, crc32 never lies!
			if err := g.deltaWriter.CopyFileAt(uint64(match.from.offset), uint64(match.from.size)); err != nil {
				return err
			}
		} else {
			// Bufs where not the same, crc32 is a LIER!
			if err := g.deltaWriter.WriteContent(dstbuf); err != nil {
				return err
			}
		}
		pos = matchStart + matchSize
	}
	// Copy any remainder after last match
	if pos < file.size {
		if err := g.copyN(file.size - pos); err != nil {
			return err
		}
	}
	return nil
}

func (g *DeltaGenerator) generateForFile(info *TargetInfo) error {
	file := info.file
	sourceFile := info.source.file

	if sourceFile.sha1 == file.sha1 && sourceFile.size == file.size {
		// Reuse exact file from old tar
		if err := g.deltaWriter.WriteOldFile(sourceFile.path, uint64(sourceFile.size)); err != nil {
			return err
		}

		if err := g.skipRest(); err != nil {
			return err
		}
	} else if file.isExecutable && sourceFile.isExecutable && file.size < maxBsdiffSize && sourceFile.size < maxBsdiffSize {
		// Use bsdiff to generate delta
		if err := g.generateForFileWithBsdiff(info); err != nil {
			return err
		}
	} else if info.rollsumMatches != nil && info.rollsumMatches.matchRatio > 20 {
		// Use rollsums to generate delta
		if err := g.generateForFileWithRollsums(info); err != nil {
			return err
		}
	} else {
		if err := g.copyRest(); err != nil {
			return err
		}
	}
	return nil
}

func generateDelta(newFile io.ReadSeeker, deltaFile io.Writer, analysis *DeltaAnalysis) error {
	tarFile, err := gzip.NewReader(newFile)
	if err != nil {
		return err
	}
	defer tarFile.Close()

	deltaWriter, err := NewDeltaWriter(deltaFile)
	if err != nil {
		return err
	}
	defer deltaWriter.Close()

	stealingTarFile := NewStealerReader(tarFile, deltaWriter)
	tarReader := tar.NewReader(stealingTarFile)

	g := &DeltaGenerator{
		stealingTarFile: stealingTarFile,
		tarReader:       tarReader,
		analysis:        analysis,
		deltaWriter:     deltaWriter,
	}

	for index := 0; true; index++ {
		g.setSkip(false)
		_, err := g.tarReader.Next()
		if err != nil {
			if err == io.EOF {
				// Expected error
				break
			} else {
				return err
			}
		}

		info := g.analysis.targetInfoByIndex[index]
		if info != nil && info.source != nil {
			if err := g.generateForFile(info); err != nil {
				return err
			}
		}
	}
	// Steal any remaining data left by tar reader
	if _, err := io.Copy(ioutil.Discard, stealingTarFile); err != nil {
		return err
	}
	// Flush any outstanding stolen data
	err = deltaWriter.FlushBuffer()
	if err != nil {
		return err
	}
	err = deltaWriter.Close()
	if err != nil {
		return err
	}

	return nil
}

func GenerateDelta(oldFile io.ReadSeeker, newFile io.ReadSeeker, deltaFile io.Writer) error {
	// First analyze both tarfiles by themselves
	oldInfo, err := analyzeTar(oldFile)
	if err != nil {
		return err
	}

	newInfo, err := analyzeTar(newFile)
	if err != nil {
		return err
	}

	// Reset tar.gz for re-reading
	oldFile.Seek(0, 0)
	newFile.Seek(0, 0)

	// Compare new and old for delta information
	analysis, err := analyzeForDelta(oldInfo, newInfo, oldFile)
	if err != nil {
		return nil
	}
	defer analysis.Close()

	// Actually create the delta
	if err := generateDelta(newFile, deltaFile, analysis); err != nil {
		return err
	}

	return nil
}
