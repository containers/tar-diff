package tardiff

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"log"

	"github.com/containers/image/v5/pkg/compression"
)

const (
	defaultMaxBsdiffSize = 192 * 1024 * 1024
)

type deltaGenerator struct {
	stealingTarFile *stealerReader
	tarReader       *tar.Reader
	analysis        *deltaAnalysis
	deltaWriter     *deltaWriter
	options         *Options
}

// Toggle whether reads from the source tarfile are copied into the delta, or skipped
func (g *deltaGenerator) setSkip(ignore bool) {
	g.stealingTarFile.SetIgnore(ignore)
}

// Skip the rest of the current file from the tarfile
func (g *deltaGenerator) skipRest() error {
	g.setSkip(true)
	_, err := io.Copy(io.Discard, g.tarReader)
	return err
}

// Read the next n bytes of data from the current file in the tarfile, not copying it to delta
func (g *deltaGenerator) readN(n int64) ([]byte, error) {
	g.setSkip(true)
	buf := make([]byte, n)
	_, err := io.ReadFull(g.tarReader, buf)
	return buf, err
}

// Copy the rest of the current file from the tarfile into the delta
func (g *deltaGenerator) copyRest() error {
	g.setSkip(false)
	_, err := io.Copy(io.Discard, g.tarReader)
	return err
}

// Copy the next n bytes of the current file from the tarfile into the delta
func (g *deltaGenerator) copyN(n int64) error {
	g.setSkip(false)
	_, err := io.CopyN(io.Discard, g.tarReader, int64(n))
	return err
}

// Read back part of the stored data for the source file
func (g *deltaGenerator) readSourceData(source *sourceInfo, offset int64, size int64) ([]byte, error) {
	sdi := g.analysis.sourceDataInfos[source]
	_, err := g.analysis.sourceData.Seek(int64(sdi.offset+offset), 0)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, size)
	_, err = io.ReadFull(g.analysis.sourceData, buf)
	return buf, err
}

func (g *deltaGenerator) generateForFileWithBsdiff(info *targetInfo) error {
	file := info.file
	source := info.source

	err := g.deltaWriter.SetCurrentFile(info.source.sourcePath)
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

func (g *deltaGenerator) generateForFileWithrollsums(info *targetInfo) error {
	file := info.file
	source := info.source
	matches := info.rollsumMatches.matches
	pos := int64(0)

	err := g.deltaWriter.SetCurrentFile(info.source.sourcePath)
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

func (g *deltaGenerator) generateForFile(info *targetInfo) error {
	file := info.file
	sourceFile := info.source.file

	maxBsdiffSize := g.options.maxBsdiffSize

	switch {
	case sourceFile.sha1 == file.sha1 && sourceFile.size == file.size:
		// Reuse exact file from old tar
		if err := g.deltaWriter.WriteOldFile(info.source.sourcePath, uint64(sourceFile.size)); err != nil {
			return err
		}

		if err := g.skipRest(); err != nil {
			return err
		}
	case maxBsdiffSize == 0 || (file.size < maxBsdiffSize && sourceFile.size < maxBsdiffSize):
		// Use bsdiff to generate delta
		if err := g.generateForFileWithBsdiff(info); err != nil {
			return err
		}
	case info.rollsumMatches != nil && info.rollsumMatches.matchRatio > 20:
		// Use rollsums to generate delta
		if err := g.generateForFileWithrollsums(info); err != nil {
			return err
		}
	default:
		if err := g.copyRest(); err != nil {
			return err
		}
	}
	return nil
}

func generateDelta(newFile io.ReadSeeker, deltaFile io.Writer, analysis *deltaAnalysis, options *Options) error {
	tarFile, _, err := compression.AutoDecompress(newFile)
	if err != nil {
		return err
	}
	defer func() {
		if err := tarFile.Close(); err != nil {
			log.Printf("close tar file: %v", err)
		}
	}()

	deltaWriter, err := newDeltaWriter(deltaFile, options.compressionLevel)
	if err != nil {
		return err
	}
	defer func() {
		if err := deltaWriter.Close(); err != nil {
			log.Printf("close tar file: %v", err)
		}
	}()

	stealingTarFile := newStealerReader(tarFile, deltaWriter)
	tarReader := tar.NewReader(stealingTarFile)

	g := &deltaGenerator{
		stealingTarFile: stealingTarFile,
		tarReader:       tarReader,
		analysis:        analysis,
		deltaWriter:     deltaWriter,
		options:         options,
	}

	for index := 0; true; index++ {
		g.setSkip(false)
		_, err := g.tarReader.Next()
		if err != nil {
			if err == io.EOF {
				// Expected error
				break
			}
			return err
		}

		info := g.analysis.targetInfoByIndex[index]
		if info != nil && info.hardlink != nil {
			// Handle hardlink - header was already copied by stealerReader
			// Hardlinks have no content, so we're done
			continue
		}
		if info != nil && info.source != nil {
			if err := g.generateForFile(info); err != nil {
				return err
			}
		}
	}
	// Steal any remaining data left by tar reader
	if _, err := io.Copy(io.Discard, stealingTarFile); err != nil {
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

// Options configures the behavior of the diff operation.
type Options struct {
	compressionLevel     int
	maxBsdiffSize        int64
	sourcePrefixes       []string
	ignoreSourcePrefixes []string
	tmpDir               string
	applyWhiteouts       bool
}

// SetCompressionLevel sets the compression level for the output diff file.
func (o *Options) SetCompressionLevel(compressionLevel int) {
	o.compressionLevel = compressionLevel
}

// SetMaxBsdiffFileSize sets the maximum file size for bsdiff operations.
func (o *Options) SetMaxBsdiffFileSize(maxBsdiffSize int64) {
	o.maxBsdiffSize = maxBsdiffSize
}

// SetSourcePrefixes sets path prefixes to filter which source files can be used for delta.
// Only files whose primary path starts with one of these prefixes will be used as delta sources.
func (o *Options) SetSourcePrefixes(prefixes []string) {
	o.sourcePrefixes = prefixes
}

// SetTmpDir sets the directory for temporary files. Defaults to os.TempDir().
func (o *Options) SetTmpDir(dir string) {
	o.tmpDir = dir
}

// SetIgnoreSourcePrefixes sets path prefixes to exclude from delta sources.
// Files whose paths all match one of these prefixes will not be used as delta sources.
// If a file has multiple names (hardlinks), any non-ignored name makes the file usable.
func (o *Options) SetIgnoreSourcePrefixes(prefixes []string) {
	o.ignoreSourcePrefixes = prefixes
}

// SetApplyWhiteouts enables docker/OCI-style whiteout processing when analyzing
// old tar layers. Whiteout files (.wh.<name> and .wh..wh..opq) in upper layers
// remove matching paths from lower layers, so the delta sources reflect the
// merged container image rather than individual layers.
func (o *Options) SetApplyWhiteouts(apply bool) {
	o.applyWhiteouts = apply
}

// NewOptions creates a new Options struct with default values.
func NewOptions() *Options {
	return &Options{
		compressionLevel:     3,
		maxBsdiffSize:        defaultMaxBsdiffSize,
		sourcePrefixes:       nil,
		ignoreSourcePrefixes: nil,
	}
}

// SourceAnalysis contains information about pre-analyzed delta sources.
// It can be reused across multiple DiffWithSources calls, including
// concurrent calls from different goroutines, as long as each call
// provides its own independent set of old tar readers.
type SourceAnalysis struct {
	sourceInfos   []sourceInfo
	sourceBySha1  map[string]*sourceInfo
	sourceByPath  map[string]*sourceInfo
	sourceByIndex map[indexKey]*sourceInfo
	numOldFiles   int
}

// AnalyzeSources pre-computes analysis of one or more old tar files
// that can be reused across multiple DiffWithSources operations.
// oldTarFiles contains one or more old tar files, in extraction order.
// The readers are only used during this call and are not retained.
func AnalyzeSources(oldTarFiles []io.ReadSeeker, options *Options) (*SourceAnalysis, error) {
	if options == nil {
		options = NewOptions()
	}

	if len(oldTarFiles) == 0 {
		return nil, fmt.Errorf("at least one old tar file is required")
	}

	oldInfos := make([]*tarInfo, len(oldTarFiles))
	for i, oldTarFile := range oldTarFiles {
		oldInfo, err := analyzeTar(oldTarFile, options.applyWhiteouts)
		if err != nil {
			return nil, err
		}
		oldInfos[i] = oldInfo
	}

	return buildSourceAnalysis(oldInfos, len(oldTarFiles), options), nil
}

// DiffWithSources creates a binary difference using a pre-computed
// SourceAnalysis. The oldTarFiles must correspond to the same tar
// files (in the same order) that were passed to AnalyzeSources. If
// they are independent readers then that allows concurrent calls from
// multiple goroutines.
func DiffWithSources(sources *SourceAnalysis, oldTarFiles []io.ReadSeeker, newTarFile io.ReadSeeker, diffFile io.Writer, options *Options) error {
	if options == nil {
		options = NewOptions()
	}

	if len(oldTarFiles) != sources.numOldFiles {
		return fmt.Errorf("expected %d old tar files, got %d", sources.numOldFiles, len(oldTarFiles))
	}

	newInfo, err := analyzeTar(newTarFile, false)
	if err != nil {
		return err
	}

	if _, err := newTarFile.Seek(0, 0); err != nil {
		return err
	}

	analysis, err := analyzeForDelta(sources, newInfo, oldTarFiles, options)
	if err != nil {
		return err
	}

	defer func() {
		if err := analysis.Close(); err != nil {
			log.Printf("close analysis: %v", err)
		}
	}()

	return generateDelta(newTarFile, diffFile, analysis, options)
}

// Diff creates a binary difference between a set of tar archives and a new tar archive.
// oldTarFiles contains one or more old tar files, in extraction order.
func Diff(oldTarFiles []io.ReadSeeker, newTarFile io.ReadSeeker, diffFile io.Writer, options *Options) error {
	sources, err := AnalyzeSources(oldTarFiles, options)
	if err != nil {
		return err
	}

	// Reset old files after AnalyzeSources read them
	for _, oldTarFile := range oldTarFiles {
		if _, err := oldTarFile.Seek(0, 0); err != nil {
			return err
		}
	}

	return DiffWithSources(sources, oldTarFiles, newTarFile, diffFile, options)
}
