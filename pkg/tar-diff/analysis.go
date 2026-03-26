// Package tardiff provides functionality for analyzing and creating binary differences between tar archives.
package tardiff

import (
	"archive/tar"
	"crypto/sha1"
	"encoding/hex"
	"io"
	"log"
	"os"
	"path"
	"strings"

	"github.com/containers/image/v5/pkg/compression"
	"github.com/containers/tar-diff/pkg/protocol"
)

type tarFileInfo struct {
	index int
	// Hard-linked files have multiple names/basenames
	basenames   []string
	paths       []string
	size        int64
	sha1        string
	blobs       []rollsumBlob
	overwritten bool
}

type hardlinkInfo struct {
	index    int
	path     string
	linkname string
	header   *tar.Header
}

type tarInfo struct {
	files     []tarFileInfo // no size=0 files
	hardlinks []hardlinkInfo
}

type targetInfo struct {
	file           *tarFileInfo
	hardlink       *hardlinkInfo
	source         *sourceInfo
	rollsumMatches *rollsumMatches
}

type sourceInfo struct {
	file               *tarFileInfo
	usedForDelta       bool
	offset             int64
	sourceTarFileIndex int
}

type deltaAnalysis struct {
	targetInfos       []targetInfo
	sourceInfos       []sourceInfo
	sourceData        *os.File
	targetInfoByIndex map[int]*targetInfo
}

func (a *deltaAnalysis) Close() error {
	err := a.sourceData.Close()
	if removeErr := os.Remove(a.sourceData.Name()); removeErr != nil {
		log.Printf("failed to remove: %v", removeErr)
		if err == nil {
			err = removeErr
		}
	}
	return err
}

func isSparseFile(hdr *tar.Header) bool {
	if hdr.Typeflag == tar.TypeGNUSparse {
		return true
	}
	if hdr.Typeflag == tar.TypeReg &&
		(hdr.PAXRecords["GNU.sparse.major"] != "" || hdr.PAXRecords["GNU.sparse.minor"] != "" || hdr.PAXRecords["GNU.sparse.map"] != "") {
		return true
	}

	return false
}

// Cleans up the path lexically
// Any ".." that extends outside the first elements (or the root itself) is invalid and returns ""

// Ignore all the files that make no sense to either delta or re-use as is
func useTarFile(hdr *tar.Header, cleanPath string) bool {
	// Don't use invalid paths (as returned by cleanPath)
	if cleanPath == "" {
		return false
	}

	if hdr.Typeflag != tar.TypeReg {
		return false
	}

	// We never create file info for empty files, since we can't delta with them
	if hdr.Size == 0 {
		return false
	}

	// Sparse headers will return file content that doesn't match the tarfile stream contents, so lets just
	// not delta them. We could do better here, but I don't think sparse files are very common.
	if isSparseFile(hdr) {
		return false
	}

	// We don't want to delta files that may be problematic to
	// read (e.g. /etc/shadow) when applying the delta. These are
	// uncommon anyway so no big deal.
	if (hdr.Mode & 00004) == 0 {
		return false
	}

	return true
}

func analyzeTar(tarMaybeCompressed io.Reader) (*tarInfo, error) {
	tarFile, _, err := compression.AutoDecompress(tarMaybeCompressed)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := tarFile.Close(); err != nil {
			log.Printf("close tar file: %v", err)
		}
	}()

	files := make([]tarFileInfo, 0)
	hardlinks := make([]hardlinkInfo, 0)
	infoByPath := make(map[string]int) // map from path to index in 'files'

	rdr := tar.NewReader(tarFile)
	for index := 0; true; index++ {
		var hdr *tar.Header
		hdr, err = rdr.Next()
		if err != nil {
			if err == io.EOF {
				break // Expected error
			}
			return nil, err
		}
		// Normalize name, for safety
		pathname := protocol.CleanPath(hdr.Name)

		// Handle hardlinks
		if hdr.Typeflag == tar.TypeLink {
			linkname := protocol.CleanPath(hdr.Linkname)
			if linkname != "" {
				// Store a copy of the header for later use
				hdrCopy := *hdr
				hardlinks = append(hardlinks, hardlinkInfo{
					index:    index,
					path:     pathname,
					linkname: linkname,
					header:   &hdrCopy,
				})
			}
			// Skip the content (hardlinks have no content)
			continue
		}

		// If a file is in the archive several times, mark it as overwritten so its not used for delta source
		if oldIndex, ok := infoByPath[pathname]; ok {
			files[oldIndex].overwritten = true
		}

		if !useTarFile(hdr, pathname) {
			continue
		}

		h := sha1.New()
		r := newRollsum()
		w := io.MultiWriter(h, r)
		if _, err := io.Copy(w, rdr); err != nil {
			return nil, err
		}

		fileInfo := tarFileInfo{
			index:     index,
			basenames: []string{path.Base(pathname)},
			paths:     []string{pathname},
			size:      hdr.Size,
			sha1:      hex.EncodeToString(h.Sum(nil)),
			blobs:     r.GetBlobs(),
		}
		infoByPath[pathname] = len(files)
		files = append(files, fileInfo)
	}

	// Add hardlink paths and basenames to their target files
	for i := range hardlinks {
		hl := &hardlinks[i]
		if fileIndex, ok := infoByPath[hl.linkname]; ok {
			files[fileIndex].paths = append(files[fileIndex].paths, hl.path)
			files[fileIndex].basenames = append(files[fileIndex].basenames, path.Base(hl.path))
		}
	}

	info := tarInfo{files: files, hardlinks: hardlinks}
	return &info, nil
}

// This is not called for files that can be used as-is, only for files that would
// be diffed with bsdiff or rollsums
func isDeltaCandidate(file *tarFileInfo) bool {
	// Look for known non-delta-able files (currently just compression)
	// NB: We explicitly don't have .gz here in case someone might be
	// using --rsyncable for that.
	for _, basename := range file.basenames {
		if strings.HasSuffix(basename, ".xz") ||
			strings.HasSuffix(basename, ".bz2") {
			return false
		}
	}

	return true
}

func nameIsSimilar(a *tarFileInfo, b *tarFileInfo, fuzzy int) bool {
	for _, aBasename := range a.basenames {
		for _, bBasename := range b.basenames {
			if fuzzy == 0 {
				if aBasename == bBasename {
					return true
				}
			} else {
				aa := strings.SplitAfterN(aBasename, ".", 2)[0]
				bb := strings.SplitAfterN(bBasename, ".", 2)[0]
				if aa == bb {
					return true
				}
			}
		}
	}
	return false
}

// Check that two files are not wildly dissimilar in size.
// This is to catch complete different version of the file, for example
// replacing a binary with a shell wrapper
func sizeIsSimilar(a *tarFileInfo, b *tarFileInfo) bool {
	// For small files, we always think they are similar size
	// There is no use considering a 5 byte and a 50 byte file
	// wildly different
	if a.size < 64*1024 && b.size < 64*1024 {
		return true
	}
	// For larger files, we check that one is not a factor of 10 larger than the other
	return a.size < 10*b.size && b.size < 10*a.size
}

type indexKey struct {
	fileIndex  int
	entryIndex int
}

func extractDeltaData(tarMaybeCompressedFiles []io.ReadSeeker, sourceByIndex map[indexKey]*sourceInfo, dest *os.File) error {
	offset := int64(0)

	for fileIndex, tarMaybeCompressed := range tarMaybeCompressedFiles {
		tarFile, _, err := compression.AutoDecompress(tarMaybeCompressed)
		if err != nil {
			return err
		}
		defer func() {
			if err := tarFile.Close(); err != nil {
				log.Printf("close tar file: %v", err)
			}
		}()

		rdr := tar.NewReader(tarFile)
		for index := 0; true; index++ {
			var hdr *tar.Header
			hdr, err = rdr.Next()
			if err != nil {
				if err == io.EOF {
					break // Expected error
				}
				return err
			}
			info := sourceByIndex[indexKey{fileIndex: fileIndex, entryIndex: index}]
			if info != nil && info.usedForDelta {
				info.offset = offset
				offset += hdr.Size
				if _, err := io.Copy(dest, rdr); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func abs(n int64) int64 {
	if n < 0 {
		return -n
	}
	return n
}

func buildSourceInfos(oldInfos []*tarInfo) []sourceInfo {
	sourceInfos := make([]sourceInfo, 0)
	pathToFileIndex := make(map[string]int)

	for fileIdx, oldInfo := range oldInfos {
		for i := range oldInfo.files {
			file := &oldInfo.files[i]

			// Check if any path from this file conflicts with existing files
			for _, p := range file.paths {
				if existingIdx, exists := pathToFileIndex[p]; exists {
					sourceInfos[existingIdx].file.overwritten = true
				}
			}

			// Add the primary path of this file (which is the one used as delta source)
			currentFileIndex := len(sourceInfos)
			pathToFileIndex[file.paths[0]] = currentFileIndex

			sourceInfos = append(sourceInfos, sourceInfo{
				file:               file,
				sourceTarFileIndex: fileIdx,
			})
		}
	}

	return sourceInfos
}

func matchesAnyPrefix(path string, prefixes []string) bool {
	if len(prefixes) == 0 {
		return true
	}
	for _, prefix := range prefixes {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}
	return false
}

func isDeltaSourceCandidate(s *sourceInfo, options *Options) bool {
	if s.file.overwritten {
		return false
	}
	primaryPath := s.file.paths[0]
	return matchesAnyPrefix(primaryPath, options.sourcePrefixes)
}

func findFuzzyDeltaSource(sourceInfos []sourceInfo, targetFile *tarFileInfo, options *Options) *sourceInfo {
	// Check for moved (first) or renamed (second) versions
	for fuzzy := 0; fuzzy < 2; fuzzy++ {
		var source *sourceInfo
		for j := range sourceInfos {
			s := &sourceInfos[j]

			// Skip files that we're not allowed to use
			if !isDeltaSourceCandidate(s, options) {
				continue
			}
			// Skip files that make no sense to delta (like compressed files)
			if !isDeltaCandidate(s.file) {
				continue
			}
			// We're looking for moved files, or renames to "similar names"
			if !nameIsSimilar(targetFile, s.file, fuzzy) {
				continue
			}
			// Skip files that are wildly dissimilar in size, such as binaries replaces by shellscripts
			if !sizeIsSimilar(targetFile, s.file) {
				continue
			}
			// Choose the matching source that have most similar size to the new file
			if source != nil && abs(source.file.size-targetFile.size) < abs(s.file.size-targetFile.size) {
				continue
			}

			source = s
		}
		if source != nil {
			return source
		}
	}
	return nil
}

func analyzeForDelta(oldInfos []*tarInfo, newTar *tarInfo, oldFiles []io.ReadSeeker, options *Options) (*deltaAnalysis, error) {
	if options == nil {
		options = NewOptions()
	}

	sourceInfos := buildSourceInfos(oldInfos)

	sourceBySha1 := make(map[string]*sourceInfo)
	sourceByPath := make(map[string]*sourceInfo)
	sourceByIndex := make(map[indexKey]*sourceInfo)
	for i := range sourceInfos {
		s := &sourceInfos[i]
		if !isDeltaSourceCandidate(s, options) {
			continue
		}
		sourceBySha1[s.file.sha1] = s
		for _, p := range s.file.paths {
			sourceByPath[p] = s
		}
		sourceByIndex[indexKey{fileIndex: s.sourceTarFileIndex, entryIndex: s.file.index}] = s
	}

	targetInfos := make([]targetInfo, 0, len(newTar.files)+len(newTar.hardlinks))

	for i := range newTar.files {
		file := &newTar.files[i]
		// First look for exact content match
		usedForDelta := false
		var source *sourceInfo
		sha1Source := sourceBySha1[file.sha1]
		// If same sha1 and size, use original total size
		if sha1Source != nil && file.size == sha1Source.file.size {
			source = sha1Source
		}
		if source == nil && isDeltaCandidate(file) {
			// No exact match, try to find a useful source

			// Check if any of the target file's paths match a source file
			var s *sourceInfo
			for _, p := range file.paths {
				if matchedSource := sourceByPath[p]; matchedSource != nil {
					s = matchedSource
					break
				}
			}

			if s != nil && isDeltaCandidate(s.file) && sizeIsSimilar(file, s.file) {
				usedForDelta = true
				source = s
			} else {
				source = findFuzzyDeltaSource(sourceInfos, file, options)
				if source != nil {
					usedForDelta = true
				}
			}
		}

		var rollsumMatches *rollsumMatches
		if source != nil {
			source.usedForDelta = source.usedForDelta || usedForDelta

			if usedForDelta {
				rollsumMatches = computeRollsumMatches(source.file.blobs, file.blobs)
			}
		}
		info := targetInfo{file: file, source: source, rollsumMatches: rollsumMatches}
		targetInfos = append(targetInfos, info)
	}

	targetInfoByIndex := make(map[int]*targetInfo, len(newTar.files)+len(newTar.hardlinks))
	for i := range targetInfos {
		t := &targetInfos[i]
		targetInfoByIndex[t.file.index] = t
	}
	// Add hardlinks to targetInfoByIndex
	for i := range newTar.hardlinks {
		hl := &newTar.hardlinks[i]
		info := targetInfo{hardlink: hl}
		targetInfos = append(targetInfos, info)
		targetInfoByIndex[hl.index] = &targetInfos[len(targetInfos)-1]
	}

	tmpfile, err := os.CreateTemp(os.TempDir(), "tar-diff-")
	if err != nil {
		return nil, err
	}

	err = extractDeltaData(oldFiles, sourceByIndex, tmpfile)
	if err != nil {
		_ = os.Remove(tmpfile.Name())
		return nil, err
	}

	return &deltaAnalysis{targetInfos: targetInfos, targetInfoByIndex: targetInfoByIndex, sourceInfos: sourceInfos, sourceData: tmpfile}, nil
}
