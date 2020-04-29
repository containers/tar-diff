package tar_diff

import (
	"archive/tar"
	"crypto/sha1"
	"encoding/hex"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"

	"github.com/containers/image/v5/pkg/compression"
)

type tarFileInfo struct {
	index       int
	basename    string
	path        string
	size        int64
	sha1        string
	blobs       []rollsumBlob
	overwritten bool
}

type tarInfo struct {
	files []tarFileInfo // no size=0 files
}

type targetInfo struct {
	file           *tarFileInfo
	source         *sourceInfo
	rollsumMatches *rollsumMatches
}

type sourceInfo struct {
	file         *tarFileInfo
	usedForDelta bool
	offset       int64
}

type deltaAnalysis struct {
	targetInfos       []targetInfo
	sourceInfos       []sourceInfo
	sourceData        *os.File
	targetInfoByIndex map[int]*targetInfo
}

func (a *deltaAnalysis) Close() {
	a.sourceData.Close()
	os.Remove(a.sourceData.Name())
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
func cleanPath(pathName string) string {
	// We make the path always absolute, that way path.Clean() ensure it never goes outside the top ("root") dir
	// even if its a relative path
	clean := path.Clean("/" + pathName)

	// We clean the initial slash, making all result relative (or "" which is error)
	return clean[1:]
}

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
	defer tarFile.Close()

	files := make([]tarFileInfo, 0)
	infoByPath := make(map[string]int) // map from path to index in 'files'

	rdr := tar.NewReader(tarFile)
	for index := 0; true; index++ {
		var hdr *tar.Header
		hdr, err = rdr.Next()
		if err != nil {
			if err == io.EOF {
				break // Expected error
			} else {
				return nil, err
			}
		}
		// Normalize name, for safety
		pathname := cleanPath(hdr.Name)

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
			index:    index,
			basename: path.Base(pathname),
			path:     pathname,
			size:     hdr.Size,
			sha1:     hex.EncodeToString(h.Sum(nil)),
			blobs:    r.GetBlobs(),
		}
		infoByPath[pathname] = len(files)
		files = append(files, fileInfo)
	}

	info := tarInfo{files: files}
	return &info, nil
}

// This is not called for files that can be used as-is, only for files that would
// be diffed with bsdiff or rollsums
func isDeltaCandidate(file *tarFileInfo) bool {
	// Look for known non-delta-able files (currently just compression)
	// NB: We explicitly don't have .gz here in case someone might be
	// using --rsyncable for that.
	if strings.HasPrefix(file.basename, ".xz") ||
		strings.HasPrefix(file.basename, ".bz2") {
		return false
	}

	return true
}

func nameIsSimilar(a *tarFileInfo, b *tarFileInfo, fuzzy int) bool {
	if fuzzy == 0 {
		return a.basename == b.basename
	} else {
		aa := strings.SplitAfterN(a.basename, ".", 2)[0]
		bb := strings.SplitAfterN(b.basename, ".", 2)[0]
		return aa == bb
	}
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

func extractDeltaData(tarMaybeCompressed io.Reader, sourceByIndex map[int]*sourceInfo, dest *os.File) error {
	offset := int64(0)

	tarFile, _, err := compression.AutoDecompress(tarMaybeCompressed)
	if err != nil {
		return err
	}
	defer tarFile.Close()

	rdr := tar.NewReader(tarFile)
	for index := 0; true; index++ {
		var hdr *tar.Header
		hdr, err = rdr.Next()
		if err != nil {
			if err == io.EOF {
				break // Expected error
			} else {
				return err
			}
		}
		info := sourceByIndex[index]
		if info != nil && info.usedForDelta {
			info.offset = offset
			offset += hdr.Size
			if _, err := io.Copy(dest, rdr); err != nil {
				return err
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
func analyzeForDelta(old *tarInfo, new *tarInfo, oldFile io.Reader) (*deltaAnalysis, error) {
	sourceInfos := make([]sourceInfo, 0, len(old.files))
	for i := range old.files {
		sourceInfos = append(sourceInfos, sourceInfo{file: &old.files[i]})
	}

	sourceBySha1 := make(map[string]*sourceInfo)
	sourceByPath := make(map[string]*sourceInfo)
	sourceByIndex := make(map[int]*sourceInfo)
	for i := range sourceInfos {
		s := &sourceInfos[i]
		if !s.file.overwritten {
			sourceBySha1[s.file.sha1] = s
			sourceByPath[s.file.path] = s
			sourceByIndex[s.file.index] = s
		}
	}

	targetInfos := make([]targetInfo, 0, len(new.files))

	for i := range new.files {
		file := &new.files[i]
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

			s := sourceByPath[file.path]

			if s != nil && isDeltaCandidate(s.file) && sizeIsSimilar(file, s.file) {
				usedForDelta = true
				source = s
			} else {
				// Check for moved (first) or renamed (second) versions
				for fuzzy := 0; fuzzy < 2 && source == nil; fuzzy++ {
					for j := range sourceInfos {
						s = &sourceInfos[j]

						// Skip files that make no sense to delta (like compressed files)
						if !isDeltaCandidate(s.file) {
							continue
						}
						// We're looking for moved files, or renames to "similar names"
						if !nameIsSimilar(file, s.file, fuzzy) {
							continue
						}
						// Skip files that are wildly dissimilar in size, such as binaries replaces by shellscripts
						if !sizeIsSimilar(file, s.file) {
							continue
						}
						// Choose the matching source that have most similar size to the new file
						if source != nil && abs(source.file.size-file.size) < abs(s.file.size-file.size) {
							continue
						}

						usedForDelta = true
						source = s
					}
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

	targetInfoByIndex := make(map[int]*targetInfo)
	for i := range targetInfos {
		t := &targetInfos[i]
		targetInfoByIndex[t.file.index] = t
	}

	tmpfile, err := ioutil.TempFile("/var/tmp", "tar-diff-")
	if err != nil {
		return nil, err
	}

	err = extractDeltaData(oldFile, sourceByIndex, tmpfile)
	if err != nil {
		return nil, err
	}

	return &deltaAnalysis{targetInfos: targetInfos, targetInfoByIndex: targetInfoByIndex, sourceInfos: sourceInfos, sourceData: tmpfile}, nil
}
