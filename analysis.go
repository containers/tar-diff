package tar_diff

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha1"
	"encoding/hex"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strings"
)

const (
	similarityPercentThreshold = 30
)

type TarFileInfo struct {
	index       int
	basename    string
	path        string
	size        int64
	sha1        string
	blobs       []RollsumBlob
	overwritten bool
}

type TarInfo struct {
	files []TarFileInfo // Sorted by size, no size=0 files
}

type TargetInfo struct {
	file           *TarFileInfo
	source         *SourceInfo
	rollsumMatches *RollsumMatches
}

type SourceInfo struct {
	file         *TarFileInfo
	usedForDelta bool
	offset       int64
}

type DeltaAnalysis struct {
	targetInfos       []TargetInfo
	sourceInfos       []SourceInfo
	sourceData        *os.File
	targetInfoByIndex map[int]*TargetInfo
}

func (a *DeltaAnalysis) Close() {
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

// Makes absolute paths relative, removes "/./" and "//" elements and resolves "/../" element inside the path
// Any ".." that extends outside the first elements is invalid and returns ""
func cleanPath(path string) string {
	elements := strings.Split(path, "/")
	res := make([]string, 0, len(elements))

	for i := range elements {
		element := elements[i]
		if element == "" {
			continue // Skip "//" style elements, or first /
		} else if element == "." {
			continue // Skip "/./" style elements
		} else if element == ".." {
			if len(res) == 0 {
				return "" // .. goes outside root, invalid
			}
			res = res[:len(res)-1]
		} else {
			res = append(res, element)
		}
	}
	return strings.Join(res, "/")
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

func analyzeTar(targzFile io.Reader) (*TarInfo, error) {
	tarFile, err := gzip.NewReader(targzFile)
	if err != nil {
		return nil, err
	}
	defer tarFile.Close()

	files := make([]TarFileInfo, 0)
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
		r := NewRollsum()
		w := io.MultiWriter(h, r)
		if _, err := io.Copy(w, rdr); err != nil {
			return nil, err
		}

		fileInfo := TarFileInfo{
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

	// Sort, smallest files first
	sort.Slice(files, func(i, j int) bool {
		return files[i].size < files[j].size
	})

	info := TarInfo{files: files}
	return &info, nil
}

// This is not called for files that can be used as-is, only for files that would
// be diffed with bsdiff or rollsums
func isDeltaCandidate(file *TarFileInfo) bool {
	// Look for known non-delta-able files (currently just compression)
	// NB: We explicitly don't have .gz here in case someone might be
	// using --rsyncable for that.
	if strings.HasPrefix(file.basename, ".xz") ||
		strings.HasPrefix(file.basename, ".bz2") {
		return false
	}

	return true
}

func nameIsSimilar(a *TarFileInfo, b *TarFileInfo, fuzzy int) bool {
	if fuzzy == 0 {
		return a.basename == b.basename
	} else {
		aa := strings.SplitAfterN(a.basename, ".", 2)[0]
		bb := strings.SplitAfterN(b.basename, ".", 2)[0]
		return aa == bb
	}
}

func extractDeltaData(tarGzFile io.Reader, sourceByIndex map[int]*SourceInfo, dest *os.File) error {
	offset := int64(0)

	tarFile, err := gzip.NewReader(tarGzFile)
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

func analyzeForDelta(old *TarInfo, new *TarInfo, oldFile io.Reader) (*DeltaAnalysis, error) {
	sourceInfos := make([]SourceInfo, 0, len(old.files))
	for i := range old.files {
		sourceInfos = append(sourceInfos, SourceInfo{file: &old.files[i]})
	}

	sourceBySha1 := make(map[string]*SourceInfo)
	sourceByPath := make(map[string]*SourceInfo)
	sourceByIndex := make(map[int]*SourceInfo)
	for i := range sourceInfos {
		s := &sourceInfos[i]
		if !s.file.overwritten {
			sourceBySha1[s.file.sha1] = s
			sourceByPath[s.file.path] = s
			sourceByIndex[s.file.index] = s
		}
	}

	targetInfos := make([]TargetInfo, 0, len(new.files))

	for i := range new.files {
		file := &new.files[i]
		// First look for exact content match
		usedForDelta := false
		var source *SourceInfo
		sha1Source := sourceBySha1[file.sha1]
		// If same sha1 and size, use original total size
		if sha1Source != nil && file.size == sha1Source.file.size {
			source = sha1Source
		}
		if source == nil && isDeltaCandidate(file) {
			// No exact match, try to find a useful source

			// If size is vastly different not useful to delta
			minSize := file.size - file.size*similarityPercentThreshold/100
			maxSize := file.size + file.size*similarityPercentThreshold/100

			// First check by exact pathname match
			s := sourceByPath[file.path]

			if s != nil && isDeltaCandidate(s.file) && s.file.size >= minSize && s.file.size < maxSize {
				usedForDelta = true
				source = s
			} else {
				// Check for moved (first) or renamed (second) versions
				lower := 0
				upper := len(sourceInfos)
				for fuzzy := 0; fuzzy < 2 && source == nil; fuzzy++ {
					for j := lower; j < upper; j++ {
						s = &sourceInfos[j]
						if !isDeltaCandidate(s.file) {
							continue
						}

						if s.file.size < minSize {
							lower++
							continue
						}

						if s.file.size > maxSize {
							break
						}

						if !nameIsSimilar(file, s.file, fuzzy) {
							continue
						}

						usedForDelta = true
						source = s
						break
					}
				}
			}
		}

		var rollsumMatches *RollsumMatches
		if source != nil {
			source.usedForDelta = usedForDelta

			if usedForDelta {
				rollsumMatches = ComputeRollsumMatches(source.file.blobs, file.blobs)
			}
		}
		info := TargetInfo{file: file, source: source, rollsumMatches: rollsumMatches}
		targetInfos = append(targetInfos, info)
	}

	targetInfoByIndex := make(map[int]*TargetInfo)
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

	return &DeltaAnalysis{targetInfos: targetInfos, targetInfoByIndex: targetInfoByIndex, sourceInfos: sourceInfos, sourceData: tmpfile}, nil
}
