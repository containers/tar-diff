// Package protocol provides shared constants and data structures for tar-diff operations.
package protocol

import "path/filepath"

// Delta operation constants define the types of operations in a delta file.
const (
	DeltaOpData     = iota // Raw data operation
	DeltaOpOpen     = iota // Open file operation
	DeltaOpCopy     = iota // Copy from source operation
	DeltaOpAddData  = iota // Add new data operation
	DeltaOpSeek     = iota // Seek operation
	DeltaOpZstdDict = iota // zstd dictionary patch against open source file
)

// DeltaHeader is the magic for v1 tar-diff files (no DeltaOpZstdDict).
var DeltaHeader = [...]byte{'t', 'a', 'r', 'd', 'f', '1', '\n', 0}

// DeltaHeaderv2 is the magic when zstd-dict ops are possible (auto or zstd mode).
var DeltaHeaderv2 = [...]byte{'t', 'a', 'r', 'd', 'f', '2', '\n', 0}

// CleanPath cleans up the path lexically and prevents path traversal attacks.
// Any ".." that extends outside the first elements (or the root itself) is invalid and returns "".
// Uses filepath.Clean for proper cross-platform path handling (Windows backslashes, drive letters).
// This is a security-critical function used by both tar-diff and tar-patch packages.
func CleanPath(pathName string) string {
	// A path with a volume name is absolute and can lead to path traversal on Windows.
	if filepath.VolumeName(pathName) != "" {
		return ""
	}

	// Convert to forward slashes for consistent processing
	pathName = filepath.ToSlash(pathName)

	// We make the path always absolute, that way filepath.Clean() ensures it never goes outside the top ("root") dir
	// even if its a relative path
	clean := filepath.Clean(filepath.Join("/", pathName))

	// Convert back to forward slashes to ensure consistent output
	clean = filepath.ToSlash(clean)

	// We clean the initial slash, making all result relative (or "" which is error)
	if len(clean) > 0 && clean[0] == '/' {
		return clean[1:]
	}
	return ""
}
