package protocol

import (
	"bytes"
	"regexp"
	"testing"
)

func TestDeltaOperationConstants(t *testing.T) {
	tests := []struct {
		name string
		got  int
		want int
	}{
		{name: "data", got: DeltaOpData, want: 0},
		{name: "open", got: DeltaOpOpen, want: 1},
		{name: "copy", got: DeltaOpCopy, want: 2},
		{name: "add data", got: DeltaOpAddData, want: 3},
		{name: "seek", got: DeltaOpSeek, want: 4},
		{name: "zstd dictionary", got: DeltaOpZstdDict, want: 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Errorf("got %d, want %d", tt.got, tt.want)
			}
		})
	}
}

func TestDeltaHeaders(t *testing.T) {
	tests := []struct {
		name string
		got  [8]byte
		want [8]byte
	}{
		{name: "v1", got: DeltaHeader, want: [8]byte{'t', 'a', 'r', 'd', 'f', '1', '\n', 0}},
		{name: "v2", got: DeltaHeaderv2, want: [8]byte{'t', 'a', 'r', 'd', 'f', '2', '\n', 0}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !bytes.Equal(tt.got[:], tt.want[:]) {
				t.Errorf("got %q, want %q", tt.got, tt.want)
			}
		})
	}
}

func TestVersionFormat(t *testing.T) {
	versionPattern := regexp.MustCompile(`^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$`)
	if !versionPattern.MatchString(VERSION) {
		t.Errorf("VERSION %q does not match the vMAJOR.MINOR.PATCH format", VERSION)
	}
}

func TestCleanPath(t *testing.T) {
	tests := []struct {
		name string
		path string
		want string
	}{
		{name: "relative", path: "dir/file", want: "dir/file"},
		{name: "absolute", path: "/dir/file", want: "dir/file"},
		{name: "duplicate separators", path: "dir//file", want: "dir/file"},
		{name: "internal traversal", path: "dir/../file", want: "file"},
		{name: "leading traversal", path: "../file", want: "file"},
		{name: "outside root", path: "../../file", want: "file"},
		{name: "empty", path: "", want: ""},
		{name: "root", path: "/", want: ""},
		{name: "current directory", path: ".", want: ""},
		{name: "parent directory", path: "..", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CleanPath(tt.path); got != tt.want {
				t.Errorf("CleanPath(%q) = %q, want %q", tt.path, got, tt.want)
			}
		})
	}
}
