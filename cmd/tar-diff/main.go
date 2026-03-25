// Package main implements the tar-diff command line tool for creating binary diffs between tar archives.
package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path"

	"github.com/containers/tar-diff/pkg/protocol"
	tardiff "github.com/containers/tar-diff/pkg/tar-diff"
)

var version = flag.Bool("version", false, "Show version")
var compressionLevel = flag.Int("compression-level", 3, "zstd compression level")
var maxBsdiffSize = flag.Int("max-bsdiff-size", 192, "Max file size in megabytes to consider using bsdiff, or 0 for no limit")

func main() {

	flag.Usage = func() {
		_, _ = fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [OPTION] old1.tar.gz [old2.tar.gz ...] new.tar.gz result.tardiff\n", path.Base(os.Args[0]))
		_, _ = fmt.Fprintf(flag.CommandLine.Output(), "Options:\n")
		flag.PrintDefaults()
	}

	flag.Parse()

	if *version {
		fmt.Printf("%s %s\n", path.Base(os.Args[0]), protocol.VERSION)
		return
	}

	if flag.NArg() < 3 {
		flag.Usage()
		os.Exit(1)
	}

	args := flag.Args()
	numOldFiles := len(args) - 2
	oldFilenames := args[0:numOldFiles]
	newFilename := args[numOldFiles]
	deltaFilename := args[numOldFiles+1]

	oldFiles := make([]io.ReadSeeker, numOldFiles)
	for i, oldFilename := range oldFilenames {
		file, err := os.Open(oldFilename)
		if err != nil {
			log.Fatalf("Error: %s", err)
		}
		defer file.Close()
		oldFiles[i] = file
	}

	newFile, err := os.Open(newFilename)
	if err != nil {
		log.Fatalf("Error: %s", err)
	}
	defer newFile.Close()

	deltaFile, err := os.Create(deltaFilename)
	if err != nil {
		log.Fatalf("Error: %s", err)
	}
	defer deltaFile.Close()

	options := tardiff.NewOptions()
	options.SetCompressionLevel(*compressionLevel)
	options.SetMaxBsdiffFileSize(int64(*maxBsdiffSize) * 1024 * 1024)

	err = tardiff.Diff(oldFiles, newFile, deltaFile, options)
	if err != nil {
		log.Fatalf("Error: %s", err)
	}

}
