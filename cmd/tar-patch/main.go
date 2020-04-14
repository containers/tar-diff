package main

import (
	"flag"
	"fmt"
	"github.com/alexlarsson/tar-diff"
	"os"
	"path"
)

var version = flag.Bool("version", false, "Show version")

func main() {
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [OPION] file.tardiff /path/to/content destination.tar\n", path.Base(os.Args[0]))
		fmt.Fprintf(flag.CommandLine.Output(), "Options:\n")
		flag.PrintDefaults()
	}

	flag.Parse()

	if *version {
		fmt.Printf("%s %s\n", path.Base(os.Args[0]), tar_diff.VERSION)
		return
	}

	if flag.NArg() != 3 {
		flag.Usage()
		os.Exit(1)
	}

	deltaFilename := flag.Arg(0)
	extractedDir := flag.Arg(1)
	patchedFilename := flag.Arg(2)

	deltaFile, err := os.Open(deltaFilename)
	if err != nil {
		fmt.Fprintf(flag.CommandLine.Output(), "Unable to open %s: %s\n", deltaFilename, err)
		os.Exit(1)
	}
	defer deltaFile.Close()

	patchedFile, err := os.Create(patchedFilename)
	if err != nil {
		fmt.Fprintf(flag.CommandLine.Output(), "Unable to create %s: %s\n", patchedFilename, err)
		os.Exit(1)
	}
	defer patchedFile.Close()

	err = tar_diff.ApplyDelta(deltaFile, extractedDir, patchedFile)
	if err != nil {
		fmt.Fprintf(flag.CommandLine.Output(), "Error applying diff: %s\n", err)
		os.Exit(1)
	}
}
