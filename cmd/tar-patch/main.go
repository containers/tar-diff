package main

import (
	"github.com/alexlarsson/tar-diff"
	"log"
	"os"
)

func main() {
	args := os.Args[1:]
	if len(args) != 3 {
		log.Fatalln("Not enough arguments")
	}

	deltaFilename := args[0]
	extractedDir := args[1]
	patchedFilename := args[2]

	deltaFile, err := os.Open(deltaFilename)
	if err != nil {
		log.Fatalln("unexpected error: %v", err)
	}
	defer deltaFile.Close()

	patchedFile, err := os.Create(patchedFilename)
	if err != nil {
		log.Fatalln("unexpected error: %v", err)
	}
	defer patchedFile.Close()

	err = tar_diff.ApplyDelta(deltaFile, extractedDir, patchedFile)
	if err != nil {
		log.Fatalln("unexpected error: %v", err)
	}

}
