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

	oldFilename := args[0]
	newFilename := args[1]
	deltaFilename := args[2]

	oldFile, err := os.Open(oldFilename)
	if err != nil {
		log.Fatalln("Unable to open %s: %s", oldFilename, err)
	}
	defer oldFile.Close()

	newFile, err := os.Open(newFilename)
	if err != nil {
		log.Fatalln("Unable to open %s: %s", newFilename, err)
	}
	defer newFile.Close()

	deltaFile, err := os.Create(deltaFilename)
	if err != nil {
		log.Fatalln("Unable to create %s: %s", deltaFilename, err)
	}

	err = tar_diff.GenerateDelta(oldFile, newFile, deltaFile)
	if err != nil {
		log.Fatalln("Error generating delta: %v", err)
	}

	err = deltaFile.Close()
	if err != nil {
		log.Fatalln("Error writing delta: %v", err)
	}

}
