package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
)

func main() {
	var file string
	flag.StringVar(&file, "file", "", "gog file path")
	flag.Parse()

	if err := run(file); err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
}

func run(file string) error {
	if file == "" {
		return errors.New("file not provided")
	}

	return nil
}
