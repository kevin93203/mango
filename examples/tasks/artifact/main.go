package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	name := flag.String("name", "artifact-task", "name included in the generated artifact")
	output := flag.String("output", "examples/artifacts/example-output.txt", "relative or absolute output path")
	content := flag.String("content", "artifact metadata example", "content written to the artifact")
	flag.Parse()

	if strings.TrimSpace(*output) == "" {
		fmt.Fprintln(os.Stderr, "output path must not be empty")
		os.Exit(2)
	}

	if directory := filepath.Dir(*output); directory != "." && directory != "" {
		if err := os.MkdirAll(directory, 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "create output directory: %v\n", err)
			os.Exit(1)
		}
	}

	payload := fmt.Sprintf("name=%s\ncontent=%s\n", *name, strings.TrimRight(*content, "\r\n"))
	if err := os.WriteFile(*output, []byte(payload), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "write artifact: %v\n", err)
		os.Exit(1)
	}

	info, err := os.Stat(*output)
	if err != nil {
		fmt.Fprintf(os.Stderr, "stat artifact: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("[%s] wrote %s (%d bytes)\n", *name, *output, info.Size())
}
