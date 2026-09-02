package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"
)

func main() {
	iterations := flag.Int("iterations", 5, "number of iterations")
	interval := flag.Duration("interval", 500*time.Millisecond, "delay between iterations")
	fail := flag.Bool("fail", false, "exit with code 2 after producing logs")
	flag.Parse()

	stderr := log.New(os.Stderr, "one-task stderr: ", log.LstdFlags|log.Lmicroseconds)
	if *iterations < 1 {
		stderr.Println("iterations must be at least 1")
		os.Exit(2)
	}
	if *interval < 0 {
		stderr.Println("interval cannot be negative")
		os.Exit(2)
	}

	fmt.Printf("one-task started: pid=%d iterations=%d interval=%s\n", os.Getpid(), *iterations, interval.String())
	stderr.Println("task diagnostic stream started")

	for i := 1; i <= *iterations; i++ {
		fmt.Printf("task stdout: iteration %d/%d\n", i, *iterations)
		if i%2 == 1 {
			stderr.Printf("task stderr: diagnostic for iteration %d/%d", i, *iterations)
		}
		if i < *iterations {
			time.Sleep(*interval)
		}
	}

	if *fail {
		stderr.Println("task failed intentionally with exit code 2")
		os.Exit(2)
	}
	fmt.Println("one-task completed successfully")
}
