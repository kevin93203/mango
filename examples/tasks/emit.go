package main

import (
	"flag"
	"fmt"
	"os"
	"time"
)

func main() {
	name := flag.String("name", "go-task", "name printed in the task log")
	steps := flag.Int("steps", 3, "number of steps to emit")
	delay := flag.Duration("delay", 250*time.Millisecond, "delay between steps")
	fail := flag.Bool("fail", false, "exit with a failure after the steps")
	flag.Parse()

	if *steps < 1 {
		fmt.Fprintln(os.Stderr, "steps must be positive")
		os.Exit(2)
	}

	fmt.Printf("[%s] started (pid=%d)\n", *name, os.Getpid())
	for step := 1; step <= *steps; step++ {
		fmt.Printf("[%s] step %d/%d\n", *name, step, *steps)
		time.Sleep(*delay)
	}

	if *fail {
		fmt.Fprintf(os.Stderr, "[%s] intentional failure\n", *name)
		os.Exit(7)
	}
	fmt.Printf("[%s] completed\n", *name)
}
