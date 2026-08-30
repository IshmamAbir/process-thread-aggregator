package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"
)

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type config struct {
	m      int
	n      int
	runFor time.Duration
}

func parseFlags(args []string, stderr io.Writer) (*config, error) {
	fs := flag.NewFlagSet("concurrent-counter", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var cfg config

	fs.IntVar(&cfg.m, "m", 4, "generator threads per producer (must be > 0)")
	fs.IntVar(&cfg.n, "n", 2, "producer processes (must be > 0)")
	fs.DurationVar(&cfg.runFor, "run-for", 0, "optional test/demo duration (0 means continuous)")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	if fs.NArg() > 0 {
		return nil, errors.New("unexpected positional arguments")
	}

	if cfg.m <= 0 {
		return nil, errors.New("m must be greater than 0")
	}
	if cfg.n <= 0 {
		return nil, errors.New("n must be greater than 0")
	}
	if cfg.runFor < 0 {
		return nil, errors.New("run-for duration cannot be negative")
	}

	return &cfg, nil
}

func run(args []string, stdout, stderr io.Writer) error {
	_, err := parseFlags(args, stderr)
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}
	if err != nil {
		return err
	}

	return nil
}
