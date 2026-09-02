package main

import (
	"errors"
	"flag"
	"io"
	"time"
)

type config struct {
	m      int
	n      int
	runFor time.Duration
}

// parseFlags parses and validates the public command-line options.
// parseFlags は、ユーザーが使うオプションを読み、値が正しいか確認します。
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

// parseProducerFlags parses and validates options for the internal producer.
// parseProducerFlags は、プロデューサーだけが使うオプションを読み、値が正しいか確認します。
func parseProducerFlags(args []string, stderr io.Writer) (int, error) {
	fs := flag.NewFlagSet("concurrent-counter producer", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var m int
	fs.IntVar(&m, "m", 0, "generator threads (must be > 0)")
	if err := fs.Parse(args); err != nil {
		return 0, err
	}
	if fs.NArg() > 0 {
		return 0, errors.New("unexpected positional arguments")
	}
	if m <= 0 {
		return 0, errors.New("m must be greater than 0")
	}
	return m, nil
}
