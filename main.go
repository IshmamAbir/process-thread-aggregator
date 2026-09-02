package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "producer" {
		signal.Ignore(os.Interrupt)
	}
	if err := run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	if len(args) > 0 && args[0] == "producer" {
		m, err := parseProducerFlags(args[1:], stderr)
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		if err != nil {
			return err
		}
		return runProducer(m, stdin, stdout)
	}

	cfg, err := parseFlags(args, stderr)
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	return runParent(ctx, cfg, stdout, stderr)
}
