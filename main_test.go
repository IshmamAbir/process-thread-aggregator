package main

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestParseFlags_Defaults(t *testing.T) {
	var stderr bytes.Buffer
	cfg, err := parseFlags([]string{}, &stderr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.m != 4 {
		t.Errorf("expected default m=4, got %d", cfg.m)
	}
	if cfg.n != 2 {
		t.Errorf("expected default n=2, got %d", cfg.n)
	}
	if cfg.runFor != 0 {
		t.Errorf("expected default runFor=0, got %v", cfg.runFor)
	}
}

func TestParseFlags_Explicit(t *testing.T) {
	var stderr bytes.Buffer
	cfg, err := parseFlags([]string{"-m", "10", "-n", "5", "-run-for", "5s"}, &stderr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.m != 10 {
		t.Errorf("expected m=10, got %d", cfg.m)
	}
	if cfg.n != 5 {
		t.Errorf("expected n=5, got %d", cfg.n)
	}
	if cfg.runFor != 5*time.Second {
		t.Errorf("expected runFor=5s, got %v", cfg.runFor)
	}
}

func TestParseFlags_InvalidArgs(t *testing.T) {
	tests := []struct {
		name string
		args []string
		err  string
	}{
		{"m zero", []string{"-m", "0"}, "m must be greater than 0"},
		{"m negative", []string{"-m", "-1"}, "m must be greater than 0"},
		{"n zero", []string{"-n", "0"}, "n must be greater than 0"},
		{"n negative", []string{"-n", "-1"}, "n must be greater than 0"},
		{"run-for negative", []string{"-run-for", "-1s"}, "run-for duration cannot be negative"},
		{"unexpected positional", []string{"-m", "4", "unexpected"}, "unexpected positional arguments"},
		{"unknown flag", []string{"-unknown"}, "flag provided but not defined: -unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stderr bytes.Buffer
			_, err := parseFlags(tt.args, &stderr)
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.err) {
				t.Errorf("expected error containing %q, got %q", tt.err, err.Error())
			}
		})
	}
}

func TestParseFlags_PublicHelp(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if err := run([]string{"-h"}, &stdout, &stderr); err != nil {
		t.Fatalf("unexpected help error: %v", err)
	}

	output := stderr.String()
	if !strings.Contains(output, "-m int") {
		t.Errorf("expected help output to contain -m")
	}
	if !strings.Contains(output, "-n int") {
		t.Errorf("expected help output to contain -n")
	}
	if !strings.Contains(output, "-run-for duration") {
		t.Errorf("expected help output to contain -run-for")
	}
	if strings.Contains(output, "-listen-addr") {
		t.Errorf("expected help output not to contain internal producer flags")
	}
}

func TestRun(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := run([]string{"-m", "2", "-n", "2"}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("unexpected error from run: %v", err)
	}
	if stdout.Len() != 0 {
		t.Errorf("expected no stdout output, got %q", stdout.String())
	}
}
