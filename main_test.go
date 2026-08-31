package main

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestProducerCounterCountsAndBoundaries(t *testing.T) {
	now := time.Unix(100, 999_999_999)
	counter := newProducerCounter(func() time.Time { return now })

	for value := 0; value < 10; value++ {
		counter.add(value)
	}
	now = time.Unix(101, 0)
	counter.add(3)

	frames := counter.snapshot(100, 102)
	for value, count := range frames[0].Counts {
		if count != 1 {
			t.Errorf("value %d: expected 1 in second 100, got %d", value, count)
		}
	}
	if frames[1].Counts[3] != 1 {
		t.Errorf("expected exact-boundary value in second 101, got %v", frames[1].Counts)
	}
}

func TestProducerCounterSnapshotIncludesEmptyBucketsAndClearsState(t *testing.T) {
	counter := newProducerCounter(func() time.Time { return time.Unix(11, 0) })
	counter.add(2)

	frames := counter.snapshot(10, 12)
	if frames[0].Counts != (counts{}) {
		t.Errorf("expected empty second 10, got %v", frames[0].Counts)
	}
	if frames[1].Counts[2] != 1 {
		t.Errorf("expected value in second 11, got %v", frames[1].Counts)
	}
	for _, frame := range counter.snapshot(10, 12) {
		if frame.Counts != (counts{}) {
			t.Errorf("expected flushed second %d to be empty, got %v", frame.Second, frame.Counts)
		}
	}
}

func TestProducerCounterConcurrentUpdates(t *testing.T) {
	counter := newProducerCounter(func() time.Time { return time.Unix(20, 0) })
	const goroutines, updates = 8, 1_000

	var wg sync.WaitGroup
	for worker := 0; worker < goroutines; worker++ {
		wg.Add(1)
		go func(value int) {
			defer wg.Done()
			for range updates {
				counter.add(value)
			}
		}(worker % 10)
	}
	wg.Wait()

	var total uint64
	for _, count := range counter.snapshot(20, 21)[0].Counts {
		total += count
	}
	if total != goroutines*updates {
		t.Errorf("expected %d updates, got %d", goroutines*updates, total)
	}
}

func TestAggregatorMergesFramesInAscendingJSONOrder(t *testing.T) {
	var output bytes.Buffer
	aggregator := newAggregator(2, 30, &output)

	frames := []struct {
		producer int
		frame    producerFrame
	}{
		{0, producerFrame{Second: 30, Counts: counts{1, 2}}},
		{0, producerFrame{Second: 31, Counts: counts{5}}},
		{1, producerFrame{Second: 30, Counts: counts{3, 4}}},
		{1, producerFrame{Second: 31, Counts: counts{7}}},
	}
	for _, input := range frames {
		if err := aggregator.add(input.producer, input.frame); err != nil {
			t.Fatalf("add frame: %v", err)
		}
	}

	decoder := json.NewDecoder(&output)
	for index, want := range []struct {
		second string
		zero   uint64
		one    uint64
	}{{"30", 4, 6}, {"31", 12, 0}} {
		var got struct {
			Time   string            `json:"time"`
			Counts map[string]uint64 `json:"counts"`
		}
		if err := decoder.Decode(&got); err != nil {
			t.Fatalf("decode record %d: %v", index, err)
		}
		if got.Time != want.second || got.Counts["0"] != want.zero || got.Counts["1"] != want.one {
			t.Errorf("record %d: got %+v", index, got)
		}
		if len(got.Counts) != 10 {
			t.Errorf("record %d: expected all ten count keys, got %v", index, got.Counts)
		}
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); err != io.EOF {
		t.Errorf("expected exactly two records, got %v", err)
	}
}

func TestAggregatorRejectsNonConsecutiveFrames(t *testing.T) {
	aggregator := newAggregator(1, 40, io.Discard)
	if err := aggregator.add(0, producerFrame{Second: 41}); err == nil || !strings.Contains(err.Error(), "expected second 40") {
		t.Fatalf("expected consecutive-frame error, got %v", err)
	}
}

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
