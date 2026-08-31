package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"path/filepath"
	"strconv"
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
	if err := run([]string{"-h"}, strings.NewReader(""), &stdout, &stderr); err != nil {
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

func TestParseProducerFlags(t *testing.T) {
	if m, err := parseProducerFlags([]string{"-m", "3"}, io.Discard); err != nil || m != 3 {
		t.Fatalf("expected m=3, got m=%d, err=%v", m, err)
	}

	for _, args := range [][]string{
		{},
		{"-m", "0"},
		{"-m", "-1"},
		{"-n", "2"},
		{"-m", "2", "unexpected"},
	} {
		if _, err := parseProducerFlags(args, io.Discard); err == nil {
			t.Errorf("expected %v to fail", args)
		}
	}
}

func TestParseControlCommand(t *testing.T) {
	for _, line := range []string{"start 10", "stop 11"} {
		if _, err := parseControlCommand(line); err != nil {
			t.Errorf("expected %q to be valid: %v", line, err)
		}
	}
	for _, line := range []string{"", "start", "pause 10", "stop -1", "start later", "stop 10 now"} {
		if _, err := parseControlCommand(line); err == nil {
			t.Errorf("expected %q to fail", line)
		}
	}
}

func TestProducerStopsBeforeStartWithoutFrame(t *testing.T) {
	start := time.Now().Unix() + 2
	input := fmt.Sprintf("start %d\nstop %d\n", start, start)
	var output bytes.Buffer
	if err := run([]string{"producer", "-m", "1"}, strings.NewReader(input), &output, io.Discard); err != nil {
		t.Fatalf("run producer: %v", err)
	}
	if output.Len() != 0 {
		t.Fatalf("expected no frame, got %q", output.String())
	}
}

func TestProducerStreamsConsecutiveFrames(t *testing.T) {
	start := time.Now().Unix() + 1
	input := fmt.Sprintf("start %d\nstop %d\n", start, start+2)
	var output bytes.Buffer
	if err := runProducer(2, strings.NewReader(input), &output); err != nil {
		t.Fatalf("run producer: %v", err)
	}

	decoder := json.NewDecoder(&output)
	for index := 0; index < 2; index++ {
		var frame producerFrame
		if err := decoder.Decode(&frame); err != nil {
			t.Fatalf("decode frame %d: %v", index, err)
		}
		if frame.Second != start+int64(index) {
			t.Errorf("frame %d: expected second %d, got %d", index, start+int64(index), frame.Second)
		}
		var total uint64
		for _, count := range frame.Counts {
			total += count
		}
		if total == 0 {
			t.Errorf("frame %d: expected generated values", index)
		}
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); err != io.EOF {
		t.Fatalf("expected exactly two frames, got %v", err)
	}
}

func TestProducerEOFStopsGenerators(t *testing.T) {
	reader, writer := io.Pipe()
	result := make(chan error, 1)
	go func() {
		result <- runProducer(2, reader, io.Discard)
	}()

	fmt.Fprintf(writer, "start %d\n", time.Now().Unix())
	time.Sleep(30 * time.Millisecond)
	writer.Close()

	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("run producer: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("producer did not stop after stdin EOF")
	}
}

func TestMultiProcessRun(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "concurrent-counter")
	build := exec.Command("go", "build", "-o", binary, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build executable: %v\n%s", err, output)
	}

	var stdout, stderr bytes.Buffer
	command := exec.Command(binary, "-m", "2", "-n", "2", "-run-for", "2s")
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		t.Fatalf("run executable: %v\nstderr: %s", err, stderr.String())
	}
	if stderr.Len() != 0 {
		t.Errorf("expected no diagnostics, got %q", stderr.String())
	}

	decoder := json.NewDecoder(&stdout)
	var previous int64
	for index := 0; index < 2; index++ {
		var record outputRecord
		if err := decoder.Decode(&record); err != nil {
			t.Fatalf("decode record %d: %v\nstdout: %s", index, err, stdout.String())
		}
		second, err := strconv.ParseInt(record.Time, 10, 64)
		if err != nil {
			t.Fatalf("record %d has invalid time %q", index, record.Time)
		}
		if index > 0 && second != previous+1 {
			t.Errorf("record %d: expected timestamp %d, got %d", index, previous+1, second)
		}
		previous = second
		if len(record.Counts) != 10 {
			t.Errorf("record %d: expected ten count keys, got %v", index, record.Counts)
		}
		var total uint64
		for value := 0; value < 10; value++ {
			total += record.Counts[strconv.Itoa(value)]
		}
		if total == 0 {
			t.Errorf("record %d: expected generated values", index)
		}
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		t.Fatalf("expected exactly two JSON records, got %v", err)
	}

	stdout.Reset()
	stderr.Reset()
	invalid := exec.Command(binary, "-n", "0")
	invalid.Stdout = &stdout
	invalid.Stderr = &stderr
	if err := invalid.Run(); err == nil {
		t.Fatal("expected invalid public arguments to fail")
	}
	if stdout.Len() != 0 || !strings.Contains(stderr.String(), "n must be greater than 0") {
		t.Fatalf("unexpected invalid-argument output: stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
}
