package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
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

// counts stores totals for values 0 through 9 at matching indexes.
// counts は、0から9までの数を同じ番号の場所に保存します。
type counts [10]uint64

// producerFrame carries one producer's counts for one Unix second.
// producerFrame は、1つのプロデューサーがUnix時間の1秒で数えた結果を持ちます。
type producerFrame struct {
	Second int64  `json:"second"`
	Counts counts `json:"counts"`
}

// outputRecord is the final JSON shape printed by the aggregator.
// outputRecord は、集計プロセスが出す最後のJSONの形です。
type outputRecord struct {
	Time   string            `json:"time"`
	Counts map[string]uint64 `json:"counts"`
}

// producerCounter safely groups one producer's generated values by second.
// producerCounter は、1つのプロデューサーで作った数を秒ごとに安全に数えます。
type producerCounter struct {
	// ponytail: one lock per producer; shard only if profiling shows M-thread contention.
	// ponytail: 1つのプロデューサーに1つのロックを使います。処理が遅いと確認できた時だけ分けます。
	mu      sync.Mutex
	buckets map[int64]counts
	now     func() time.Time
}

func newProducerCounter(now func() time.Time) *producerCounter {
	return &producerCounter{buckets: make(map[int64]counts), now: now}
}

func (c *producerCounter) add(value int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	second := c.now().Unix()
	bucket := c.buckets[second]
	bucket[value]++
	c.buckets[second] = bucket
}

// snapshot returns every bucket in [from, cutoff), including empty buckets, then clears them.
// snapshot は [from, cutoff) の秒を全部返します。空の秒も返し、その後データを消します。
func (c *producerCounter) snapshot(from, cutoff int64) []producerFrame {
	c.mu.Lock()
	defer c.mu.Unlock()

	var frames []producerFrame
	for second := from; second < cutoff; second++ {
		frames = append(frames, producerFrame{Second: second, Counts: c.buckets[second]})
		delete(c.buckets, second)
	}
	return frames
}

// aggregateBucket holds the partial sum and received producer count for one second.
// aggregateBucket は、1秒の途中の合計と、結果を受け取ったプロデューサーの数を持ちます。
type aggregateBucket struct {
	counts   counts
	received int
}

// aggregator validates and combines frames; one event loop owns it, so it needs no mutex.
// aggregator はフレームを確認して合計します。1つのイベントループだけが使うため、mutexは必要ありません。
type aggregator struct {
	next    []int64
	pending map[int64]aggregateBucket
	encoder *json.Encoder
}

// newAggregator prepares state for N producers starting at the same second.
// newAggregator は、同じ秒から始まるN個のプロデューサー用の状態を作ります。
func newAggregator(producers int, firstSecond int64, output io.Writer) *aggregator {
	next := make([]int64, producers)
	for producer := range next {
		next[producer] = firstSecond
	}
	return &aggregator{
		next:    next,
		pending: make(map[int64]aggregateBucket),
		encoder: json.NewEncoder(output),
	}
}

// add accepts one producer's next frame and writes JSON when all producers report that second.
// add は1つのプロデューサーから次のフレームを受け取ります。全プロデューサーの結果がそろうとJSONを出します。
func (a *aggregator) add(producer int, frame producerFrame) error {
	if producer < 0 || producer >= len(a.next) {
		return fmt.Errorf("unknown producer %d", producer)
	}
	if frame.Second != a.next[producer] {
		return fmt.Errorf("producer %d: expected second %d, got %d", producer, a.next[producer], frame.Second)
	}
	a.next[producer]++

	bucket := a.pending[frame.Second]
	for value, count := range frame.Counts {
		bucket.counts[value] += count
	}
	bucket.received++
	if bucket.received < len(a.next) {
		a.pending[frame.Second] = bucket
		return nil
	}
	delete(a.pending, frame.Second)

	outputCounts := make(map[string]uint64, len(bucket.counts))
	for value, count := range bucket.counts {
		outputCounts[strconv.Itoa(value)] = count
	}
	return a.encoder.Encode(outputRecord{strconv.FormatInt(frame.Second, 10), outputCounts})
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
