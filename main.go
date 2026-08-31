package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
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

type producerChild struct {
	command *exec.Cmd
	control io.WriteCloser
	output  io.ReadCloser
	done    chan struct{}
}

type producerEvent struct {
	producer int
	frame    producerFrame
	hasFrame bool
	err      error
}

func startProducer(executable string, m int, stderr io.Writer) (*producerChild, error) {
	command := exec.Command(executable, "producer", "-m", strconv.Itoa(m))
	command.Stderr = stderr

	control, err := command.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("create producer stdin pipe: %w", err)
	}
	output, err := command.StdoutPipe()
	if err != nil {
		control.Close()
		return nil, fmt.Errorf("create producer stdout pipe: %w", err)
	}
	if err := command.Start(); err != nil {
		control.Close()
		output.Close()
		return nil, fmt.Errorf("start producer: %w", err)
	}

	return &producerChild{command, control, output, make(chan struct{})}, nil
}

func readProducer(producer int, child *producerChild, events chan<- producerEvent, cancelled <-chan struct{}) {
	defer close(child.done)
	defer child.output.Close()

	send := func(event producerEvent) bool {
		select {
		case events <- event:
			return true
		case <-cancelled:
			return false
		}
	}

	decoder := json.NewDecoder(child.output)
	for {
		var frame producerFrame
		err := decoder.Decode(&frame)
		if err == nil {
			if send(producerEvent{producer: producer, frame: frame, hasFrame: true}) {
				continue
			}
			io.Copy(io.Discard, child.output)
			child.command.Wait()
			return
		}
		if errors.Is(err, io.EOF) {
			err = child.command.Wait()
			if err != nil {
				err = fmt.Errorf("producer %d exited: %w", producer, err)
			}
			send(producerEvent{producer: producer, err: err})
			return
		}

		send(producerEvent{producer: producer, err: fmt.Errorf("producer %d: decode frame: %w", producer, err)})
		io.Copy(io.Discard, child.output)
		child.command.Wait()
		return
	}
}

func abortChildren(children []*producerChild, cancelReaders func(), wait time.Duration) {
	for _, child := range children {
		if child.control != nil {
			child.control.Close()
			child.control = nil
		}
	}
	cancelReaders()

	allDone := make(chan struct{})
	go func() {
		for _, child := range children {
			<-child.done
		}
		close(allDone)
	}()

	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-allDone:
		return
	case <-timer.C:
	}

	for _, child := range children {
		select {
		case <-child.done:
		default:
			child.command.Process.Kill()
		}
	}
	<-allDone
}

func allChildrenDone(children []*producerChild) bool {
	for _, child := range children {
		select {
		case <-child.done:
		default:
			return false
		}
	}
	return true
}

func runParent(ctx context.Context, cfg *config, stdout, stderr io.Writer) error {
	executable, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve current executable: %w", err)
	}

	events := make(chan producerEvent, cfg.n)
	cancelled := make(chan struct{})
	var cancelOnce sync.Once
	cancelReaders := func() { cancelOnce.Do(func() { close(cancelled) }) }
	defer cancelReaders()

	children := make([]*producerChild, 0, cfg.n)
	fail := func(err error) error {
		abortChildren(children, cancelReaders, 2*time.Second)
		return err
	}
	for producer := 0; producer < cfg.n; producer++ {
		child, err := startProducer(executable, cfg.m, stderr)
		if err != nil {
			return fail(fmt.Errorf("producer %d: %w", producer, err))
		}
		children = append(children, child)
		go readProducer(producer, child, events, cancelled)
	}

	start := time.Now().Truncate(time.Second).Add(2 * time.Second).Unix()
	for producer, child := range children {
		if _, err := fmt.Fprintf(child.control, "start %d\n", start); err != nil {
			return fail(fmt.Errorf("producer %d: send start: %w", producer, err))
		}
	}

	aggregator := newAggregator(cfg.n, start, stdout)
	interrupt := ctx.Done()
	var runDone <-chan time.Time
	var runTimer *time.Timer
	var timedCutoff int64
	if cfg.runFor > 0 {
		deadline := time.Unix(start, 0).Add(cfg.runFor)
		runTimer = time.NewTimer(max(0, time.Until(deadline)))
		defer runTimer.Stop()
		runDone = runTimer.C
		timedCutoff = deadline.Truncate(time.Second).Unix()
	}

	shuttingDown := false
	cutoff := int64(0)
	var shutdownDone <-chan time.Time
	var shutdownTimer *time.Timer
	beginShutdown := func(exclusiveCutoff int64) error {
		for producer, child := range children {
			if _, err := fmt.Fprintf(child.control, "stop %d\n", exclusiveCutoff); err != nil {
				return fmt.Errorf("producer %d: send stop: %w", producer, err)
			}
			if err := child.control.Close(); err != nil {
				return fmt.Errorf("producer %d: close control pipe: %w", producer, err)
			}
			child.control = nil
		}
		shuttingDown = true
		cutoff = exclusiveCutoff
		interrupt = nil
		runDone = nil
		shutdownTimer = time.NewTimer(2 * time.Second)
		shutdownDone = shutdownTimer.C
		return nil
	}

	exited := 0
	for exited < len(children) {
		select {
		case event := <-events:
			if event.err != nil {
				return fail(event.err)
			}
			if event.hasFrame {
				if shuttingDown && event.frame.Second >= cutoff {
					return fail(fmt.Errorf("producer %d: frame at or after cutoff %d", event.producer, cutoff))
				}
				if err := aggregator.add(event.producer, event.frame); err != nil {
					return fail(err)
				}
				continue
			}
			exited++
			if !shuttingDown {
				return fail(fmt.Errorf("producer %d exited before shutdown", event.producer))
			}
		case <-runDone:
			if err := beginShutdown(timedCutoff); err != nil {
				return fail(err)
			}
		case <-interrupt:
			if err := beginShutdown(max(start, time.Now().Unix())); err != nil {
				return fail(err)
			}
		case <-shutdownDone:
			if allChildrenDone(children) {
				shutdownDone = nil
				continue
			}
			abortChildren(children, cancelReaders, 0)
			return errors.New("producer shutdown exceeded two seconds")
		}
	}
	if shutdownTimer != nil {
		shutdownTimer.Stop()
	}

	for producer, next := range aggregator.next {
		if next != cutoff {
			return fmt.Errorf("producer %d: expected frames through %d, stopped at %d", producer, cutoff-1, next-1)
		}
	}
	if len(aggregator.pending) != 0 {
		return errors.New("incomplete aggregate buckets at shutdown")
	}
	return nil
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

type controlCommand struct {
	name   string
	second int64
}

func parseControlCommand(line string) (controlCommand, error) {
	fields := strings.Fields(line)
	if len(fields) != 2 || (fields[0] != "start" && fields[0] != "stop") {
		return controlCommand{}, fmt.Errorf("invalid control command %q", line)
	}
	second, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil || second < 0 {
		return controlCommand{}, fmt.Errorf("invalid control second %q", fields[1])
	}
	return controlCommand{fields[0], second}, nil
}

type stopRequest struct {
	cutoff int64
	err    error
}

func runProducer(m int, stdin io.Reader, stdout io.Writer) error {
	scanner := bufio.NewScanner(stdin)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return fmt.Errorf("read start command: %w", err)
		}
		return errors.New("missing start command")
	}
	startCommand, err := parseControlCommand(scanner.Text())
	if err != nil || startCommand.name != "start" {
		return errors.New("expected start <unix-second>")
	}

	stopRequests := make(chan stopRequest, 1)
	go func() {
		if scanner.Scan() {
			command, err := parseControlCommand(scanner.Text())
			if err == nil && command.name != "stop" {
				err = errors.New("expected stop <exclusive-unix-second>")
			}
			stopRequests <- stopRequest{command.second, err}
			return
		}
		if err := scanner.Err(); err != nil {
			stopRequests <- stopRequest{err: fmt.Errorf("read stop command: %w", err)}
			return
		}
		stopRequests <- stopRequest{cutoff: time.Now().Unix()}
	}()

	start := startCommand.second
	var cutoff int64
	hasCutoff := false
	if delay := time.Until(time.Unix(start, 0)); delay > 0 {
		timer := time.NewTimer(delay)
		select {
		case request := <-stopRequests:
			timer.Stop()
			if request.err != nil {
				return request.err
			}
			if request.cutoff <= start {
				return nil
			}
			cutoff, hasCutoff = request.cutoff, true
			<-time.After(time.Until(time.Unix(start, 0)))
		case <-timer.C:
		}
	}

	counter := newProducerCounter(time.Now)
	done := make(chan struct{})
	var generators sync.WaitGroup
	for range m {
		generators.Add(1)
		go func() {
			defer generators.Done()
			runtime.LockOSThread()
			defer runtime.UnlockOSThread()

			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					counter.add(rand.IntN(10))
				case <-done:
					return
				}
			}
		}()
	}

	var stopOnce sync.Once
	stopGenerators := func() {
		stopOnce.Do(func() {
			close(done)
			generators.Wait()
		})
	}
	defer stopGenerators()

	encoder := json.NewEncoder(stdout)
	writeFrames := func(from, to int64) error {
		for _, frame := range counter.snapshot(from, to) {
			if err := encoder.Encode(frame); err != nil {
				return fmt.Errorf("write producer frame: %w", err)
			}
		}
		return nil
	}

	next := start
	for {
		if hasCutoff && time.Now().Unix() >= cutoff {
			stopGenerators()
			return writeFrames(next, cutoff)
		}

		boundary := next + 1
		timer := time.NewTimer(max(0, time.Until(time.Unix(boundary, 0))))
		var controls <-chan stopRequest = stopRequests
		if hasCutoff {
			controls = nil
		}
		select {
		case request := <-controls:
			timer.Stop()
			if request.err != nil {
				return request.err
			}
			cutoff, hasCutoff = request.cutoff, true
		case <-timer.C:
			if hasCutoff && boundary >= cutoff {
				stopGenerators()
				return writeFrames(next, cutoff)
			}
			if err := writeFrames(next, boundary); err != nil {
				return err
			}
			next = boundary
		}
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
