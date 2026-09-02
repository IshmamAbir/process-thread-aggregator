package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"time"
)

// outputRecord is the final JSON shape printed by the aggregator.
// outputRecord は、集計プロセスが出す最後のJSONの形です。
type outputRecord struct {
	Time   string            `json:"time"`
	Counts map[string]uint64 `json:"counts"`
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

// producerChild holds one child process, its pipes, and its completion signal.
// producerChild は、1つの子プロセス、パイプ、終了を知らせるチャネルを持ちます。
type producerChild struct {
	command *exec.Cmd
	control io.WriteCloser
	output  io.ReadCloser
	done    chan struct{}
}

// producerEvent carries a frame, exit, or error from one producer.
// producerEvent は、1つのプロデューサーのフレーム、終了、エラーを親へ送ります。
type producerEvent struct {
	producer int
	frame    producerFrame
	hasFrame bool
	err      error
}

// startProducer starts one child and returns the pipes used by the parent.
// startProducer は子プロセスを1つ始め、親が使うパイプを返します。
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

// readProducer decodes frames, sends events, and waits for the child to exit.
// readProducer はフレームを読み、イベントを送り、子プロセスの終了を待ちます。
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

// abortChildren requests shutdown, then kills children that exceed the wait time.
// abortChildren は終了を伝え、時間内に終わらない子プロセスを強制終了します。
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

// allChildrenDone reports whether every child reader has finished.
// allChildrenDone は、すべての子プロセスの読み取りが終わったかを返します。
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

// runParent starts producers, aggregates frames, and coordinates shutdown.
// runParent はプロデューサーを始め、結果を集計し、終了の処理を行います。
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
