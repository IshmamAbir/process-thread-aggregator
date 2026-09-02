package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

// counts stores totals for values 0 through 9 at matching indexes.
// counts は、0から9までの数を同じ番号の場所に保存します。
type counts [10]uint64

// producerFrame carries one producer's counts for one Unix second.
// producerFrame は、1つのプロデューサーがUnix時間の1秒で数えた結果を持ちます。
type producerFrame struct {
	Second int64  `json:"second"`
	Counts counts `json:"counts"`
}

// producerCounter safely groups one producer's generated values by second.
// producerCounter は、1つのプロデューサーで作った数を秒ごとに安全に数えます。
type producerCounter struct {
	// one lock per producer
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

// controlCommand holds a start or stop command and its Unix second.
// controlCommand は、開始か終了のコマンドとUnix秒を持ちます。
type controlCommand struct {
	name   string
	second int64
}

// parseControlCommand reads and validates one control-command line.
// parseControlCommand は、1行のコマンドを読み、正しい形か確認します。
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

// stopRequest carries a stop time or a control-read error.
// stopRequest は、終了する秒、またはコマンドの読み取りエラーを持ちます。
type stopRequest struct {
	cutoff int64
	err    error
}

// runProducer reads control commands, runs generators, and writes completed frames.
// runProducer は、コマンドを読み、数を作り、終わった秒のフレームを書きます。
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
