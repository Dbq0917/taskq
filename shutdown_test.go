package taskq_test

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vmihailenco/taskq/v3"
	"github.com/vmihailenco/taskq/v3/memqueue"
)

func TestGracefulShutdownRollsBackPartialBatch(t *testing.T) {
	var (
		attempt      int32
		mu           sync.Mutex
		downstream   []string
		firstStarted = make(chan struct{}, 1)
		rollbackDone = make(chan struct{}, 1)
		secondDone   = make(chan struct{}, 1)
		rollbackOnce sync.Once
	)

	opt := &taskq.QueueOptions{
		Name:            "shutdown-batch",
		MinNumWorker:    1,
		MaxNumWorker:    1,
		BufferSize:      1,
		ReservationSize: 1,
		Handler: taskq.HandlerFunc(func(msg *taskq.Message) error {
			if atomic.AddInt32(&attempt, 1) > 1 {
				mu.Lock()
				downstream = append(downstream, "record")
				mu.Unlock()
				secondDone <- struct{}{}
				return nil
			}

			mu.Lock()
			downstream = append(downstream, "partial")
			mu.Unlock()
			firstStarted <- struct{}{}

			<-msg.Ctx.Done()

			mu.Lock()
			downstream = nil
			mu.Unlock()
			rollbackOnce.Do(func() { rollbackDone <- struct{}{} })

			msg.Delay = time.Millisecond
			return msg.Ctx.Err()
		}),
	}

	q := memqueue.NewQueue(opt)
	p := q.Consumer()
	defer func() {
		_ = p.StopTimeout(time.Second)
		_ = q.CloseTimeout(time.Second)
	}()

	msg := taskq.NewMessage(context.Background())
	msg.TaskName = "shutdown-batch"
	if err := q.Add(msg); err != nil {
		t.Fatal(err)
	}

	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("batch did not start")
	}

	start := time.Now()
	err := p.StopTimeout(50 * time.Millisecond)
	if err == nil {
		t.Fatal("StopTimeout returned nil for an unfinished batch")
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("StopTimeout took %s, wanted about the 50ms budget", elapsed)
	}

	select {
	case <-rollbackDone:
	case <-time.After(time.Second):
		t.Fatal("partial downstream writes were not rolled back after cancellation")
	}

	deadline := time.Now().Add(time.Second)
	for {
		err := p.Start(context.Background())
		if err == nil {
			break
		}
		if !strings.Contains(err.Error(), "is stopping") {
			t.Fatalf("Start after timed-out stop failed: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("consumer did not settle after timed-out stop: %v", err)
		}
		time.Sleep(5 * time.Millisecond)
	}

	select {
	case <-secondDone:
	case <-time.After(2 * time.Second):
		t.Fatal("redelivered batch was not processed")
	}

	if err := p.StopTimeout(time.Second); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(downstream) != 1 {
		t.Fatalf("downstream records = %d (%v), wanted 1 after redelivery", len(downstream), downstream)
	}
}
