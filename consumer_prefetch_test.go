package taskq_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vmihailenco/taskq/v3"
)

type prefetchQueue struct {
	opt   *taskq.QueueOptions
	calls int32
	lastN int32
	rtt   time.Duration
}

func (q *prefetchQueue) Name() string                     { return q.opt.Name }
func (q *prefetchQueue) String() string                   { return q.opt.Name }
func (q *prefetchQueue) Options() *taskq.QueueOptions     { return q.opt }
func (q *prefetchQueue) Consumer() taskq.QueueConsumer    { return taskq.NewConsumer(q) }
func (q *prefetchQueue) Len() (int, error)                { return 0, nil }
func (q *prefetchQueue) Add(*taskq.Message) error         { return nil }
func (q *prefetchQueue) Release(*taskq.Message) error     { return nil }
func (q *prefetchQueue) Delete(*taskq.Message) error      { return nil }
func (q *prefetchQueue) Purge() error                     { return nil }
func (q *prefetchQueue) Close() error                     { return nil }
func (q *prefetchQueue) CloseTimeout(time.Duration) error { return nil }

func (q *prefetchQueue) ReserveN(
	ctx context.Context, n int, _ time.Duration,
) ([]taskq.Message, error) {
	atomic.AddInt32(&q.calls, 1)
	atomic.StoreInt32(&q.lastN, int32(n))
	if q.rtt > 0 {
		timer := time.NewTimer(q.rtt)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		}
	}

	msgs := make([]taskq.Message, n)
	for i := range msgs {
		msgs[i].TaskName = "prefetch-test"
	}
	return msgs, nil
}

func TestProcessOnePrefetchesBoundedBatch(t *testing.T) {
	tests := []struct {
		name            string
		reservationSize int
		bufferSize      int
		wantReserveN    int32
	}{
		{name: "default sized buffer", reservationSize: 5, bufferSize: 5, wantReserveN: 5},
		{name: "buffer limits batch", reservationSize: 10, bufferSize: 2, wantReserveN: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var processed int32
			opt := &taskq.QueueOptions{
				Name:            "prefetch-" + tt.name,
				ReservationSize: tt.reservationSize,
				BufferSize:      tt.bufferSize,
				Handler: taskq.HandlerFunc(func(*taskq.Message) error {
					atomic.AddInt32(&processed, 1)
					return nil
				}),
			}
			opt.Init()

			q := &prefetchQueue{opt: opt}
			c := taskq.NewConsumer(q)
			ctx := context.Background()

			if err := c.ProcessOne(ctx); err != nil {
				t.Fatal(err)
			}
			if got := atomic.LoadInt32(&q.lastN); got != tt.wantReserveN {
				t.Fatalf("ReserveN n=%d, wanted %d", got, tt.wantReserveN)
			}
			if got := c.Len(); got != int(tt.wantReserveN)-1 {
				t.Fatalf("buffered=%d, wanted %d", got, tt.wantReserveN-1)
			}

			for i := int(tt.wantReserveN) - 1; i > 0; i-- {
				if err := c.ProcessOne(ctx); err != nil {
					t.Fatal(err)
				}
			}
			if got := atomic.LoadInt32(&processed); got != tt.wantReserveN {
				t.Fatalf("processed=%d, wanted %d", got, tt.wantReserveN)
			}
			if got := atomic.LoadInt32(&q.calls); got != 1 {
				t.Fatalf("ReserveN calls=%d, wanted 1", got)
			}
		})
	}
}

func BenchmarkProcessOnePrefetch(b *testing.B) {
	opt := &taskq.QueueOptions{
		Name:            "benchmark-process-one-prefetch",
		ReservationSize: 50,
		BufferSize:      50,
		WaitTimeout:     time.Second,
		Handler: taskq.HandlerFunc(func(*taskq.Message) error {
			time.Sleep(3 * time.Millisecond)
			return nil
		}),
	}
	opt.Init()

	c := taskq.NewConsumer(&prefetchQueue{opt: opt, rtt: 20 * time.Millisecond})
	ctx := context.Background()

	b.ResetTimer()
	var wg sync.WaitGroup
	var errOnce sync.Once
	var firstErr error
	for worker := 0; worker < 5; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := worker; i < b.N; i += 5 {
				if err := c.ProcessOne(ctx); err != nil {
					errOnce.Do(func() { firstErr = err })
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	b.StopTimer()

	if firstErr != nil {
		b.Fatal(firstErr)
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msgs/s")
}

func BenchmarkConsumerPrefetch(b *testing.B) {
	var wg sync.WaitGroup
	var seen int64
	wg.Add(b.N)

	opt := &taskq.QueueOptions{
		Name:            "benchmark-prefetch",
		MinNumWorker:    5,
		MaxNumWorker:    5,
		ReservationSize: 50,
		BufferSize:      50,
		WaitTimeout:     time.Second,
		Handler: taskq.HandlerFunc(func(*taskq.Message) error {
			time.Sleep(3 * time.Millisecond)
			if atomic.AddInt64(&seen, 1) <= int64(b.N) {
				wg.Done()
			}
			return nil
		}),
	}
	opt.Init()

	c := taskq.NewConsumer(&prefetchQueue{opt: opt, rtt: 20 * time.Millisecond})
	if err := c.Start(context.Background()); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	wg.Wait()
	b.StopTimer()

	if err := c.StopTimeout(5 * time.Second); err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msgs/s")
}
