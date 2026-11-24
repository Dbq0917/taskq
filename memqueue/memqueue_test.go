package memqueue_test

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	uuid "github.com/satori/go.uuid"

	"github.com/vmihailenco/taskq/v3"
	"github.com/vmihailenco/taskq/v3/memqueue"
)

func TestMemqueue(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "memqueue")
}

var _ = BeforeSuite(func() {
	taskq.SetLogger(log.New(ioutil.Discard, "", 0))
})

var _ = BeforeEach(func() {
	taskq.Tasks.Reset()
})

var _ = Describe("message with args", func() {
	ctx := context.Background()
	ch := make(chan bool, 10)

	BeforeEach(func() {
		q := memqueue.NewQueue(&taskq.QueueOptions{
			Name:    "test",
			Storage: taskq.NewLocalStorage(),
		})
		task := taskq.RegisterTask(&taskq.TaskOptions{
			Name: "test",
			Handler: func(s string, i int) {
				Expect(s).To(Equal("string"))
				Expect(i).To(Equal(42))
				ch <- true
			},
		})
		err := q.Add(task.WithArgs(ctx, "string", 42))
		Expect(err).NotTo(HaveOccurred())

		err = q.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("handler is called with args", func() {
		Expect(ch).To(Receive())
		Expect(ch).NotTo(Receive())
	})
})

var _ = Describe("context.Context", func() {
	ctx := context.Background()
	ch := make(chan bool, 10)

	BeforeEach(func() {
		q := memqueue.NewQueue(&taskq.QueueOptions{
			Name:    "test",
			Storage: taskq.NewLocalStorage(),
		})
		task := taskq.RegisterTask(&taskq.TaskOptions{
			Name: "test",
			Handler: func(c context.Context, s string, i int) {
				Expect(s).To(Equal("string"))
				Expect(i).To(Equal(42))
				ch <- true
			},
		})
		err := q.Add(task.WithArgs(ctx, "string", 42))
		Expect(err).NotTo(HaveOccurred())

		err = q.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("handler is called with args", func() {
		Expect(ch).To(Receive())
		Expect(ch).NotTo(Receive())
	})
})

var _ = Describe("message with invalid number of args", func() {
	ctx := context.Background()
	ch := make(chan bool, 10)

	BeforeEach(func() {
		q := memqueue.NewQueue(&taskq.QueueOptions{
			Name:    "test",
			Storage: taskq.NewLocalStorage(),
		})
		task := taskq.RegisterTask(&taskq.TaskOptions{
			Name: "test",
			Handler: func(s string) {
				ch <- true
			},
			RetryLimit: 1,
		})
		q.Consumer().Stop()

		err := q.Add(task.WithArgs(ctx))
		Expect(err).NotTo(HaveOccurred())

		err = q.Consumer().ProcessOne(ctx)
		Expect(err).To(MatchError("taskq: got 0 args, wanted 1"))

		err = q.Consumer().ProcessAll(ctx)
		Expect(err).NotTo(HaveOccurred())

		err = q.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("handler is not called", func() {
		Expect(ch).NotTo(Receive())
	})
})

var _ = Describe("HandlerFunc", func() {
	ctx := context.Background()
	ch := make(chan bool, 10)

	BeforeEach(func() {
		q := memqueue.NewQueue(&taskq.QueueOptions{
			Name:    "test",
			Storage: taskq.NewLocalStorage(),
		})
		task := taskq.RegisterTask(&taskq.TaskOptions{
			Name: "test",
			Handler: func(msg *taskq.Message) error {
				Expect(msg.Args).To(Equal([]interface{}{"string", 42}))
				ch <- true
				return nil
			},
		})

		err := q.Add(task.WithArgs(ctx, "string", 42))
		Expect(err).NotTo(HaveOccurred())

		err = q.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("is called with Message", func() {
		Expect(ch).To(Receive())
		Expect(ch).NotTo(Receive())
	})
})

var _ = Describe("message retry timing", func() {
	ctx := context.Background()
	var q *memqueue.Queue
	var task *taskq.Task
	backoff := 100 * time.Millisecond
	var count int
	var ch chan time.Time

	BeforeEach(func() {
		count = 0
		ch = make(chan time.Time, 10)
		q = memqueue.NewQueue(&taskq.QueueOptions{
			Name:    "test",
			Storage: taskq.NewLocalStorage(),
		})
		task = taskq.RegisterTask(&taskq.TaskOptions{
			Name: "test",
			Handler: func() error {
				ch <- time.Now()
				count++
				return fmt.Errorf("fake error #%d", count)
			},
			RetryLimit: 3,
			MinBackoff: backoff,
		})
	})

	Context("without delay", func() {
		var now time.Time

		BeforeEach(func() {
			now = time.Now()
			_ = q.Add(task.WithArgs(ctx))

			err := q.Close()
			Expect(err).NotTo(HaveOccurred())
		})

		It("is retried in time", func() {
			Expect(ch).To(Receive(BeTemporally("~", now, backoff/10)))
			Expect(ch).To(Receive(BeTemporally("~", now.Add(backoff), backoff/10)))
			Expect(ch).To(Receive(BeTemporally("~", now.Add(3*backoff), backoff/10)))
			Expect(ch).NotTo(Receive())
		})
	})

	Context("message with delay", func() {
		var now time.Time

		BeforeEach(func() {
			msg := task.WithArgs(ctx)
			msg.Delay = 5 * backoff
			now = time.Now().Add(msg.Delay)

			q.Add(msg)

			err := q.Close()
			Expect(err).NotTo(HaveOccurred())
		})

		It("is retried in time", func() {
			Expect(ch).To(Receive(BeTemporally("~", now, backoff/10)))
			Expect(ch).To(Receive(BeTemporally("~", now.Add(backoff), backoff/10)))
			Expect(ch).To(Receive(BeTemporally("~", now.Add(3*backoff), backoff/10)))
			Expect(ch).NotTo(Receive())
		})
	})
})

var _ = Describe("failing queue with error handler", func() {
	ctx := context.Background()
	var q *memqueue.Queue
	ch := make(chan bool, 10)

	BeforeEach(func() {
		q = memqueue.NewQueue(&taskq.QueueOptions{
			Name:    "test",
			Storage: taskq.NewLocalStorage(),
		})
		task := taskq.RegisterTask(&taskq.TaskOptions{
			Name: "test",
			Handler: func() error {
				return errors.New("fake error")
			},
			FallbackHandler: func() {
				ch <- true
			},
			RetryLimit: 1,
		})
		q.Add(task.WithArgs(ctx))

		err := q.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("error handler is called when handler fails", func() {
		Expect(ch).To(Receive())
		Expect(ch).NotTo(Receive())
	})
})

var _ = Describe("named message", func() {
	ctx := context.Background()
	var count int64

	BeforeEach(func() {
		q := memqueue.NewQueue(&taskq.QueueOptions{
			Name:    "test",
			Storage: taskq.NewLocalStorage(),
		})
		task := taskq.RegisterTask(&taskq.TaskOptions{
			Name: "test",
			Handler: func() {
				atomic.AddInt64(&count, 1)
			},
		})

		name := uuid.NewV4().String()

		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				msg := task.WithArgs(ctx)
				msg.Name = name
				q.Add(msg)
			}()
		}
		wg.Wait()

		err := q.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("is processed once", func() {
		n := atomic.LoadInt64(&count)
		Expect(n).To(Equal(int64(1)))
	})
})

var _ = Describe("CallOnce", func() {
	ctx := context.Background()
	var now time.Time
	delay := 3 * time.Second
	ch := make(chan time.Time, 10)

	BeforeEach(func() {
		now = time.Now()

		q := memqueue.NewQueue(&taskq.QueueOptions{
			Name:    "test",
			Storage: taskq.NewLocalStorage(),
		})
		task := taskq.RegisterTask(&taskq.TaskOptions{
			Name: "test",
			Handler: func() error {
				ch <- time.Now()
				return nil
			},
		})

		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()

				msg := task.WithArgs(ctx)
				msg.OnceInPeriod(delay)

				q.Add(msg)
			}()
		}
		wg.Wait()

		err := q.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("processes message once with delay", func() {
		Eventually(ch).Should(Receive(BeTemporally(">", now.Add(delay), time.Second)))
		Consistently(ch).ShouldNot(Receive())
	})
})

var _ = Describe("stress testing", func() {
	const n = 10000
	ctx := context.Background()
	var count int64

	BeforeEach(func() {
		q := memqueue.NewQueue(&taskq.QueueOptions{
			Name:    "test",
			Storage: taskq.NewLocalStorage(),
		})
		task := taskq.RegisterTask(&taskq.TaskOptions{
			Name: "test",
			Handler: func() {
				atomic.AddInt64(&count, 1)
			},
		})

		for i := 0; i < n; i++ {
			q.Add(task.WithArgs(ctx))
		}

		err := q.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("handler is called for all messages", func() {
		nn := atomic.LoadInt64(&count)
		Expect(nn).To(Equal(int64(n)))
	})
})

var _ = Describe("stress testing failing queue", func() {
	const n = 100000
	ctx := context.Background()
	var errorCount int64

	BeforeEach(func() {
		q := memqueue.NewQueue(&taskq.QueueOptions{
			Name:                 "test",
			PauseErrorsThreshold: -1,
			Storage:              taskq.NewLocalStorage(),
		})
		task := taskq.RegisterTask(&taskq.TaskOptions{
			Name: "test",
			Handler: func() error {
				return errors.New("fake error")
			},
			FallbackHandler: func() {
				atomic.AddInt64(&errorCount, 1)
			},
			RetryLimit: 1,
		})

		for i := 0; i < n; i++ {
			q.Add(task.WithArgs(ctx))
		}

		err := q.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("error handler is called for all messages", func() {
		nn := atomic.LoadInt64(&errorCount)
		Expect(nn).To(Equal(int64(n)))
	})
})

var _ = Describe("byte capacity", func() {
	ctx := context.Background()
	var q *memqueue.Queue
	var task *taskq.Task

	BeforeEach(func() {
		q = memqueue.NewQueue(&taskq.QueueOptions{
			Name:    "test",
			MaxSize: 32 * 1024,
			Storage: taskq.NewLocalStorage(),
		})
		task = taskq.RegisterTask(&taskq.TaskOptions{
			Name:    "test",
			Handler: func([]byte) {},
		})
		Expect(q.Consumer().Stop()).To(Succeed())
	})

	AfterEach(func() {
		_ = q.Purge()
		Expect(q.Close()).To(Succeed())
	})

	message := func(size int) *taskq.Message {
		return task.WithArgs(ctx, make([]byte, size))
	}

	messageSize := func(msg *taskq.Message) int64 {
		b, err := msg.MarshalArgs()
		Expect(err).NotTo(HaveOccurred())
		return int64(len(b))
	}

	It("accounts mixed message sizes by bytes", func() {
		var added int
		for i := 0; ; i++ {
			size := 128
			if i%3 == 0 {
				size = 8 * 1024
			}

			err := q.Add(message(size))
			if errors.Is(err, taskq.ErrQueueFull) {
				break
			}
			Expect(err).NotTo(HaveOccurred())
			added++
		}

		Expect(added).To(BeNumerically(">", 0))
		Expect(q.Bytes()).To(BeNumerically("<=", q.Options().MaxSize))

		err := q.Add(message(8 * 1024))
		Expect(err).To(MatchError(taskq.ErrQueueFull))
		Expect(q.Bytes()).To(BeNumerically("<=", q.Options().MaxSize))
	})

	It("allows the exact remaining capacity and frees it on purge", func() {
		small := message(128)
		large := message(8 * 1024)
		smallSize := messageSize(small)
		largeSize := messageSize(large)

		maxSize := smallSize + largeSize + smallSize
		Expect(q.Close()).To(Succeed())
		q = memqueue.NewQueue(&taskq.QueueOptions{
			Name:    "test",
			MaxSize: maxSize,
			Storage: taskq.NewLocalStorage(),
		})
		Expect(q.Consumer().Stop()).To(Succeed())

		Expect(q.Add(small)).To(Succeed())
		Expect(q.Add(large)).To(Succeed())
		Expect(q.Add(message(128))).To(Succeed())
		Expect(q.Bytes()).To(Equal(maxSize))

		Expect(q.Add(message(128))).To(MatchError(taskq.ErrQueueFull))
		Expect(q.Bytes()).To(Equal(maxSize))

		Expect(q.Purge()).To(Succeed())
		Expect(q.Bytes()).To(BeZero())
		Expect(q.Add(message(128))).To(Succeed())
	})

	It("does not oversubscribe capacity when enqueueing concurrently", func() {
		const goroutines = 32

		var wg sync.WaitGroup
		wg.Add(goroutines)
		for i := 0; i < goroutines; i++ {
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				_ = q.Add(message(4 * 1024))
			}()
		}
		wg.Wait()

		Expect(q.Bytes()).To(BeNumerically("<=", q.Options().MaxSize))
		Expect(q.Bytes()).To(BeNumerically(">", 0))
	})

	It("releases capacity after messages are processed", func() {
		for i := 0; ; i++ {
			size := 128
			if i%3 == 0 {
				size = 8 * 1024
			}

			err := q.Add(message(size))
			if errors.Is(err, taskq.ErrQueueFull) {
				break
			}
			Expect(err).NotTo(HaveOccurred())
		}
		Expect(q.Bytes()).To(BeNumerically(">", 0))

		Expect(q.Consumer().ProcessAll(ctx)).To(Succeed())
		Expect(q.Bytes()).To(BeZero())

		Expect(q.Add(message(8 * 1024))).To(Succeed())
	})

	It("keeps the reservation while a message is retried", func() {
		retryTask := taskq.RegisterTask(&taskq.TaskOptions{
			Name: "retry",
			Handler: func([]byte) error {
				return errors.New("retry")
			},
			RetryLimit: 2,
			MinBackoff: time.Millisecond,
		})

		msg := retryTask.WithArgs(ctx, make([]byte, 128))
		maxSize := messageSize(msg)

		Expect(q.Close()).To(Succeed())
		q = memqueue.NewQueue(&taskq.QueueOptions{
			Name:    "test",
			MaxSize: maxSize,
			Storage: taskq.NewLocalStorage(),
		})
		Expect(q.Consumer().Stop()).To(Succeed())

		Expect(q.Add(msg)).To(Succeed())
		Expect(q.Bytes()).To(Equal(maxSize))
		Expect(q.Add(message(128))).To(MatchError(taskq.ErrQueueFull))

		Expect(q.Consumer().ProcessAll(ctx)).To(Succeed())
		Expect(q.Bytes()).To(BeZero())
	})
})

var _ = Describe("empty queue", func() {
	ctx := context.Background()
	var q *memqueue.Queue
	var task *taskq.Task
	var processed uint32

	BeforeEach(func() {
		processed = 0
		q = memqueue.NewQueue(&taskq.QueueOptions{
			Name:    "test",
			Storage: taskq.NewLocalStorage(),
		})
		task = taskq.RegisterTask(&taskq.TaskOptions{
			Name: "test",
			Handler: func() {
				atomic.AddUint32(&processed, 1)
			},
		})
	})

	AfterEach(func() {
		_ = q.Close()
	})

	It("can be closed", func() {
		err := q.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("stops processor", func() {
		err := q.Consumer().Stop()
		Expect(err).NotTo(HaveOccurred())
	})

	testEmptyQueue := func() {
		It("processes all messages", func() {
			err := q.Consumer().ProcessAll(ctx)
			Expect(err).NotTo(HaveOccurred())
		})

		It("processes one message", func() {
			err := q.Consumer().ProcessOne(ctx)
			Expect(err).To(MatchError("taskq: queue is empty"))

			err = q.Consumer().ProcessAll(ctx)
			Expect(err).NotTo(HaveOccurred())
		})
	}

	Context("when processor is stopped", func() {
		BeforeEach(func() {
			err := q.Consumer().Stop()
			Expect(err).NotTo(HaveOccurred())
		})

		testEmptyQueue()

		Context("when there are messages in the queue", func() {
			BeforeEach(func() {
				for i := 0; i < 3; i++ {
					err := q.Add(task.WithArgs(ctx))
					Expect(err).NotTo(HaveOccurred())
				}
			})

			It("processes all messages", func() {
				p := q.Consumer()

				err := p.ProcessAll(ctx)
				Expect(err).NotTo(HaveOccurred())

				n := atomic.LoadUint32(&processed)
				Expect(n).To(Equal(uint32(3)))
			})

			It("processes one message", func() {
				p := q.Consumer()

				err := p.ProcessOne(ctx)
				Expect(err).NotTo(HaveOccurred())

				n := atomic.LoadUint32(&processed)
				Expect(n).To(Equal(uint32(1)))

				err = p.ProcessAll(ctx)
				Expect(err).NotTo(HaveOccurred())

				n = atomic.LoadUint32(&processed)
				Expect(n).To(Equal(uint32(3)))
			})
		})
	})
})

// slot splits time into equal periods (called slots) and returns
// slot number for provided time.
func slot(period time.Duration) int64 {
	tm := time.Now()
	periodSec := int64(period / time.Second)
	if periodSec == 0 {
		return tm.Unix()
	}
	return tm.Unix() / periodSec
}
