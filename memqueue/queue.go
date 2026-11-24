package memqueue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vmihailenco/taskq/v3"
	"github.com/vmihailenco/taskq/v3/internal"
	"github.com/vmihailenco/taskq/v3/internal/msgutil"
)

type scheduler struct {
	timerLock sync.Mutex
	timerMap  map[*taskq.Message]*time.Timer
}

func (q *scheduler) Schedule(msg *taskq.Message, fn func()) {
	q.timerLock.Lock()
	defer q.timerLock.Unlock()

	timer := time.AfterFunc(msg.Delay, func() {
		// Remove our entry from the map
		q.timerLock.Lock()
		delete(q.timerMap, msg)
		q.timerLock.Unlock()

		fn()
	})

	if q.timerMap == nil {
		q.timerMap = make(map[*taskq.Message]*time.Timer)
	}
	q.timerMap[msg] = timer
}

func (q *scheduler) Remove(msg *taskq.Message) {
	q.timerLock.Lock()
	defer q.timerLock.Unlock()

	timer, ok := q.timerMap[msg]
	if ok {
		timer.Stop()
		delete(q.timerMap, msg)
	}
}

func (q *scheduler) Purge() []*taskq.Message {
	q.timerLock.Lock()
	defer q.timerLock.Unlock()

	// Stop all delayed items
	msgs := make([]*taskq.Message, 0, len(q.timerMap))
	for _, timer := range q.timerMap {
		timer.Stop()
	}
	for msg := range q.timerMap {
		msgs = append(msgs, msg)
	}

	q.timerMap = nil

	return msgs
}

//------------------------------------------------------------------------------

const (
	stateRunning = 0
	stateClosing = 1
	stateClosed  = 2
)

type Queue struct {
	usedBytes int64
	maxBytes  int64

	opt *taskq.QueueOptions

	sync    bool
	noDelay bool

	pendingMu   sync.Mutex
	pending     []*taskq.Message
	pendingWake chan struct{}

	dispatchMu   sync.Mutex
	dispatchStop chan struct{}
	dispatchDone chan struct{}

	wg       sync.WaitGroup
	consumer *taskq.Consumer
	msgSizes sync.Map

	scheduler scheduler

	_state int32
}

var _ taskq.Queue = (*Queue)(nil)

func NewQueue(opt *taskq.QueueOptions) *Queue {
	return NewQueueMaybeConsumer(true, opt)
}

func NewQueueMaybeConsumer(startConsumer bool, opt *taskq.QueueOptions) *Queue {
	opt.Init()

	q := &Queue{
		opt:         opt,
		maxBytes:    opt.MaxSize,
		pendingWake: make(chan struct{}, 1),
	}

	q.consumer = taskq.NewConsumer(q)
	if startConsumer {
		if err := q.consumer.Start(context.Background()); err != nil {
			panic(err)
		}
	}

	return q
}

func (q *Queue) Name() string {
	return q.opt.Name
}

func (q *Queue) String() string {
	return fmt.Sprintf("queue=%q", q.Name())
}

func (q *Queue) Options() *taskq.QueueOptions {
	return q.opt
}

func (q *Queue) Consumer() taskq.QueueConsumer {
	return q.consumer
}

func (q *Queue) SetSync(sync bool) {
	q.sync = sync
}

func (q *Queue) SetNoDelay(noDelay bool) {
	q.noDelay = noDelay
}

// Close is like CloseTimeout with 30 seconds timeout.
func (q *Queue) Close() error {
	return q.CloseTimeout(30 * time.Second)
}

// CloseTimeout closes the queue waiting for pending messages to be processed.
func (q *Queue) CloseTimeout(timeout time.Duration) error {
	if !atomic.CompareAndSwapInt32(&q._state, stateRunning, stateClosing) {
		return fmt.Errorf("taskq: %s is already closed", q)
	}
	err := q.WaitTimeout(timeout)

	if !atomic.CompareAndSwapInt32(&q._state, stateClosing, stateClosed) {
		panic("not reached")
	}

	_ = q.consumer.StopTimeout(timeout)
	_ = q.Purge()

	return err
}

func (q *Queue) WaitTimeout(timeout time.Duration) error {
	done := make(chan struct{}, 1)
	go func() {
		q.wg.Wait()
		done <- struct{}{}
	}()

	select {
	case <-done:
	case <-time.After(timeout):
		return fmt.Errorf("taskq: %s: messages are not processed after %s", q.consumer, timeout)
	}

	return nil
}

func (q *Queue) Len() (int, error) {
	q.pendingMu.Lock()
	pending := len(q.pending)
	q.pendingMu.Unlock()
	return q.consumer.Len() + pending, nil
}

// Add adds message to the queue.
func (q *Queue) Add(msg *taskq.Message) error {
	if q.closed() {
		return fmt.Errorf("taskq: %s is closed", q)
	}
	if msg.TaskName == "" {
		return internal.ErrTaskNameRequired
	}
	if q.isDuplicate(msg) {
		msg.Err = taskq.ErrDuplicate
		return nil
	}

	size, err := q.reserveMessage(msg)
	if err != nil {
		return err
	}

	q.wg.Add(1)
	if err := q.enqueueMessage(msg); err != nil {
		if !q.sync {
			q.wg.Done()
			q.releaseMessage(msg, size)
		}
		return err
	}
	return nil
}

func (q *Queue) enqueueMessage(msg *taskq.Message) error {
	if (q.noDelay || q.sync) && msg.Delay > 0 {
		msg.Delay = 0
	}
	msg.ReservedCount++

	if q.sync {
		return q.consumer.Process(msg)
	}

	if msg.Delay > 0 {
		q.scheduler.Schedule(msg, func() {
			// If the queue closed while we were waiting, just return
			if q.closed() {
				_ = q.Delete(msg)
				return
			}
			msg.Delay = 0
			q.queueForConsumer(msg)
		})
		return nil
	}
	if q.maxBytes > 0 {
		q.queueForConsumer(msg)
		return nil
	}
	return q.consumer.Add(msg)
}

func (q *Queue) ReserveN(_ context.Context, _ int, _ time.Duration) ([]taskq.Message, error) {
	return nil, internal.ErrNotSupported
}

func (q *Queue) Release(msg *taskq.Message) error {
	size, _ := q.msgSizes.Load(msg)
	if size != nil {
		q.msgSizes.Delete(msg)
	}

	// Shallow copy.
	clone := *msg
	clone.Err = nil
	if size != nil {
		q.msgSizes.Store(&clone, size)
	}

	if err := q.enqueueMessage(&clone); err != nil {
		if !q.sync {
			if size != nil {
				q.msgSizes.Delete(&clone)
				q.releaseBytes(size.(int64))
			}
			q.wg.Done()
		}
		return err
	}
	return nil
}

func (q *Queue) Delete(msg *taskq.Message) error {
	q.scheduler.Remove(msg)
	if size, ok := q.msgSizes.LoadAndDelete(msg); ok {
		q.releaseBytes(size.(int64))
	}
	q.wg.Done()
	return nil
}

func (q *Queue) DeleteBatch(msgs []*taskq.Message) error {
	if len(msgs) == 0 {
		return errors.New("taskq: no messages to delete")
	}
	for _, msg := range msgs {
		if err := q.Delete(msg); err != nil {
			return err
		}
	}
	return nil
}

func (q *Queue) Purge() error {
	q.stopDispatcher()

	// Purge any messages already in the consumer
	err := q.consumer.Purge()

	q.pendingMu.Lock()
	pending := q.pending
	q.pending = nil
	q.pendingMu.Unlock()
	for _, msg := range pending {
		_ = q.Delete(msg)
	}

	purged := q.scheduler.Purge()
	for _, msg := range purged {
		_ = q.Delete(msg)
	}

	return err
}

// Bytes returns the total size of messages currently held by the queue.
func (q *Queue) Bytes() int64 {
	return atomic.LoadInt64(&q.usedBytes)
}

func (q *Queue) reserveBytes(size int64) bool {
	if q.maxBytes <= 0 {
		return true
	}
	if size > q.maxBytes {
		return false
	}

	for {
		used := atomic.LoadInt64(&q.usedBytes)
		if size > q.maxBytes-used {
			return false
		}
		if atomic.CompareAndSwapInt64(&q.usedBytes, used, used+size) {
			return true
		}
	}
}

func (q *Queue) reserveMessage(msg *taskq.Message) (int64, error) {
	if q.maxBytes <= 0 {
		return 0, nil
	}

	size, err := messageSize(msg)
	if err != nil {
		return 0, err
	}
	if !q.reserveBytes(size) {
		return 0, taskq.ErrQueueFull
	}
	q.msgSizes.Store(msg, size)
	return size, nil
}

func (q *Queue) releaseMessage(msg *taskq.Message, size int64) {
	if q.maxBytes > 0 {
		q.msgSizes.Delete(msg)
		q.releaseBytes(size)
	}
}

func (q *Queue) releaseBytes(size int64) {
	if size > 0 {
		atomic.AddInt64(&q.usedBytes, -size)
	}
}

func (q *Queue) queueForConsumer(msg *taskq.Message) {
	q.dispatchMu.Lock()
	defer q.dispatchMu.Unlock()

	q.pendingMu.Lock()
	if len(q.pending) == 0 && q.dispatchStop == nil {
		q.pendingMu.Unlock()
		if err := q.consumer.TryAdd(msg); err == nil {
			return
		}
		q.pendingMu.Lock()
	}
	q.pending = append(q.pending, msg)
	q.pendingMu.Unlock()

	if q.dispatchStop == nil {
		stop := make(chan struct{})
		done := make(chan struct{})
		q.dispatchStop = stop
		q.dispatchDone = done
		go q.dispatch(stop, done)
	}

	select {
	case q.pendingWake <- struct{}{}:
	default:
	}
}

func (q *Queue) stopDispatcher() {
	q.dispatchMu.Lock()
	stop := q.dispatchStop
	done := q.dispatchDone
	if stop == nil {
		q.dispatchMu.Unlock()
		return
	}
	q.dispatchStop = nil
	q.dispatchDone = nil
	close(stop)
	q.dispatchMu.Unlock()

	<-done
}

func (q *Queue) dispatch(stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)

	for {
		msg, ok := q.popPending()
		if !ok {
			select {
			case <-stop:
				return
			case <-q.pendingWake:
			}
			continue
		}

		for {
			if err := q.consumer.TryAdd(msg); err == nil {
				break
			}

			select {
			case <-stop:
				q.pushPending(msg)
				return
			case <-time.After(10 * time.Millisecond):
			}
		}
	}
}

func (q *Queue) popPending() (*taskq.Message, bool) {
	q.pendingMu.Lock()
	defer q.pendingMu.Unlock()

	if len(q.pending) == 0 {
		return nil, false
	}

	msg := q.pending[0]
	q.pending[0] = nil
	q.pending = q.pending[1:]
	return msg, true
}

func (q *Queue) pushPending(msg *taskq.Message) {
	q.pendingMu.Lock()
	q.pending = append([]*taskq.Message{msg}, q.pending...)
	q.pendingMu.Unlock()
}

func messageSize(msg *taskq.Message) (int64, error) {
	b, err := msg.MarshalArgs()
	if err != nil {
		return 0, err
	}
	return int64(len(b)), nil
}

func (q *Queue) closed() bool {
	return atomic.LoadInt32(&q._state) == stateClosed
}

func (q *Queue) isDuplicate(msg *taskq.Message) bool {
	if msg.Name == "" {
		return false
	}
	return q.opt.Storage.Exists(msg.Ctx, msgutil.FullMessageName(q, msg))
}
