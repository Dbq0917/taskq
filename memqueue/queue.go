package memqueue

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/vmihailenco/taskq/v3"
	"github.com/vmihailenco/taskq/v3/internal"
	"github.com/vmihailenco/taskq/v3/internal/msgutil"
)

// persistInterval is how often delayed messages are flushed to disk when
// persistence is enabled, so that a single write covers the whole batch of
// waiting messages instead of writing once per message.
const persistInterval = time.Second

type persistedMessage struct {
	// Deadline is the moment the message becomes due, in Unix nanoseconds.
	Deadline int64
	Msg      []byte
}

type schedEntry struct {
	timer    *time.Timer
	deadline time.Time
}

type scheduler struct {
	path string

	timerLock sync.Mutex
	timerMap  map[*taskq.Message]*schedEntry
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
		q.timerMap = make(map[*taskq.Message]*schedEntry)
	}
	q.timerMap[msg] = &schedEntry{
		timer:    timer,
		deadline: time.Now().Add(msg.Delay),
	}
}

func (q *scheduler) Remove(msg *taskq.Message) {
	q.timerLock.Lock()
	defer q.timerLock.Unlock()

	entry, ok := q.timerMap[msg]
	if ok {
		entry.timer.Stop()
		delete(q.timerMap, msg)
	}
}

func (q *scheduler) Purge() int {
	q.timerLock.Lock()
	defer q.timerLock.Unlock()

	// Stop all delayed items
	for _, entry := range q.timerMap {
		entry.timer.Stop()
	}

	n := len(q.timerMap)
	q.timerMap = nil

	return n
}

// persist writes all pending delayed messages together with their deadlines
// to the persistence file in a single batched write.
func (q *scheduler) persist() {
	if q.path == "" {
		return
	}

	q.timerLock.Lock()
	defer q.timerLock.Unlock()

	entries := make([]persistedMessage, 0, len(q.timerMap))
	for msg, entry := range q.timerMap {
		b, err := msg.MarshalBinary()
		if err != nil {
			internal.Logger.Printf("taskq: persisting message failed: %s", err)
			continue
		}
		entries = append(entries, persistedMessage{
			Deadline: entry.deadline.UnixNano(),
			Msg:      b,
		})
	}

	if err := writePersistedMessages(q.path, entries); err != nil {
		internal.Logger.Printf("taskq: persisting delayed messages failed: %s", err)
	}
}

func writePersistedMessages(path string, entries []persistedMessage) error {
	b, err := msgpack.Marshal(entries)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func readPersistedMessages(path string) ([]persistedMessage, error) {
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var entries []persistedMessage
	if err := msgpack.Unmarshal(b, &entries); err != nil {
		return nil, err
	}
	return entries, nil
}

//------------------------------------------------------------------------------

const (
	stateRunning = 0
	stateClosing = 1
	stateClosed  = 2
)

type Queue struct {
	opt *taskq.QueueOptions

	sync    bool
	noDelay bool

	wg       sync.WaitGroup
	consumer *taskq.Consumer

	scheduler scheduler

	persistStop chan struct{}
	persistDone chan struct{}

	_state int32
}

var _ taskq.Queue = (*Queue)(nil)

func NewQueue(opt *taskq.QueueOptions) *Queue {
	return NewQueueMaybeConsumer(true, opt)
}

func NewQueueMaybeConsumer(startConsumer bool, opt *taskq.QueueOptions) *Queue {
	opt.Init()

	q := &Queue{
		opt: opt,
	}
	q.scheduler.path = opt.PersistencePath

	q.consumer = taskq.NewConsumer(q)
	if startConsumer {
		if err := q.consumer.Start(context.Background()); err != nil {
			panic(err)
		}
	}

	if opt.PersistencePath != "" {
		q.restore()
		q.persistStop = make(chan struct{})
		q.persistDone = make(chan struct{})
		go q.persistLoop()
	}

	return q
}

// persistLoop periodically flushes pending delayed messages to disk so that
// the backoff state survives a process restart.
func (q *Queue) persistLoop() {
	defer close(q.persistDone)

	ticker := time.NewTicker(persistInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			q.scheduler.persist()
		case <-q.persistStop:
			return
		}
	}
}

// restore re-enqueues delayed messages persisted by a previous run,
// keeping the remaining backoff instead of starting it over.
func (q *Queue) restore() {
	entries, err := readPersistedMessages(q.scheduler.path)
	if err != nil {
		internal.Logger.Printf("taskq: restoring delayed messages failed: %s", err)
		return
	}

	for i := range entries {
		msg := &taskq.Message{Ctx: context.Background()}
		if err := msg.UnmarshalBinary(entries[i].Msg); err != nil {
			internal.Logger.Printf("taskq: restoring delayed message failed: %s", err)
			continue
		}
		msg.Delay = time.Until(time.Unix(0, entries[i].Deadline))
		if msg.Delay < 0 {
			msg.Delay = 0
		}
		q.wg.Add(1)
		q.enqueueDelayed(msg)
	}
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

	if q.persistStop != nil {
		close(q.persistStop)
		<-q.persistDone
		// Flush the still pending delayed messages so that a new queue
		// using the same persistence file resumes their backoff.
		q.scheduler.persist()
	}

	_ = q.purge()

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
	return q.consumer.Len(), nil
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
	q.wg.Add(1)
	return q.enqueueMessage(msg)
}

func (q *Queue) enqueueMessage(msg *taskq.Message) error {
	if (q.noDelay || q.sync) && msg.Delay > 0 {
		msg.Delay = 0
	}
	msg.ReservedCount++

	if q.sync {
		return q.consumer.Process(msg)
	}
	return q.enqueueDelayed(msg)
}

// enqueueDelayed enqueues a message without counting it as a new
// reservation. It is used for messages restored from the persistence file
// whose reservation count was already persisted.
func (q *Queue) enqueueDelayed(msg *taskq.Message) error {
	if msg.Delay > 0 {
		q.scheduler.Schedule(msg, func() {
			// If the queue closed while we were waiting, just return
			if q.closed() {
				q.wg.Done()
				return
			}
			msg.Delay = 0
			_ = q.consumer.Add(msg)
		})
		return nil
	}
	return q.consumer.Add(msg)
}

func (q *Queue) ReserveN(_ context.Context, _ int, _ time.Duration) ([]taskq.Message, error) {
	return nil, internal.ErrNotSupported
}

func (q *Queue) Release(msg *taskq.Message) error {
	// Shallow copy.
	clone := *msg
	clone.Err = nil
	return q.enqueueMessage(&clone)
}

func (q *Queue) Delete(msg *taskq.Message) error {
	q.scheduler.Remove(msg)
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
	err := q.purge()
	// Explicitly purged messages must not be restored after a restart.
	q.scheduler.persist()
	return err
}

func (q *Queue) purge() error {
	// Purge any messages already in the consumer
	err := q.consumer.Purge()

	numPurged := q.scheduler.Purge()
	for i := 0; i < numPurged; i++ {
		q.wg.Done()
	}

	return err
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
