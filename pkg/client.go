package pkg

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/azrilpramudia/go-job-queue/internal/job"
	"github.com/azrilpramudia/go-job-queue/internal/queue"
	"github.com/azrilpramudia/go-job-queue/internal/store"
	"github.com/azrilpramudia/go-job-queue/internal/worker"
)

var (
	ErrInvalidJobSpec = errors.New("invalid job specification")
	ErrDuplicateJobID = errors.New("job ID already exists")
	ErrClientClosed   = errors.New("client is closed")
)

type Task func(context.Context) error

type TaskResolver func(name string, payload []byte) (Task, bool)

type Status string

const (
	StatusPending Status = "pending"
	StatusRunning Status = "running"
	StatusDone    Status = "done"
	StatusFailed  Status = "failed"
)

type JobSpec struct {
	ID         string
	Task       Task
	TaskName   string
	Payload    []byte
	Timeout    time.Duration
	Priority   int
	MaxRetries int
	BaseDelay  time.Duration
}

type JobStatus struct {
	ID       string `json:"id"`
	Status   Status `json:"status"`
	Attempts int    `json:"attempts"`
	Error    string `json:"error,omitempty"`
}

type ClientOptions struct {
	WorkerCount     int
	QueueCapacity   int
	SQLitePath      string
	MaxBackoff      time.Duration
	RetentionPeriod time.Duration
	MaxTerminalJobs int
	ResolveTask     TaskResolver
	Logger          *slog.Logger
}

type Metrics struct {
	Accepted  int64
	Completed int64
	Failed    int64
	Retried   int64
	Pending   int64
	Running   int64
}

type Client struct {
	queue       *queue.Queue
	pool        *worker.Pool
	ctx         context.Context
	cancel      context.CancelFunc
	store       *store.SQLite
	options     ClientOptions
	logger      *slog.Logger
	lifecycleMu sync.RWMutex
	closed      bool
	submitWG    sync.WaitGroup
	shutdown    sync.Once
	jobsMu      sync.RWMutex
	jobs        map[string]*job.Job
	done        chan struct{}
	closeErr    error
	janitorStop chan struct{}
	janitorWG   sync.WaitGroup
	accepted    atomic.Int64
	completed   atomic.Int64
	failed      atomic.Int64
	retried     atomic.Int64
}

func NewClient(workerCount, queueCapacity int) (*Client, error) {
	return NewClientWithOptions(ClientOptions{
		WorkerCount:   workerCount,
		QueueCapacity: queueCapacity,
	})
}

func NewClientWithOptions(options ClientOptions) (*Client, error) {
	if options.WorkerCount <= 0 {
		return nil, errors.New("worker count must be greater than zero")
	}
	if options.QueueCapacity < 0 {
		return nil, errors.New("queue capacity cannot be negative")
	}
	if options.MaxBackoff < 0 {
		return nil, errors.New("maximum backoff cannot be negative")
	}
	if options.RetentionPeriod < 0 {
		return nil, errors.New("retention period cannot be negative")
	}
	if options.MaxTerminalJobs < 0 {
		return nil, errors.New("maximum terminal jobs cannot be negative")
	}
	if options.MaxBackoff == 0 {
		options.MaxBackoff = 30 * time.Second
	}
	if options.RetentionPeriod == 0 {
		options.RetentionPeriod = 7 * 24 * time.Hour
	}
	if options.MaxTerminalJobs == 0 {
		options.MaxTerminalJobs = 10_000
	}
	if options.SQLitePath != "" && options.ResolveTask == nil {
		return nil, errors.New("task resolver is required when SQLite persistence is enabled")
	}
	if options.Logger == nil {
		options.Logger = slog.Default()
	}

	var persistentStore *store.SQLite
	var err error
	if options.SQLitePath != "" {
		persistentStore, err = store.OpenSQLite(options.SQLitePath)
		if err != nil {
			return nil, err
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	q := queue.NewQueue(options.QueueCapacity)
	c := &Client{
		queue:       q,
		ctx:         ctx,
		cancel:      cancel,
		store:       persistentStore,
		options:     options,
		logger:      options.Logger,
		jobs:        make(map[string]*job.Job),
		done:        make(chan struct{}),
		janitorStop: make(chan struct{}),
	}
	c.pool = worker.NewPoolWithOptions(options.WorkerCount, q, ctx, worker.PoolOptions{
		MaxBackoff: options.MaxBackoff,
		Logger:     options.Logger,
		OnUpdate:   c.persistUpdate,
	})

	if persistentStore != nil {
		if err := c.restoreJobs(); err != nil {
			q.Close()
			cancel()
			if closeErr := persistentStore.Close(); closeErr != nil {
				options.Logger.Error("close SQLite database after client initialization failure", "error", closeErr)
			}
			return nil, err
		}
	} else {
		c.pool.Start()
	}
	c.janitorWG.Add(1)
	go c.runRetentionJanitor()

	return c, nil
}

func (c *Client) restoreJobs() error {
	ctx := context.Background()
	if _, err := c.store.Prune(ctx, time.Now().Add(-c.options.RetentionPeriod), c.options.MaxTerminalJobs); err != nil {
		return err
	}

	records, err := c.store.Recover(ctx)
	if err != nil {
		return err
	}
	var pending []*job.Job
	for _, record := range records {
		if record.Status != string(job.StatusPending) && record.Status != string(job.StatusRunning) &&
			record.Status != string(job.StatusDone) && record.Status != string(job.StatusFailed) {
			return fmt.Errorf("restore job %q: invalid persisted status %q", record.ID, record.Status)
		}
		var task Task
		if record.Status == string(job.StatusPending) {
			var ok bool
			task, ok = c.options.ResolveTask(record.TaskName, append([]byte(nil), record.Payload...))
			if !ok || task == nil {
				return fmt.Errorf("restore job %q: task %q is not registered", record.ID, record.TaskName)
			}
		} else {
			task = func(context.Context) error { return nil }
		}
		j := job.NewJob(record.ID, func(ctx context.Context) error {
			return task(ctx)
		}, record.Timeout)
		j.TaskName = record.TaskName
		j.Payload = append([]byte(nil), record.Payload...)
		j.Priority = record.Priority
		j.MaxRetries = record.MaxRetries
		j.BaseDelay = record.BaseDelay
		state := job.Status(record.Status)
		var taskErr error
		if record.Error != "" {
			taskErr = errors.New(record.Error)
		}
		j.Restore(state, taskErr, record.Attempts)
		if state == job.StatusDone || state == job.StatusFailed {
			j.ReleaseTaskData()
		}
		c.jobs[record.ID] = j
		if state == job.StatusPending {
			pending = append(pending, j)
		}
	}
	c.pool.Start()
	for _, j := range pending {
		c.queue.Push(j)
	}
	return nil
}

func (c *Client) Submit(spec JobSpec) error {
	return c.SubmitContext(context.Background(), spec)
}

func (c *Client) SubmitContext(ctx context.Context, spec JobSpec) error {
	if ctx == nil {
		return errors.New("context cannot be nil")
	}
	if spec.ID == "" {
		return fmt.Errorf("%w: job ID cannot be empty", ErrInvalidJobSpec)
	}
	if spec.Task == nil {
		return fmt.Errorf("%w: job task cannot be nil", ErrInvalidJobSpec)
	}
	if spec.Timeout <= 0 {
		return fmt.Errorf("%w: job timeout must be greater than zero", ErrInvalidJobSpec)
	}
	if spec.MaxRetries < 0 {
		return fmt.Errorf("%w: max retries cannot be negative", ErrInvalidJobSpec)
	}
	if spec.BaseDelay < 0 {
		return fmt.Errorf("%w: base delay cannot be negative", ErrInvalidJobSpec)
	}
	if c.store != nil {
		if spec.TaskName == "" || c.options.ResolveTask == nil {
			return fmt.Errorf("%w: persistent jobs need a registered task name", ErrInvalidJobSpec)
		}
		if spec.Payload == nil {
			spec.Payload = []byte("null")
		}
		task, ok := c.options.ResolveTask(spec.TaskName, append([]byte(nil), spec.Payload...))
		if !ok || task == nil {
			return fmt.Errorf("%w: task %q is not registered", ErrInvalidJobSpec, spec.TaskName)
		}
		spec.Task = task
	}

	j := job.NewJob(spec.ID, func(ctx context.Context) error {
		return spec.Task(ctx)
	}, spec.Timeout)
	j.TaskName = spec.TaskName
	j.Payload = append([]byte(nil), spec.Payload...)
	j.Priority = spec.Priority
	j.MaxRetries = spec.MaxRetries
	j.BaseDelay = spec.BaseDelay

	c.lifecycleMu.RLock()
	if c.closed {
		c.lifecycleMu.RUnlock()
		return ErrClientClosed
	}
	c.submitWG.Add(1)
	c.lifecycleMu.RUnlock()
	defer c.submitWG.Done()
	operationCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(c.ctx, cancel)
	defer stop()
	defer cancel()

	c.jobsMu.Lock()
	if _, exists := c.jobs[spec.ID]; exists {
		c.jobsMu.Unlock()
		return fmt.Errorf("%w: %q", ErrDuplicateJobID, spec.ID)
	}
	if c.store != nil {
		if err := c.store.Insert(operationCtx, recordFromJob(j)); err != nil {
			c.jobsMu.Unlock()
			return err
		}
	}
	c.jobs[spec.ID] = j
	c.jobsMu.Unlock()

	err := c.queue.PushContext(operationCtx, j)
	if err != nil {
		c.jobsMu.Lock()
		delete(c.jobs, spec.ID)
		c.jobsMu.Unlock()
		if c.store != nil {
			if deleteErr := c.store.Delete(context.Background(), spec.ID); deleteErr != nil {
				return fmt.Errorf("enqueue job failed (%v), and persistence cleanup failed: %w", err, deleteErr)
			}
		}
		return err
	}
	c.accepted.Add(1)
	return nil
}

func recordFromJob(j *job.Job) store.Record {
	state, err, attempts := j.Snapshot()
	record := store.Record{
		ID:         j.ID,
		TaskName:   j.TaskName,
		Payload:    append([]byte(nil), j.Payload...),
		Timeout:    j.Timeout,
		Priority:   j.Priority,
		MaxRetries: j.MaxRetries,
		BaseDelay:  j.BaseDelay,
		Status:     string(state),
		Attempts:   attempts,
	}
	if err != nil {
		record.Error = err.Error()
	}
	return record
}

func (c *Client) persistUpdate(j *job.Job) error {
	record := recordFromJob(j)
	if record.Attempts > 1 && record.Status == string(job.StatusRunning) {
		c.retried.Add(1)
	}
	if state, _, _ := j.Snapshot(); state == job.StatusDone || state == job.StatusFailed {
		c.muUpdateMetrics(state)
	} else if c.store == nil {
		return nil
	}
	if c.store != nil {
		if err := c.store.Update(context.Background(), record); err != nil {
			if record.Status == string(job.StatusDone) || record.Status == string(job.StatusFailed) {
				j.ReleaseTaskData()
			}
			return err
		}
	}
	if record.Status == string(job.StatusDone) || record.Status == string(job.StatusFailed) {
		j.ReleaseTaskData()
		return c.pruneStatuses()
	}
	return nil
}

func (c *Client) pruneStatuses() error {
	c.pruneMemoryStatuses()
	if c.store == nil {
		return nil
	}
	removed, err := c.store.Prune(
		context.Background(),
		time.Now().Add(-c.options.RetentionPeriod),
		c.options.MaxTerminalJobs,
	)
	if err != nil {
		return err
	}
	c.jobsMu.Lock()
	for _, id := range removed {
		delete(c.jobs, id)
	}
	c.jobsMu.Unlock()
	return nil
}

func (c *Client) pruneMemoryStatuses() {
	c.jobsMu.Lock()
	defer c.jobsMu.Unlock()

	type terminalJob struct {
		id       string
		finished time.Time
	}
	var terminal []terminalJob
	for id, j := range c.jobs {
		state, _, _ := j.Snapshot()
		if state == job.StatusDone || state == job.StatusFailed {
			terminal = append(terminal, terminalJob{id: id, finished: j.FinishedAt()})
		}
	}

	sort.Slice(terminal, func(i, j int) bool {
		return terminal[i].finished.After(terminal[j].finished)
	})
	cutoff := time.Now().Add(-c.options.RetentionPeriod)
	for index, completed := range terminal {
		if completed.finished.Before(cutoff) || index >= c.options.MaxTerminalJobs {
			delete(c.jobs, completed.id)
		}
	}
}

func (c *Client) runRetentionJanitor() {
	defer c.janitorWG.Done()
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := c.pruneStatuses(); err != nil {
				c.logger.Error("prune expired job statuses", "error", err)
			}
		case <-c.janitorStop:
			return
		}
	}
}

func (c *Client) muUpdateMetrics(state job.Status) {
	if state == job.StatusDone {
		c.completed.Add(1)
	} else if state == job.StatusFailed {
		c.failed.Add(1)
	}
}

func (c *Client) IsClosed() bool {
	c.lifecycleMu.RLock()
	defer c.lifecycleMu.RUnlock()
	return c.closed
}

func (c *Client) PersistenceEnabled() bool {
	return c.store != nil
}

func (c *Client) Status(id string) (JobStatus, bool) {
	c.jobsMu.RLock()
	j, exists := c.jobs[id]
	c.jobsMu.RUnlock()
	if !exists {
		return JobStatus{}, false
	}
	return statusFromJob(j), true
}

func statusFromJob(j *job.Job) JobStatus {
	state, err, attempts := j.Snapshot()
	result := JobStatus{
		ID:       j.ID,
		Status:   Status(state),
		Attempts: attempts,
	}
	if err != nil {
		result.Error = err.Error()
	}
	return result
}

func (c *Client) ListStatuses() []JobStatus {
	c.jobsMu.RLock()
	jobs := make([]*job.Job, 0, len(c.jobs))
	for _, j := range c.jobs {
		jobs = append(jobs, j)
	}
	c.jobsMu.RUnlock()

	statuses := make([]JobStatus, 0, len(jobs))
	for _, j := range jobs {
		statuses = append(statuses, statusFromJob(j))
	}
	return statuses
}

func (c *Client) Metrics() Metrics {
	var result Metrics
	result.Accepted = c.accepted.Load()
	result.Completed = c.completed.Load()
	result.Failed = c.failed.Load()
	result.Retried = c.retried.Load()
	c.jobsMu.RLock()
	defer c.jobsMu.RUnlock()
	for _, j := range c.jobs {
		switch j.Status() {
		case job.StatusPending:
			result.Pending++
		case job.StatusRunning:
			result.Running++
		}
	}
	return result
}

func (c *Client) Close() {
	if err := c.CloseWithContext(context.Background()); err != nil {
		c.logger.Error("close job client", "error", err)
	}
}

func (c *Client) CloseWithContext(ctx context.Context) error {
	if ctx == nil {
		return errors.New("context cannot be nil")
	}
	c.lifecycleMu.Lock()
	if !c.closed {
		c.closed = true
	}
	c.lifecycleMu.Unlock()
	c.shutdown.Do(func() {
		go func() {
			close(c.janitorStop)
			c.janitorWG.Wait()
			c.submitWG.Wait()
			c.queue.Close()
			c.pool.Wait()
			if c.store != nil {
				c.closeErr = c.store.Close()
			}
			close(c.done)
		}()
	})

	select {
	case <-c.done:
		return c.closeErr
	case <-ctx.Done():
		c.cancel()
		return ctx.Err()
	}
}
