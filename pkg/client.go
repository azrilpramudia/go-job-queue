package pkg

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/azrilpramudia/go-job-queue/internal/job"
	"github.com/azrilpramudia/go-job-queue/internal/queue"
	"github.com/azrilpramudia/go-job-queue/internal/worker"
)

type Task func(context.Context) error

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
	Timeout    time.Duration
	Priority   int
	MaxRetries int
	BaseDelay  time.Duration
}

type JobStatus struct {
	ID       string
	Status   Status
	Attempts int
	Error    string
}

type Client struct {
	queue       *queue.Queue
	pool        *worker.Pool
	lifecycleMu sync.RWMutex
	closed      bool
	jobsMu      sync.RWMutex
	jobs        map[string]*job.Job
}

func NewClient(workerCount, queueCapacity int) (*Client, error) {
	if workerCount <= 0 {
		return nil, errors.New("worker count must be greater than zero")
	}
	if queueCapacity < 0 {
		return nil, errors.New("queue capacity cannot be negative")
	}

	q := queue.NewQueue(queueCapacity)
	c := &Client{
		queue: q,
		pool:  worker.NewPool(workerCount, q, context.Background()),
		jobs:  make(map[string]*job.Job),
	}
	c.pool.Start()
	return c, nil
}

func (c *Client) Submit(spec JobSpec) error {
	if spec.ID == "" {
		return errors.New("job ID cannot be empty")
	}
	if spec.Task == nil {
		return errors.New("job task cannot be nil")
	}
	if spec.Timeout <= 0 {
		return errors.New("job timeout must be greater than zero")
	}

	j := job.NewJob(spec.ID, func(ctx context.Context) error {
		return spec.Task(ctx)
	}, spec.Timeout)
	j.Priority = spec.Priority
	j.MaxRetries = spec.MaxRetries
	j.BaseDelay = spec.BaseDelay

	c.lifecycleMu.RLock()
	defer c.lifecycleMu.RUnlock()
	if c.closed {
		return errors.New("client is closed")
	}

	c.jobsMu.Lock()
	if _, exists := c.jobs[spec.ID]; exists {
		c.jobsMu.Unlock()
		return fmt.Errorf("job %q already exists", spec.ID)
	}
	c.jobs[spec.ID] = j
	c.jobsMu.Unlock()

	c.queue.Push(j)
	return nil
}

func (c *Client) Status(id string) (JobStatus, bool) {
	c.jobsMu.RLock()
	j, exists := c.jobs[id]
	c.jobsMu.RUnlock()
	if !exists {
		return JobStatus{}, false
	}

	state, err, attempts := j.Snapshot()
	result := JobStatus{
		ID:       j.ID,
		Status:   Status(state),
		Attempts: attempts,
	}
	if err != nil {
		result.Error = err.Error()
	}
	return result, true
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
		state, err, attempts := j.Snapshot()
		status := JobStatus{
			ID:       j.ID,
			Status:   Status(state),
			Attempts: attempts,
		}
		if err != nil {
			status.Error = err.Error()
		}
		statuses = append(statuses, status)
	}
	return statuses
}

func (c *Client) Close() {
	c.lifecycleMu.Lock()
	if !c.closed {
		c.closed = true
		c.queue.Close()
	}
	c.lifecycleMu.Unlock()
	c.pool.Wait()
}
