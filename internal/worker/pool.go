package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/azrilpramudia/go-job-queue/internal/job"
	"github.com/azrilpramudia/go-job-queue/internal/queue"
)

type Pool struct {
	workerCount int
	queue       *queue.Queue
	wg          sync.WaitGroup
	ctx         context.Context
	maxBackoff  time.Duration
	onUpdate    func(*job.Job) error
	logger      *slog.Logger
}

type PoolOptions struct {
	MaxBackoff time.Duration
	OnUpdate   func(*job.Job) error
	Logger     *slog.Logger
}

func NewPool(workerCount int, q *queue.Queue, ctx context.Context) *Pool {
	return NewPoolWithOptions(workerCount, q, ctx, PoolOptions{})
}

func NewPoolWithOptions(workerCount int, q *queue.Queue, ctx context.Context, options PoolOptions) *Pool {
	if options.MaxBackoff <= 0 {
		options.MaxBackoff = 30 * time.Second
	}
	if options.Logger == nil {
		options.Logger = slog.Default()
	}
	return &Pool{
		workerCount: workerCount,
		queue:       q,
		ctx:         ctx,
		maxBackoff:  options.MaxBackoff,
		onUpdate:    options.OnUpdate,
		logger:      options.Logger,
	}
}

func (p *Pool) Start() {
	for i := 0; i < p.workerCount; i++ {
		p.wg.Add(1)
		go p.worker(i)
	}
}

func backoff(base time.Duration, attempt int, maximum time.Duration) time.Duration {
	if base <= 0 || attempt < 0 {
		return 0
	}
	delay := base
	for i := 0; i < attempt && delay < maximum; i++ {
		if delay > maximum/2 {
			return maximum
		}
		delay *= 2
	}
	if delay > maximum {
		return maximum
	}
	return delay
}

func (p *Pool) worker(id int) {
	defer p.wg.Done()
	for {
		j, ok := p.queue.Pop()
		if !ok {
			return
		}
		p.logger.Info("running job", "worker", id, "job_id", j.ID)
		p.run(id, j)
	}
}

func (p *Pool) run(id int, j *job.Job) {
	j.MarkRunning()
	p.update(j)

	for {
		j.IncAttempts()
		p.update(j)

		ctx, cancel := context.WithTimeout(p.ctx, j.Timeout)
		err := j.Task(ctx)
		cancel()

		if err == nil {
			j.MarkDone()
			p.update(j)
			p.logger.Info("job completed", "worker", id, "job_id", j.ID, "attempts", j.Attempts())
			return
		}

		if errors.Is(err, context.Canceled) {
			j.MarkFailed(err)
			p.update(j)
			p.logger.Info("job canceled", "worker", id, "job_id", j.ID)
			return
		}

		if j.Attempts() > j.MaxRetries {
			j.MarkFailed(err)
			p.update(j)
			p.logger.Error("job permanently failed", "worker", id, "job_id", j.ID, "error", err)
			return
		}

		delay := backoff(j.BaseDelay, j.Attempts()-1, p.maxBackoff)
		p.logger.Warn("job failed; retrying", "worker", id, "job_id", j.ID, "error", err, "delay", delay)

		select {
		case <-time.After(delay):
		case <-p.ctx.Done():
			j.MarkFailed(p.ctx.Err())
			p.update(j)
			return
		}
	}
}

func (p *Pool) update(j *job.Job) {
	if p.onUpdate == nil {
		return
	}
	if err := p.onUpdate(j); err != nil {
		p.logger.Error("persist job state", "job_id", j.ID, "error", err)
	}
}

func (p *Pool) Wait() {
	p.wg.Wait()
}
