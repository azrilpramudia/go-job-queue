package worker

import (
	"context"
	"errors"
	"fmt"
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
}

func NewPool(workerCount int, q *queue.Queue, ctx context.Context) *Pool {
	return &Pool{workerCount: workerCount, queue: q, ctx: ctx}
}

func (p *Pool) Start() {
	for i := 0; i < p.workerCount; i++ {
		p.wg.Add(1)
		go p.worker(i)
	}
}

func backoff(base time.Duration, attempt int) time.Duration {
	return base * time.Duration(1<<attempt)
}

func (p *Pool) worker(id int) {
	defer p.wg.Done()
	for j := range p.queue.Pop() {
		fmt.Printf("[worker %d] menjalankan job %s\n", id, j.ID)
		p.run(id, j)
	}
}

func (p *Pool) run(id int, j *job.Job) {
	j.MarkRunning()

	for {
		j.IncAttempts()

		ctx, cancel := context.WithTimeout(p.ctx, j.Timeout)
		err := j.Task(ctx)
		cancel()

		if err == nil {
			j.MarkDone()
			fmt.Printf("[worker %d] job %s selesai (percobaan ke-%d)\n", id, j.ID, j.Attempts())
			return
		}

		if errors.Is(err, context.Canceled) {
			j.MarkFailed(err)
			fmt.Printf("[worker %d] job %s dibatalkan\n", id, j.ID)
			return
		}

		if j.Attempts() > j.MaxRetries {
			j.MarkFailed(err)
			fmt.Printf("[worker %d] job %s gagal permanen: %v\n", id, j.ID, err)
			return
		}

		delay := backoff(j.BaseDelay, j.Attempts()-1)
		fmt.Printf("[worker %d] job %s gagal (%v), retry dalam %v\n", id, j.ID, err, delay)

		select {
		case <-time.After(delay):
		case <-p.ctx.Done(): // shutdown saat menunggu backoff
			j.MarkFailed(p.ctx.Err())
			return
		}
	}
}

func (p *Pool) Wait() {
	p.wg.Wait()
}
