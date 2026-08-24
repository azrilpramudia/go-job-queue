package worker

import (
	"context"
	"fmt"
	"sync"

	"github.com/azrilpramudia/go-job-queue/internal/job"
	"github.com/azrilpramudia/go-job-queue/internal/queue"
)

type Pool struct {
	workerCount int
	queue 	*queue.Queue
	wg sync.WaitGroup
}

func NewPool(workerCount int, q *queue.Queue) *Pool {
	return &Pool{
		workerCount: workerCount,
		queue: q,
	}
}

func (p *Pool) Start() {
	for i := 0; i < p.workerCount; i++ {
		p.wg.Add(1)
		go p.worker(i)
	} 
}

func (p *Pool) worker(id int) {
	defer p.wg.Done()

	for j := range p.queue.Pop() {
		fmt.Printf("[worker %d] menjalankan job %s\n", id, j.ID)
		err := j.Task(context.Background())
		if err != nil {
			j.Status = job.StatusFailed
			j.Err = err
			fmt.Printf("[worker %d] job %s gagal: %v\n", id, j.ID, err)
		} else {
			j.Status = job.StatusDone
			fmt.Printf("[worker %d] job %s selesai\n", id, j.ID)
		}
	}
} 

func (p *Pool) Wait() {
	p.wg.Wait()
}
