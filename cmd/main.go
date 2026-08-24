package main

import (
	"context"
	"fmt"
	"time"

	"github.com/azrilpramudia/go-job-queue/internal/job"
	"github.com/azrilpramudia/go-job-queue/internal/queue"
	"github.com/azrilpramudia/go-job-queue/internal/worker"
)

func main() {
	q := queue.NewQueue(10)
	pool := worker.NewPool(3, q)
	pool.Start()

	for i := 1; i <= 5; i++ {
		id := fmt.Sprintf("job-%d", i)
		q.Push(job.NewJob(id, func(ctx context.Context) error{
			time.Sleep(1 * time.Second)
			return nil
		}))
	}

	q.Close()
	pool.Wait()
	fmt.Println("semua job selesai diproses")
}