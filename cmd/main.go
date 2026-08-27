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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	q := queue.NewQueue(10)
	pool := worker.NewPool(3, q, ctx)
	pool.Start()

	for i := 1; i <= 5; i++ {
		id := fmt.Sprintf("job-%d", i)

		if i == 3 {
			q.Push(job.NewJob(id, func(ctx context.Context) error {
				select {
				case <-time.After(5 * time.Second):
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}, 2*time.Second))
			continue
		}

		q.Push(job.NewJob(id, func(ctx context.Context) error{
			time.Sleep(1 * time.Second)
			return nil
		}, 3*time.Second))
	}

	q.Close()
	pool.Wait()
	fmt.Println("semua job selesai diproses")
}