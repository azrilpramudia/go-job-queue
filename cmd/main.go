package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/azrilpramudia/go-job-queue/internal/job"
	"github.com/azrilpramudia/go-job-queue/internal/queue"
	"github.com/azrilpramudia/go-job-queue/internal/worker"
)

func main() {
	// context 1: sinyal OS (Ctrl+C / SIGTERM)
	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// context 2: milik Pool
	workCtx, forceCancel := context.WithCancel(context.Background())
	defer forceCancel()

	q := queue.NewQueue(10)
	pool := worker.NewPool(3, q, workCtx)
	pool.Start()

	go func() {
		defer q.Close()
		for i := 1; i <= 20; i++ {
			select {
			case <-sigCtx.Done():
				fmt.Println("[producer] sinyal diterima, berhenti menerima job baru")
				return
			case <-time.After(500 * time.Millisecond):
			}

			id := fmt.Sprintf("job-%d", i)
			q.Push(job.NewJob(id, func(ctx context.Context) error {
				select {
				case <-time.After(3 * time.Second):
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}, 10*time.Second))
		}
	}()

	// pool.Wait()
	done := make(chan struct{})
	go func() {
		pool.Wait()
		close(done)
	}()

	select {
	case <-done:
		fmt.Println("semua job selesai diproses")
	case <-sigCtx.Done():
		fmt.Println("shutdown dimulai, menunggu job berjalan (maks 10 detik)...")
		select {
		case <-done:
			fmt.Println("shutdown bersih, semua job selesai")
		case <-time.After(10 * time.Second):
			fmt.Println("grace period habis, memaksa job berhenti")
			forceCancel()
			<-done
			fmt.Println("shutdown paksa selesai")
		}
	}
}
