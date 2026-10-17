package queue_test

import (
	"context"
	"testing"
	"time"

	"github.com/azrilpramudia/go-job-queue/internal/job"
	"github.com/azrilpramudia/go-job-queue/internal/queue"
)

func newTestJob(id string) *job.Job {
	return job.NewJob(id, func(ctx context.Context) error { return nil }, time.Second)
}

func TestQueue_FIFOOrder(t *testing.T) {
	q := queue.NewQueue(3)
	q.Push(newTestJob("a"))
	q.Push(newTestJob("b"))
	q.Push(newTestJob("c"))
	q.Close()

	var got []string
	for {
		j, ok := q.Pop()
		if !ok {
			break
		}
		got = append(got, j.ID)
	}

	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("urutan ke-%d = %s, want %s", i, got[i], want[i])
		}
	}
}

func TestQueue_PriorityOrder(t *testing.T) {
	q := queue.NewQueue(4)

	low := newTestJob("low")
	normal := newTestJob("normal")
	high := newTestJob("high")
	alsoHigh := newTestJob("also-high")
	low.Priority = -1
	high.Priority = 10
	alsoHigh.Priority = 10
	q.Push(low)
	q.Push(normal)
	q.Push(high)
	q.Push(alsoHigh)
	q.Close()

	var got []string
	for {
		j, ok := q.Pop()
		if !ok {
			break
		}
		got = append(got, j.ID)
	}

	want := []string{"high", "also-high", "normal", "low"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("position %d = %s, want %s", i, got[i], want[i])
		}
	}
}

func TestQueue_CloseEndsRange(t *testing.T) {
	q := queue.NewQueue(1)
	q.Close()

	if _, ok := q.Pop(); ok {
		t.Error("Pop() should report closed and empty queue")
	}
}

func TestQueue_PushBlocksWhenFull(t *testing.T) {
	q := queue.NewQueue(1)
	q.Push(newTestJob("a")) // buffer penuh

	pushed := make(chan struct{})
	go func() {
		q.Push(newTestJob("b")) // blocking
		close(pushed)
	}()

	select {
	case <-pushed:
		t.Fatal("Push seharusnya blocking saat buffer penuh")
	case <-time.After(100 * time.Millisecond):
	}

	if j, ok := q.Pop(); !ok || j.ID != "a" {
		t.Fatalf("first Pop() = (%v, %t), want job a", j, ok)
	}
	select {
	case <-pushed:
	case <-time.After(time.Second):
		t.Fatal("Push tidak lanjut setelah ada slot kosong")
	}
	if j, ok := q.Pop(); !ok || j.ID != "b" {
		t.Fatalf("second Pop() = (%v, %t), want job b", j, ok)
	}
	q.Close()
}

func TestQueue_UnbufferedPushWaitsForPop(t *testing.T) {
	q := queue.NewQueue(0)
	pushed := make(chan struct{})
	go func() {
		q.Push(newTestJob("a"))
		close(pushed)
	}()

	select {
	case <-pushed:
		t.Fatal("Push should wait until a Pop is ready")
	case <-time.After(50 * time.Millisecond):
	}

	if j, ok := q.Pop(); !ok || j.ID != "a" {
		t.Fatalf("Pop() = (%v, %t), want job a", j, ok)
	}
	select {
	case <-pushed:
	case <-time.After(time.Second):
		t.Fatal("Push did not complete after Pop")
	}
	q.Close()
}

func TestQueue_PushContextUnblocksWhenCanceled(t *testing.T) {
	q := queue.NewQueue(1)
	q.Push(newTestJob("full"))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- q.PushContext(ctx, newTestJob("blocked"))
	}()
	cancel()

	select {
	case err := <-done:
		if err != context.Canceled {
			t.Errorf("PushContext() error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("PushContext() did not unblock after context cancellation")
	}
	q.Close()
}
