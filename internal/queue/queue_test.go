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
	for j := range q.Pop() {
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

func TestQueue_CloseEndsRange(t *testing.T) {
	q := queue.NewQueue(1)
	q.Close()

	select {
	case _, ok := <-q.Pop():
		if ok {
			t.Error("channel seharusnya sudah closed dan kosong")
		}
	case <-time.After(time.Second):
		t.Fatal("Pop() hang padahal queue sudah ditutup")
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

	<-q.Pop()
	select {
	case <-pushed:
	case <-time.After(time.Second):
		t.Fatal("Push tidak lanjut setelah ada slot kosong")
	}
}
