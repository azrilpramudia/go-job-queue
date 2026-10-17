package queue

import (
	"container/heap"
	"sync"

	"github.com/azrilpramudia/go-job-queue/internal/job"
)

type Queue struct {
	mu             sync.Mutex
	cond           *sync.Cond
	jobs           jobHeap
	capacity       int
	waitingPoppers int
	nextSequence   uint64
	closed         bool
}

func NewQueue(bufferSize int) *Queue {
	if bufferSize < 0 {
		panic("queue: negative capacity")
	}
	q := &Queue{capacity: bufferSize}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *Queue) Push(j *job.Job) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for !q.closed && q.isFull() {
		q.cond.Wait()
	}
	if q.closed {
		panic("queue: push to closed queue")
	}

	heap.Push(&q.jobs, queuedJob{
		job:      j,
		priority: j.Priority,
		sequence: q.nextSequence,
	})
	q.nextSequence++
	q.cond.Broadcast()
}

func (q *Queue) Pop() (*job.Job, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for q.jobs.Len() == 0 && !q.closed {
		q.waitingPoppers++
		q.cond.Broadcast()
		q.cond.Wait()
		q.waitingPoppers--
	}
	if q.jobs.Len() == 0 {
		return nil, false
	}

	item := heap.Pop(&q.jobs).(queuedJob)
	q.cond.Broadcast()
	return item.job, true
}

func (q *Queue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		panic("queue: close of closed queue")
	}
	q.closed = true
	q.cond.Broadcast()
}

func (q *Queue) isFull() bool {
	if q.capacity == 0 {
		return q.jobs.Len() > 0 || q.waitingPoppers == 0
	}
	return q.jobs.Len() >= q.capacity
}

type queuedJob struct {
	job      *job.Job
	priority int
	sequence uint64
}

type jobHeap []queuedJob

func (h jobHeap) Len() int { return len(h) }

func (h jobHeap) Less(i, j int) bool {
	if h[i].priority != h[j].priority {
		return h[i].priority > h[j].priority
	}
	return h[i].sequence < h[j].sequence
}

func (h jobHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *jobHeap) Push(value any) {
	*h = append(*h, value.(queuedJob))
}

func (h *jobHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = queuedJob{}
	*h = old[:n-1]
	return item
}
