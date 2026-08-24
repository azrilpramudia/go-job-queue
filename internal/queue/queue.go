package queue

import (
	"github.com/azrilpramudia/go-job-queue/internal/job"
)

type Queue struct {
	jobs chan *job.Job
}

func NewQueue(bufferSize int) *Queue {
	return &Queue{
		jobs: make(chan *job.Job, bufferSize),
	}
}

func (q *Queue) Push(j *job.Job) {
	q.jobs <- j
}

func (q *Queue) Pop() <-chan *job.Job {
	return q.jobs 
}

func (q *Queue) Close() {
	close(q.jobs)
}
