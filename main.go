package main

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
)

var maxRetryCount = 3

type Job struct {
	JobId    int
	RetryCnt int
	// RetryAfter time.Time
}

type Queue struct {
	Jobs       chan Job
	RetryQueue chan Job
	Dlq        chan Job
	JobWg      sync.WaitGroup
	// ShutDown   chan struct{}
}

func (q *Queue) handleRetry(job Job) {
	if job.RetryCnt >= maxRetryCount {
		q.Dlq <- job
		q.JobWg.Done()
	} else {
		job.RetryCnt++
		// job.RetryAfter = time.Now().Add(time.Second * time.Duration(1<<job.RetryCnt))
		q.RetryQueue <- job
	}
}

func (q *Queue) exponentialRetryMechanism() {
	for job := range q.RetryQueue {
		// if time.Now().After(job.RetryAfter) {
		// 	q.Jobs <- job
		// }
		q.Jobs <- job
	}
}

func (q *Queue) worker(workerNum int, wg *sync.WaitGroup) {
	defer wg.Done()
	for job := range q.Jobs {
		var err error
		if rand.Intn(3) == 0 {
			err = errors.New("random failure")
		}
		if err != nil {
			fmt.Println("Job failed", job.JobId, "by worker", workerNum)
			q.handleRetry(job)
		} else {
			fmt.Println("Working on job", job.JobId, "by worker", workerNum)
			q.JobWg.Done()
		}
	}
}

func main() {
	numJobs := 10
	numWorkers := 3

	queues := Queue{
		Jobs:       make(chan Job, numJobs),
		RetryQueue: make(chan Job, numJobs),
		Dlq:        make(chan Job, numJobs),
		// ShutDown:   make(chan struct{}),
	}

	var workerWg sync.WaitGroup
	var retryWg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		workerWg.Add(1)
		go queues.worker(i, &workerWg)
	}

	for i := 0; i < numJobs; i++ {
		queues.JobWg.Add(1)
		queues.Jobs <- Job{
			JobId: i + 1,
		}
	}
	retryWg.Add(1)

	go func() {
		defer retryWg.Done()
		queues.exponentialRetryMechanism()
	}()

	go func() {

		queues.JobWg.Wait()
		close(queues.Jobs)

		close(queues.RetryQueue)
		retryWg.Wait()

	}()

	workerWg.Wait()
}
