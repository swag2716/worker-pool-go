package main

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
)

var maxRetryCount = 3

type Job struct {
	JobId     int
	RestryCnt int
}

type Queue struct {
	Jobs       chan Job
	RetryQueue chan Job
	Dlq        chan Job
}

func (q *Queue) handleRetry(job Job) {
	if job.RestryCnt >= maxRetryCount {
		q.Dlq <- job
	} else {
		job.RestryCnt++
		q.RetryQueue <- job
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
			q.handleRetry(job)
		} else {
			fmt.Println("Working on job", job.JobId, "by worker", workerNum)
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
	}

	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go queues.worker(i, &wg)
	}

	for i := 0; i < numJobs; i++ {
		queues.Jobs <- Job{
			JobId: i + 1,
		}
	}

	close(queues.Jobs)
	wg.Wait()
}
