package main

import (
	"fmt"
	"sync"
)

type Job struct {
	JobId int
}

func worker(workerNum int, jobs chan Job, wg *sync.WaitGroup) {
	defer wg.Done()
	for job := range jobs {
		fmt.Println("Working on job", job.JobId, "by worker", workerNum)
	}
}

func main() {
	numJobs := 10
	numWorkers := 3

	jobs := make(chan Job, numJobs)
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(i, jobs, &wg)
	}

	for i := 0; i < numJobs; i++ {
		jobs <- Job{
			JobId: i + 1,
		}
	}
	close(jobs)
	wg.Wait()
}
