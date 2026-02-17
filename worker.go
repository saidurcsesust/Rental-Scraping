package main

import (
	"context"
	"sync"
)

type detailJob struct {
	ListingURL string
}

func (s *Scraper) startWorkers(ctx context.Context, n int, jobs <-chan detailJob, wg *sync.WaitGroup) {
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case job, ok := <-jobs:
					if !ok {
						return
					}
					s.processDetailJob(ctx, job)
				}
			}
		}()
	}
}
