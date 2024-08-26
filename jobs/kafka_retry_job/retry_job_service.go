package kafka_retry_job

import (
	"log"
	"time"
)

type RetryJobService struct {
	shovel *Shovel
}

func NewRetryJobService(shovel *Shovel) *RetryJobService {
	return &RetryJobService{shovel: shovel}
}

func (r *RetryJobService) ExecuteJob() error {
	log.Println("Hello", time.Now())
	r.shovel.Run()
	return nil
}
