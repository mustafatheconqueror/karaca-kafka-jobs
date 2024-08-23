package main

import (
	"log"
	"time"
)

type RetryJobService struct {
}

func (r *RetryJobService) ExecuteJob() error {
	log.Println("Hello", time.Now())
	return nil
}

func NewRetryJobService() *RetryJobService {

	return &RetryJobService{}
}
