package kafka_transporter_job

import (
	"log"
	"time"
)

type TransporterJobService struct {
	shovel *Shovel
}

func NewTransporterJobService(shovel *Shovel) *TransporterJobService {
	return &TransporterJobService{shovel: shovel}
}

func (r *TransporterJobService) ExecuteJob() error {
	log.Println("Hello ", time.Now())
	r.shovel.TransporterRun()
	return nil
}
