package main

import (
	"github.com/jasonlvhit/gocron"
	"log"
	"time"
)

func main() {

	var jobExecutionTime = 20

	gocron.SetLocker(NewMemoryLocker())

	location, _ := time.LoadLocation("Europe/Istanbul")

	var retryJobService = NewRetryJobService()

	gocron.Every(uint64(jobExecutionTime)).Seconds().Loc(location).Lock().Do(retryJobService.ExecuteJob)

	log.Println("Started job execution at ", time.Now())

	<-gocron.Start()

}
