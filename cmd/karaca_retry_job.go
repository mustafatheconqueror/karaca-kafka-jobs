package cmd

import (
	"github.com/spf13/cobra"

	job "karaca-kafka-jobs/jobs/kafka_retry_job"
)

func init() {
	RootCmd.AddCommand(&cobra.Command{
		Use:   "kafka_retry_job",
		Short: "Karaca retry job",
		Long:  `"Running Karaca Retry Job`,
		RunE:  job.Init,
	})
}
