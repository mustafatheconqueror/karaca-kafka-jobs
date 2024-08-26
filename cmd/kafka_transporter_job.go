package cmd

import (
	"github.com/spf13/cobra"
	job "karaca-kafka-jobs/jobs/kafka_transporter_job"
)

func init() {
	RootCmd.AddCommand(&cobra.Command{
		Use:   "kafka_transporter_job",
		Short: "Kafka Transporter  job",
		Long:  `"Kafka Transporter Job`,
		RunE:  job.Init,
	})
}
