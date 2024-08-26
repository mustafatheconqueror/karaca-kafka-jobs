package kafka_transporter_job

type TopicConfig struct {
	DeadSuffix      string
	RetrySuffix     string
	MetadataTimeout int
}
