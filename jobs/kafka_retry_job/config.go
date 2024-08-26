package kafka_retry_job

type TopicConfig struct {
	ErrorSuffix     string
	RetrySuffix     string
	MetadataTimeout int
}
