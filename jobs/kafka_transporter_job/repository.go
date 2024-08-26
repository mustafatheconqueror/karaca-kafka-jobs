package kafka_transporter_job

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"strings"
	"time"
)

type TopicRepository struct {
	adminClient *kafka.AdminClient
	topicConfig TopicConfig
}

func NewTopicRepository(adminClient *kafka.AdminClient, topicConfig TopicConfig) *TopicRepository {
	return &TopicRepository{
		adminClient: adminClient,
		topicConfig: topicConfig,
	}
}

func (repo *TopicRepository) GetDeadTopics() ([]string, error) {
	return repo.GetTopics(repo.topicConfig.DeadSuffix)
}

func (repo *TopicRepository) GetTopics(filter string) ([]string, error) {
	_, cancel := context.WithTimeout(context.Background(), time.Duration(repo.topicConfig.MetadataTimeout)*time.Second)
	defer cancel()

	metadata, err := repo.adminClient.GetMetadata(nil, true, int(repo.topicConfig.MetadataTimeout*1000))
	if err != nil {
		return nil, err
	}

	topics := make([]string, 0)
	for _, topic := range metadata.Topics {
		if filter == "" || strings.HasSuffix(topic.Topic, filter) {
			topics = append(topics, topic.Topic)
		}
	}

	return topics, nil
}
