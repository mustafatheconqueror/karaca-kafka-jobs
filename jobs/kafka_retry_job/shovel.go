package kafka_retry_job

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"log"
	"strings"
	"time"
)

type Shovel struct {
	consumer      *kafka.Consumer
	producer      *kafka.Producer
	topicRepo     *TopicRepository
	topicConfig   TopicConfig
	consoleLogger *log.Logger
}

func NewShovel(consumer *kafka.Consumer, producer *kafka.Producer, topicRepo *TopicRepository, topicConfig TopicConfig, logger *log.Logger) *Shovel {
	return &Shovel{
		consumer:      consumer,
		producer:      producer,
		topicRepo:     topicRepo,
		topicConfig:   topicConfig,
		consoleLogger: logger,
	}
}

func (s *Shovel) ProduceToRetry(topic string, key []byte, value []byte, headers []kafka.Header) error {
	return s.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            key,
		Value:          value,
		Headers:        headers,
	}, nil)
}

func (s *Shovel) Run() {
	errorTopics, err := s.topicRepo.GetErrorTopics()
	if err != nil {
		s.consoleLogger.Printf("Failed to get error topics: %v", err)
		return
	}

	for _, errorTopic := range errorTopics {
		s.consoleLogger.Printf("Started shovel for topic: %s", errorTopic)
		shoveledMessageCount := 0
		startDateForTopic := time.Now().UTC()

		err = s.consumer.SubscribeTopics([]string{errorTopic}, nil)
		if err != nil {
			s.consoleLogger.Printf("Failed to subscribe to topic %s: %v", errorTopic, err)
			continue
		}

		for {
			msg, err := s.consumer.ReadMessage(-1)
			if err != nil {
				if err.(kafka.Error).Code() == kafka.ErrTimedOut {
					break
				}
				s.consoleLogger.Printf("Failed to consume message: %v", err)
				break
			}

			if msg.Timestamp.After(startDateForTopic) {
				break
			}

			retryTopic := strings.Replace(errorTopic, s.topicConfig.ErrorSuffix, s.topicConfig.RetrySuffix, 1)
			err = s.ProduceToRetry(retryTopic, msg.Key, msg.Value, msg.Headers)
			if err != nil {
				s.consoleLogger.Printf("Failed to produce to retry topic %s: %v", retryTopic, err)
				break
			}

			_, err = s.consumer.CommitMessage(msg)
			if err != nil {
				s.consoleLogger.Printf("Failed to commit message: %v", err)
				break
			}

			shoveledMessageCount++
		}

		s.consoleLogger.Printf("Finished shovel for topic: %s. %d messages sent to retry topic", errorTopic, shoveledMessageCount)
		err := s.consumer.Unsubscribe()
		if err != nil {
			return
		}
	}
}
