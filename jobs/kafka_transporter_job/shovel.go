package kafka_transporter_job

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

func (s *Shovel) TransporterRun() {
	deadTopics, err := s.topicRepo.GetDeadTopics()
	if err != nil {
		s.consoleLogger.Printf("Failed to get deadTopics topics: %v", err)
		return
	}
	for _, deadTopic := range deadTopics {
		s.consoleLogger.Printf("*****************Started shovel********************* for topic: %s", deadTopic)
		shoveledMessageCount := 0
		startDateForTopic := time.Now().UTC()

		err = s.consumer.SubscribeTopics([]string{deadTopic}, nil)
		if err != nil {
			s.consoleLogger.Printf("Failed to subscribe to topic %s: %v", deadTopic, err)
			continue
		}

		for {
			msg, err := s.consumer.ReadMessage(20 * time.Second)
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

			// Header'lar arasında isRetryable değerini kontrol et
			isRetryable := false
			for _, header := range msg.Headers {
				if header.Key == "isRetryable" && string(header.Value) == "true" {
					isRetryable = true
					break
				}
			}

			if isRetryable {
				retryTopic := strings.Replace(deadTopic, s.topicConfig.DeadSuffix, s.topicConfig.RetrySuffix, 1)
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
			} else {
				s.consoleLogger.Printf("Ignoring message from topic: %s because isRetryable is false", deadTopic)
			}
		}

		s.consoleLogger.Printf(" ----------------Finished shovel for topic: %s. %d messages sent to retry topic ----------------", deadTopic, shoveledMessageCount)
		err := s.consumer.Unsubscribe()
		if err != nil {
			return
		}
	}
}
