package main

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"log"
	"os"
	"strings"
	"time"
)

type RetryJobService struct {
}

func (r *RetryJobService) ExecuteJob() error {
	log.Println("Hello", time.Now())

	topicConfig := TopicConfig{
		ErrorSuffix:     "_error",
		RetrySuffix:     "_retry",
		MetadataTimeout: 10,
	}

	brokers := []string{"localhost:9092", "localhost:9093", "localhost:9094"}

	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": strings.Join(brokers, ",")})
	if err != nil {
		log.Fatalf("Failed to create AdminClient: %s", err)
	}
	defer adminClient.Close()

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": strings.Join(brokers, ","),
		"group.id":          "order.created.consumer",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatalf("Failed to create consumer: %s", err)
	}
	defer consumer.Close()

	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": strings.Join(brokers, ",")})
	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}
	defer producer.Close()

	consoleLogger := log.New(os.Stdout, "INFO: ", log.LstdFlags)

	topicRepo := &TopicRepository{adminClient: adminClient, topicConfig: topicConfig}
	shovel := &Shovel{
		consumer:      consumer,
		producer:      producer,
		topicRepo:     topicRepo,
		topicConfig:   topicConfig,
		consoleLogger: consoleLogger,
	}

	shovel.Run()
	return nil
}

func NewRetryJobService() *RetryJobService {

	return &RetryJobService{}
}

type TopicConfig struct {
	ErrorSuffix     string
	RetrySuffix     string
	MetadataTimeout int
}

type TopicRepository struct {
	adminClient *kafka.AdminClient
	topicConfig TopicConfig
}

func (repo *TopicRepository) GetErrorTopics() ([]string, error) {
	return repo.GetTopics(repo.topicConfig.ErrorSuffix)
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

type Shovel struct {
	consumer      *kafka.Consumer
	producer      *kafka.Producer
	topicRepo     *TopicRepository
	topicConfig   TopicConfig
	consoleLogger *log.Logger
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
		s.consoleLogger.Printf("Failed to get dead topics: %v", err)
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
