package kafka_transporter_job

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/jasonlvhit/gocron"
	"github.com/spf13/cobra"
	"karaca-kafka-jobs/shared"
	"log"
	"os"
	"strings"
	"time"
)

func Init(cmd *cobra.Command, args []string) error {

	topicConfig := TopicConfig{
		DeadSuffix:      "_dead",
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
		"bootstrap.servers":  strings.Join(brokers, ","),
		"group.id":           "transporter-job",
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": true,
		"session.timeout.ms": 10000,
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

	// Todo: create, persistence and service folder maybe
	topicRepo := NewTopicRepository(adminClient, topicConfig)
	shovel := NewShovel(consumer, producer, topicRepo, topicConfig, consoleLogger)

	gocron.SetLocker(shared.NewMemoryLocker())

	transporterJobService := NewTransporterJobService(shovel)

	log.Println(" ---------------- Started Transporter job execution at ----------------", time.Now())

	transporterJobService.ExecuteJob()

	return nil
}
