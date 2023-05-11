package consumer

import (
	"context"
	"github.com/Shopify/sarama"
	"log"
	"testing"
)

func newConfig(t *testing.T) *sarama.Config {
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.BalanceStrategyRange}
	//config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.BalanceStrategySticky}
	//config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.BalanceStrategyRoundRobin}

	return config
}

// 消费者组
func TestKafkaConsumerGroup(t *testing.T) {
	config := newConfig(t)
	groupID := "g1"
	client, err := sarama.NewConsumerGroup([]string{"dev0:9092", "dev1:9092"}, groupID, config)
	if err != nil {
		log.Fatalln(err)
	}

	consumer := Consumer{
		ready: make(chan bool),
	}
	for {
		if err := client.Consume(context.Background(), []string{"first"}, &consumer); err != nil {
			log.Fatalln(err)
		}
	}
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	ready chan bool
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/main/consumer_group.go#L27-L29
	for {
		select {
		case message := <-claim.Messages():
			log.Printf("Partition:%d Topic:%s Key:%s Value:%s\n",
				message.Partition, message.Topic, string(message.Key), string(message.Value))
			session.MarkMessage(message, "")

		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/Shopify/sarama/issues/1192
		case <-session.Context().Done():
			return nil
		}
	}
}
