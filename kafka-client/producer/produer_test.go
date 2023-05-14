package producer

import (
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"os"
	"os/signal"
	"testing"
	"time"
)

// 异步生产者
func TestKafkaProducerAsync(t *testing.T) {
	conf := sarama.NewConfig()
	conf.Producer.Return.Successes = true

	producer, err := sarama.NewAsyncProducer([]string{"dev0:9092", "dev1:9092"}, conf)
	if err != nil {
		log.Fatalln(err)
	}
	defer func(producer sarama.AsyncProducer) {
		if err := producer.Close(); err != nil {
			log.Fatalln(err)
		}
	}(producer)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	var enqueued, producerErrors int
	var i int
ProducerLoop:
	for {
		select {
		case producer.Input() <- &sarama.ProducerMessage{Topic: "fist", Key: nil, Value: sarama.StringEncoder(fmt.Sprintf("async %d", i))}:
			enqueued++
			i++
			time.Sleep(100 * time.Millisecond)
		case err := <-producer.Errors():
			log.Println("Failed to produce message", err)
			producerErrors++
		case s := <-producer.Successes():
			log.Printf("Partition:%d Offset:%d\n", s.Partition, s.Offset)
		case <-signals:
			break ProducerLoop
		}
	}
	log.Printf("Enqueued: %d; errors: %d\n", enqueued, producerErrors)

}

// 同步生产者
func TestKafkaProducerSync(t *testing.T) {
	conf := sarama.NewConfig()
	conf.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer([]string{"dev0:9092", "dev1:9092"}, conf)
	if err != nil {
		log.Fatalln(err)
	}
	defer func(producer sarama.SyncProducer) {
		if err := producer.Close(); err != nil {
			log.Fatalln(err)
		}
	}(producer)

	msg := &sarama.ProducerMessage{
		Topic: "first",
	}
	for i := 0; i < 50; i++ {
		val := fmt.Sprintf("sync %d", i)
		msg.Value = sarama.ByteEncoder(val)
		pid, offset, err := producer.SendMessage(msg)
		if err != nil {
			log.Fatalln(err)
		}
		log.Printf("Partition:%d Offset:%d\n", pid, offset)
	}
}
