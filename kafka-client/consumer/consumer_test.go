package consumer

import (
	"github.com/Shopify/sarama"
	"log"
	"sync"
	"testing"
)

// 单个消费者
func TestKafkaConsumer(t *testing.T) {
	config := sarama.NewConfig()

	consumer, err := sarama.NewConsumer([]string{"dev0:9092", "dev1:9092"}, config)
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	// 获取 topic 为 first 的所有分区
	partitions, err := consumer.Partitions("first")
	if err != nil {
		log.Fatalln(err)
	}

	var wg sync.WaitGroup
	for partition := range partitions {
		c, err := consumer.ConsumePartition("first", int32(partition), sarama.OffsetNewest)
		if err != nil {
			log.Println(err)
			continue
		}
		wg.Add(1)
		go func(c sarama.PartitionConsumer) {
			for {
				select {
				case msg := <-c.Messages():
					log.Printf("Partiton:%d Topic:%s Key:%s Value:%s\n",
						msg.Partition, msg.Topic, string(msg.Key), string(msg.Value))
				}
			}
		}(c)
	}

	wg.Wait()
}
