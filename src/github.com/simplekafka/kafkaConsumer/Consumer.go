package kafkaConsumer

import (
	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kafka/consumergroup"
	"github.com/wvanbergen/kazoo-go"
	conf "github.com/simplekafka/configuration"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"
)

var (
	zookeeperNodes []string
)

func StartConsumer() {
	log.Println("Consumer Started.")

	consumerGroup := conf.Config.ConsumerGroup

	if consumerGroup == "" {
		log.Fatalf("Empty consumer group")
	}

	zookeeper := strings.Join(conf.Config.Zookeeper, ",")

	if zookeeper == "" {
		log.Fatal("Empty zookeeper")
	}

	config := consumergroup.NewConfig()
	config.Offsets.Initial = sarama.OffsetNewest
	config.Offsets.ProcessingTimeout = 10 * time.Second

	zookeeperNodes, config.Zookeeper.Chroot = kazoo.ParseConnectionString(zookeeper)

	kafkaTopics := conf.Config.Topic.KafkaTopics
	if kafkaTopics == nil {
		log.Fatalf("Empty kafka topics")
	}

	consumer, consumerErr := consumergroup.JoinConsumerGroup(consumerGroup, kafkaTopics, zookeeperNodes, config)
	if consumerErr != nil {
		log.Fatalln(consumerErr)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		if err := consumer.Close(); err != nil {
			sarama.Logger.Println("Error closing the consumer", err)
		}
	}()

	go func() {
		for err := range consumer.Errors() {
			log.Println(err)
		}
	}()

	eventCount := 0
	offsets := make(map[string]map[int32]int64)

	for message := range consumer.Messages() {

		go handleMessage(message)

		if offsets[message.Topic] == nil {
			offsets[message.Topic] = make(map[int32]int64)
		}

		eventCount += 1
		if offsets[message.Topic][message.Partition] != 0 && offsets[message.Topic][message.Partition] != message.Offset-1 {
			log.Printf("Unexpected offset on %s:%d. Expected %d, found %d, diff %d.\n", message.Topic, message.Partition, offsets[message.Topic][message.Partition]+1, message.Offset, message.Offset-offsets[message.Topic][message.Partition]+1)
		}

		offsets[message.Topic][message.Partition] = message.Offset
		consumer.CommitUpto(message)

	}

	log.Printf("Processed %d events.", eventCount)
	log.Printf("%+v", offsets)
}

func handleMessage(message *sarama.ConsumerMessage) {
	log.Println("\n ====> Consumed Message: ", string(message.Value))
}
