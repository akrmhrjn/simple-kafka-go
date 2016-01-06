package kafkaProducer

import (
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	conf "github.com/simplekafka/configuration"
	"io/ioutil"
	"log"
	"os"
)

var (
	key         = ""
	partitioner = ""
	partition   = -1
	verbose     = false
	silent      = false

	logger = log.New(os.Stderr, "", log.LstdFlags)
)

func MessageProducer(topic string, value string) error {
	var err error

	if topic == "" {
		err = errors.New("no -topic specified")
		log.Println(err)
		return err
	}

	if verbose {
		sarama.Logger = logger
	}

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll

	switch partitioner {
	case "":
		if partition >= 0 {
			config.Producer.Partitioner = sarama.NewManualPartitioner
		} else {
			config.Producer.Partitioner = sarama.NewHashPartitioner
		}
	case "hash":
		config.Producer.Partitioner = sarama.NewHashPartitioner
	case "random":
		config.Producer.Partitioner = sarama.NewRandomPartitioner
	case "manual":
		config.Producer.Partitioner = sarama.NewManualPartitioner
		if partition == -1 {
			err = errors.New("-partition is required when partitioning manually")
			log.Println(err)
			return err
		}
	default:
		err = errors.New(fmt.Sprintf("Partitioner %s not supported.", partitioner))
		log.Println(err)
		return err
	}

	message := &sarama.ProducerMessage{Topic: topic, Partition: int32(partition)}

	if key != "" {
		message.Key = sarama.StringEncoder(key)
	}

	if value != "" {
		message.Value = sarama.StringEncoder(value)
	} else if stdinAvailable() {
		bytes, err := ioutil.ReadAll(os.Stdin)
		if err != nil {
			log.Println("Failed to read data from the standard input: %s", err)
			return err
		}
		message.Value = sarama.ByteEncoder(bytes)
	} else {
		err = errors.New("-value is required, or you have to provide the value on stdin")
		log.Println(err)
		return err
	}

	producer, err := sarama.NewSyncProducer(conf.Config.Brokers, config)
	if err != nil {
		log.Println("Failed to open Kafka producer: %s", err)
		return err
	}
	defer func() {
		if err := producer.Close(); err != nil {
			logger.Println("Failed to close Kafka producer cleanly:", err)
		}
	}()

	partition, offset, err := producer.SendMessage(message)
	log.Println("\n ====> Produced Message: ", message)
	if err != nil {
		log.Println("Failed to produce message: %s", err)
		return err
	} else if !silent {
		fmt.Printf("topic=%s\tpartition=%d\toffset=%d\n", topic, partition, offset)
	}

	return err

}

func stdinAvailable() bool {
	stat, _ := os.Stdin.Stat()
	return (stat.Mode() & os.ModeCharDevice) == 0
}