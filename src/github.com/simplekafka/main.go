package main

import (
	"github.com/simplekafka/kafkaConsumer"
	"github.com/simplekafka/configuration"
	"github.com/simplekafka/kafkaProducer"
)

func main(){
	configuration.LoadConfig()
	msg := "This is a test message"
	topic := configuration.Config.Topic.Test
	kafkaProducer.MessageProducer(topic, msg)
	kafkaConsumer.StartConsumer()
}