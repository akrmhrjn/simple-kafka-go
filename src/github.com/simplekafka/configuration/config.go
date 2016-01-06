package configuration

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
)

const (
	//file = "/Users/akrmhrjn/IdeaProjects/simple-kafka/src/github.com/simplekafka/configuration/config.yml"
	file = "/go/src/github.com/simplekafka/configuration/config.yml"
)

var Config Conf

type Conf struct {

	ConsumerGroup string   `yaml:"consumer_group"`
	Zookeeper     []string `yaml:"zookeeper"`
	Brokers       []string `yaml:"brokers"`

	Topic         topic `yaml:"topic"`
}

type topic struct {
	Test        string `yaml:"test"`

	KafkaTopics []string `yaml:"kafka_topics"`
}


func LoadConfig() {
	source, err := ioutil.ReadFile(file)
	if err != nil {
		log.Println(err)
	}
	err = yaml.Unmarshal(source, &Config)
	if err != nil {
		log.Println(err)
	}
	log.Printf("Config file loaded successfully.")
}
