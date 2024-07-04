package main

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"norrbom.org/aggregator/model"
)

const (
	KafkaServer = "127.0.0.1:9092"
	KafkaTopic  = "aux"
	MsgCount    = 100
)

func main() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": KafkaServer,
	})

	if err != nil {
		panic(err)
	}

	defer p.Close()

	// Go-routine to handle message delivery reports and
	// possibly other event types (errors, stats, etc)
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Failed to deliver message: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Produced event to topic %s: key = %-10s value = %s\n",
						*ev.TopicPartition.Topic, string(ev.Key), string(ev.Value))
				}
			}
		}
	}()

	var messages [][]byte

	for n := 0; n < MsgCount; n++ {
		aux := model.Aux{
			Name:        "name-" + strconv.Itoa(n),
			Annotations: []string{string(n%75 + 48), string(n%75 + 49), string(n%75 + 50), string(n%75 + 51)},
		}
		auxMsg, err := json.Marshal(aux)
		messages = append(messages, auxMsg)

		if err != nil {
			panic(err)
		}
	}

	topic := KafkaTopic
	for n := 0; n < MsgCount; n++ {
		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            []byte("aux"),
			Value:          messages[n],
		}, nil)

		if err != nil {
			panic(err)
		}
	}

	// Wait for all messages to be delivered
	p.Flush(15 * 1000)
}
