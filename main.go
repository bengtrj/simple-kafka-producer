package main

import (
	"bufio"
	"fmt"
	"log"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {

	if len(os.Args) != 3 {
		printUsage()
		os.Exit(1)
	}

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
	if err != nil {
		panic(err)
	}

	defer p.Close()

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	// Produce messages to topic (asynchronously)
	topic := os.Args[1]
	filePath := os.Args[2]

	file, err := os.Open(filePath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	//reads two lines at a time
	for scanner.Scan() {
		key := scanner.Bytes()
		if scanner.Scan() {
			p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Value:          scanner.Bytes(),
				Key:            key,
			}, nil)
		}
	}

	// Dumb error handling, should be enough for local files. Indicates probable corrupt/invalid file
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	// Wait for message deliveries before shutting down
	unsentCount := p.Flush(15 * 1000)
	if unsentCount > 0 {
		fmt.Printf("There were %d unflushed events\n", unsentCount)
	}
}

func printUsage() {
	fmt.Println("Usage:")
	fmt.Println("go run main.go -- topic file\n")
	fmt.Println("where:\n")
	fmt.Println(" topic is the name of the kafka topic to publish events\n")
	fmt.Println(" file is the path to the file where the events to produce are\n")
}
