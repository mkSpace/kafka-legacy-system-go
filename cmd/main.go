package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
	"strconv"
	"sync"
	"time"
)

const (
	topicPrefix         = "test-topic-"
	firstBrokerAddress  = "192.168.0.16:9092"
	secondBrokerAddress = "192.168.0.16:9093"
	thirdBrokerAddress  = "192.168.0.16:9094"
)

func main() {
	var wg sync.WaitGroup
	topicName := topicPrefix + strconv.Itoa(int(time.Now().UnixMilli()))
	createTopic(topicName)

	wg.Add(2)
	go produceMessages(topicName, &wg)
	go consumeMessages(topicName, &wg)

	wg.Wait()
}

func createTopic(topic string) {
	conn, err := kafka.Dial("tcp", firstBrokerAddress)
	if err != nil {
		log.Fatal("failed to connect to Kafka:", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		log.Fatal("failed to get controller:", err)
	}
	controllerConn, err := kafka.Dial("tcp", controller.Host+":"+strconv.Itoa(controller.Port))
	if err != nil {
		log.Fatal("failed to connect to controller:", err)
	}
	defer controllerConn.Close()

	topicConfig := kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     2,
		ReplicationFactor: 3,
	}
	err = controllerConn.CreateTopics(topicConfig)
	if err != nil {
		log.Fatal("failed to create topic :", err)
	}
	fmt.Println("Topic Created: ", topic)
}

func produceMessages(topic string, wg *sync.WaitGroup) {
	writer := kafka.Writer{
		Addr:  kafka.TCP(firstBrokerAddress, secondBrokerAddress, thirdBrokerAddress),
		Topic: topic,
		// 어떤 기준으로 파티션을 라우팅 할 건지에 대한 설정. RoundRobin, LeastBytes(최소 바이트 기반), Hash 등의 전략이 있음
		Balancer: &kafka.LeastBytes{},
	}
	defer func() {
		writer.Close()
		wg.Done()
	}()

	i := 0
	for {
		err := writer.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte(fmt.Sprintf("Key-%d", i)),
			Value: []byte(fmt.Sprintf("Message #%d", i)),
		})
		if err != nil {
			log.Fatal("failed to write messages:", err)
		}
		fmt.Println("Sent: ", i)
		i++
		time.Sleep(time.Second)
	}
}

func consumeMessages(topic string, wg *sync.WaitGroup) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{firstBrokerAddress, secondBrokerAddress, thirdBrokerAddress},
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
		GroupID:  "consumer-group-1",
	})
	defer func() {
		reader.Close()
		wg.Done()
	}()

	for {
		message, err := reader.FetchMessage(context.Background())
		if err != nil {
			log.Fatal("failed to fetch message:", err)
		}
		fmt.Printf("Received: %s, Key: %s\n", string(message.Value), string(message.Key))

		if err := reader.CommitMessages(context.Background(), message); err != nil {
			log.Fatal("failed to commit messages:", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
