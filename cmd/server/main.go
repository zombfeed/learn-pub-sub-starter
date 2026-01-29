package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")
	connection := "amqp://guest:guest@localhost:5672/"
	amqpConnection, err := amqp.Dial(connection)
	if err != nil {
		log.Fatalf("could not connect to RabbitMQ: %v", err)
	}
	defer amqpConnection.Close()
	fmt.Println("Successfully connected to RabbitMQ!")

	conCh, err := amqpConnection.Channel()
	if err != nil {
		log.Fatalf("could not create channel: %v", err)
	}

	err = pubsub.PublishJson(
		conCh,
		routing.ExchangePerilDirect,
		routing.PauseKey,
		routing.PlayingState{
			IsPaused: true,
		},
	)
	if err != nil {
		log.Fatalf("failed to publish to channel: %v", err)
	}
	fmt.Println("Pause message sent!")
}
