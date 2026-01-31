package main

import (
	"fmt"
	"log"
	"strings"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")
	rabbitConnString := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(rabbitConnString)
	if err != nil {
		log.Fatalf("could not connect to RabbitMQ: %v", err)
	}
	defer conn.Close()
	fmt.Println("Successfully connected to RabbitMQ!")

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal("could not create channel: %v", err)
	}
	defer ch.Close()

	err = ch.ExchangeDeclare(
		routing.ExchangePerilDirect,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("could not declare exchange: %v", err)
	}
	err = ch.ExchangeDeclare(
		routing.ExchangePerilTopic,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("could not declare exchange: %v", err)
	}
	_, queue, err := pubsub.DeclareAndBind(
		conn,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		routing.GameLogSlug+".*",
		pubsub.SimpleQueueDurable,
	)
	if err != nil {
		log.Fatalf("could not declare and bind queue: %v", err)
	}
	fmt.Printf("Queue %v declared and bound!\n", queue.Name)

	err = pubsub.SubscribeGob(
		conn,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		routing.GameLogSlug+".*",
		pubsub.SimpleQueueDurable,
		handlerLogs(),
	)
	if err != nil {
		log.Fatalf("could not subscribe to game_log: %v", err)
	}
	gamelogic.PrintServerHelp()
	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}
		firstWord := strings.ToLower(input[0])
		switch firstWord {
		case "pause":
			err = sendMessage(conn, routing.PauseKey)
			if err != nil {
				log.Fatal(err)
			}
			log.Println("Pause message sent!")
		case "resume":
			err = sendMessage(conn, "resume")
			if err != nil {
				log.Fatal(err)
			}
			log.Println("Resume message sent!")
		case "quit":
			log.Println("Quitting!")
			return
		default:
			log.Println("Unrecognized command.")
		}

	}
}

func sendMessage(conn *amqp.Connection, key string) error {
	publishCh, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("could not create channel: %v", err)
	}
	isPaused := key == routing.PauseKey

	err = pubsub.PublishJSON(
		publishCh,
		routing.ExchangePerilDirect,
		routing.PauseKey,
		routing.PlayingState{
			IsPaused: isPaused,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish to channel: %v", err)
	}
	return nil
}
