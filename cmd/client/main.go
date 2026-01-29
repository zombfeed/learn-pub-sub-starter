package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func main() {
	fmt.Println("Starting Peril client...")
	rabbitConnString := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(rabbitConnString)
	if err != nil {
		log.Fatalf("could not connect to RabbitMQ: %v", err)
	}
	defer conn.Close()
	fmt.Println("Successfully connected to RabbitMQ!")
	userName, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("could not get user: %v", err)
	}
	_, _, err = pubsub.DeclareAndBind(
		conn,
		routing.ExchangePerilDirect,
		"pause."+userName,
		routing.PauseKey,
		pubsub.QueueTransient,
	)
	if err != nil {
		log.Fatalf("could not declare and bind the client queue: %v", err)
	}

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)
	<-signalCh
	fmt.Println("RabbitMQ connection closed.")
}
