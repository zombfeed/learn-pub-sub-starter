package main

import (
	"fmt"
	"log"
	"strings"

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

	gameState := gamelogic.NewGameState(userName)
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		routing.PauseKey+"."+gameState.GetUsername(),
		routing.PauseKey,
		pubsub.SimpleQueueTransient,
		handlerPause(gameState),
	)
	if err != nil {
		log.Fatalf("could not declare and bind the client queue: %v", err)
	}

	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}
		switch strings.ToLower(input[0]) {
		case "spawn":
			if len(input) != 3 {
				log.Println("invalid command")
				continue
			}
			if err = gameState.CommandSpawn(input); err != nil {
				log.Println("invalid command")
			}
		case "move":
			if len(input) != 3 {
				log.Println("invalid command")
				continue
			}
			_, err := gameState.CommandMove(input)
			if err != nil {
				log.Println("invalid command")
			}
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			log.Println("Spamming is not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			log.Println("invalid command")
		}
	}
}
