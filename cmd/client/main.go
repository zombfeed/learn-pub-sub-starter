package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

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
	publishCh, err := conn.Channel()
	if err != nil {
		log.Fatalf("could not create channel: %v", err)
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
		log.Fatalf("could not subscribe to pause: %v", err)
	}

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		routing.ArmyMovesPrefix+"."+gameState.GetUsername(),
		routing.ArmyMovesPrefix+".*",
		pubsub.SimpleQueueTransient,
		handlerMove(gameState, publishCh),
	)
	if err != nil {
		log.Fatalf("could not subscribe to army moves: %v", err)
	}

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		routing.WarRecognitionsPrefix,
		routing.WarRecognitionsPrefix+".*",
		pubsub.SimpleQueueDurable,
		handlerWar(gameState, publishCh),
	)
	if err != nil {
		log.Fatalf("could not subscribe to war: %v", err)
	}

	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}
		switch strings.ToLower(input[0]) {
		case "spawn":
			if err = gameState.CommandSpawn(input); err != nil {
				log.Println("invalid command")
			}
		case "move":
			move, err := gameState.CommandMove(input)
			if err != nil {
				log.Println("invalid command")
			}
			err = pubsub.PublishJSON(
				publishCh,
				routing.ExchangePerilTopic,
				routing.ArmyMovesPrefix+"."+move.Player.Username,
				move,
			)
			if err != nil {
				log.Printf("error: %v", err)
				continue
			}
			fmt.Printf("Moved %v units to %s\n", len(move.Units), move.ToLocation)
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			if len(input) != 2 {
				fmt.Printf("expected: spam <amount>")
				continue
			}
			n, err := strconv.Atoi(input[1])
			if err != nil {
				fmt.Printf("error: %s is not a valid number\n", input[1])
				continue
			}
			for i := 0; i <= n; i++ {
				msg := gamelogic.GetMaliciousLog()
				err = publishGameLog(gameState, msg, publishCh)
				if err != nil {
					fmt.Printf("could not publish malicious log: %v", err)
					continue
				}
			}
			fmt.Printf("published %v malicious logs\n", n)
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("invalid command")
		}
	}
}

func publishGameLog(gs *gamelogic.GameState, message string, publishCh *amqp.Channel) error {
	gameLog := routing.GameLog{
		CurrentTime: time.Now(),
		Message:     message,
		Username:    gs.GetUsername(),
	}

	err := pubsub.PublishGob(
		publishCh,
		routing.ExchangePerilTopic,
		routing.GameLogSlug+"."+gs.GetUsername(),
		gameLog,
	)
	if err != nil {
		return fmt.Errorf("error: %v", err)
	}

	return nil
}
