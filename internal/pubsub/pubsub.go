package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	QueueDurable SimpleQueueType = iota
	QueueTransient
)

func PublishJson[T any](ch *amqp.Channel, exchange, key string, val T) error {
	jsonVal, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("could not marshal data: %v", err)
	}
	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        jsonVal,
	})
	if err != nil {
		return fmt.Errorf("could not publish data: %v", err)
	}
	return nil
}

func DeclareAndBind(conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType) (*amqp.Channel, amqp.Queue, error) {
	var emptyQueue amqp.Queue
	ch, err := conn.Channel()
	if err != nil {
		return nil, emptyQueue, fmt.Errorf("could not create connection: %v", err)
	}

	durable := queueType == QueueDurable
	transient := queueType == QueueTransient
	queue, err := ch.QueueDeclare(
		queueName,
		durable,
		transient,
		transient,
		false,
		nil,
	)
	if err != nil {
		return nil, emptyQueue, err
	}
	return ch, queue, nil
}
