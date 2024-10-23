package rabbitmq

import (
	"fmt"
	"github.com/rabbitmq/amqp091-go"
)

func ConnectToRabbitMQ(config RabbitMQConfig) (*amqp091.Connection, error) {
	conn, err := amqp091.Dial(config.URI)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %v", err)
	}
	return conn, nil
}
