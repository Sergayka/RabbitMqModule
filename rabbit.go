package rabbitmq

import (
	"encoding/json"
	"fmt"
	"github.com/rabbitmq/amqp091-go"
	"log"
)

// PublishTask -> sends the task to the RabbitMQ queue.
// It accepts the RabbitMQ configuration and the task to be sent.
func PublishTask(config RabbitMQConfig, task Task) error {
	conn, err := ConnectToRabbitMQ(config)
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %v", err)
	}
	conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open a channel: %v", err)
	}
	ch.Close()

	err = ch.ExchangeDeclarePassive(
		config.Exchange, // name of exchange
		"direct",        // type of exchange
		false,           // durable
		false,           // auto-deleted
		false,           // internal
		false,           // no-wait
		nil,             // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare exchange: %v", err)
	}

	// marshall task to JSON
	body, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("failed to marshal task: %v", err)
	}

	// publishing task to exchange
	err = ch.Publish(
		config.Exchange,   // name of exchange
		config.RoutingKey, // routing key
		false,             // mandatory
		false,             // immediate
		amqp091.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish task: %v", err)
	}

	return nil
}

// ConsumeTask -> reads messages from the RabbitMQ queue.
// It also accepts the RabbitMQ configuration.
func ConsumeTask(config RabbitMQConfig) error {
	conn, err := ConnectToRabbitMQ(config)
	if err != nil {
		return err
	}
	conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	ch.Close()

	// checking the queue passively
	q, err := ch.QueueDeclarePassive(
		config.Queue, // имя очереди
		false,        // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		map[string]interface{}{
			"x-dead-letter-exchange":    config.DLXExchange,
			"x-dead-letter-routing-key": config.DLXKey,
		}, // arguments
	)
	if err != nil {
		return err
	}

	// linking the queue to exchange
	err = ch.QueueBind(
		q.Name,                 // queue name
		"",                     // routing key
		"msfs.output.exchange", // exchange
		false,
		nil,
	)
	if err != nil {
		return err
	}

	messages, err := ch.Consume(
		q.Name, // name of queue
		"",     // consumer tag
		false,  // auto-ack (false, чтобы вручную подтверждать)
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		return err
	}

	// reading messages from a queue
	go func() {
		for message := range messages {
			// parsing JSON-сообщение
			var taskMessage Task

			err := json.Unmarshal(message.Body, &taskMessage)
			if err != nil {
				message.Reject(false) // rejecting the message because it is invalid
				continue
			}

			log.Printf("Parsed Task - FileID: %s, FileName: %s, URL: %s",
				taskMessage.FileID, taskMessage.FileName, taskMessage.URL)

			message.Ack(false) // confirmation of processing
			log.Println("Message acknowledged.")
		}
	}()

	log.Println(" [*] Waiting for messages. To exit press CTRL+C")
	forever := make(chan bool)
	<-forever

	return nil
}
