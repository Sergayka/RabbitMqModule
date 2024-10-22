package rabbitmq

import (
	"encoding/json"
	"fmt"
	"github.com/rabbitmq/amqp091-go"
	"log"
)

func PublishTask(config RabbitMQConfig, task Task) error {
	log.Println("Connecting to RabbitMQ...")

	conn, err := ConnectToRabbitMQ(config)
	if err != nil {
		log.Printf("Failed to connect to RabbitMQ: %v", err)
		return fmt.Errorf("failed to connect to RabbitMQ: %v", err)
	}
	defer func() {
		log.Println("Closing RabbitMQ connection...")
		conn.Close()
	}()

	log.Println("Opening RabbitMQ channel...")
	ch, err := conn.Channel()
	if err != nil {
		log.Printf("Failed to open a channel: %v", err)
		return fmt.Errorf("failed to open a channel: %v", err)
	}
	defer func() {
		log.Println("Closing RabbitMQ channel...")
		ch.Close()
	}()

	log.Printf("Declaring exchange: %s", config.Exchange)
	err = ch.ExchangeDeclarePassive(
		"msfs.output.exchange", // name of exchange
		"direct",               // type of exchange
		false,                  // durable
		false,                  // auto-deleted
		false,                  // internal
		false,                  // no-wait
		nil,                    // arguments
	)
	if err != nil {
		log.Printf("Failed to declare exchange: %v", err)
		return fmt.Errorf("failed to declare exchange: %v", err)
	}

	// marshall task to JSON
	log.Println("Marshalling task to JSON...")
	body, err := json.Marshal(task)
	if err != nil {
		log.Printf("Failed to marshal task: %v", err)
		return fmt.Errorf("failed to marshal task: %v", err)
	}

	// publishing task to exchange
	log.Printf("Publishing task to exchange %s with routing key %s", config.Exchange, config.RoutingKey)
	err = ch.Publish(
		config.Exchange,   // имя exchange
		config.RoutingKey, // routing key
		false,             // mandatory
		false,             // immediate
		amqp091.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)
	if err != nil {
		log.Printf("Failed to publish task: %v", err)
		return fmt.Errorf("failed to publish task: %v", err)
	}

	log.Printf("Task successfully sent to RabbitMQ: %s", string(body))
	return nil
}

func ConsumeTask(config RabbitMQConfig) error {
	log.Println("Connecting to RabbitMQ for consumer...")

	conn, err := ConnectToRabbitMQ(config)
	if err != nil {
		log.Printf("Failed to connect to RabbitMQ: %v", err)
		return err
	}
	defer func() {
		log.Println("Closing RabbitMQ connection...")
		conn.Close()
	}()

	log.Println("Opening RabbitMQ channel for consumer...")
	ch, err := conn.Channel()
	if err != nil {
		log.Printf("Failed to open a channel: %v", err)
		return err
	}
	defer func() {
		log.Println("Closing RabbitMQ channel...")
		ch.Close()
	}()

	// Проверяем очередь пассивно
	q, err := ch.QueueDeclarePassive(
		"msticket.input.queue", // имя очереди
		false,                  // durable
		false,                  // delete when unused
		false,                  // exclusive
		false,                  // no-wait
		map[string]interface{}{
			"x-dead-letter-exchange":    "msticket.dlx",
			"x-dead-letter-routing-key": "dlx",
		}, // аргументы
	)
	if err != nil {
		log.Printf("Failed to declare queue: %v", err)
		return err
	}

	// Привязываем очередь к exchange
	err = ch.QueueBind(
		q.Name,                 // queue name
		"",                     // routing key
		"msfs.output.exchange", // exchange
		false,
		nil,
	)
	if err != nil {
		log.Printf("Failed to bind queue: %v", err)
		return err
	}

	log.Println("Consuming messages from queue...")
	messages, err := ch.Consume(
		q.Name, // имя очереди
		"",     // consumer tag
		false,  // auto-ack (false, чтобы вручную подтверждать)
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		log.Printf("Failed to register a consumer: %v", err)
		return err
	}

	// Чтение сообщений из очереди
	go func() {
		for message := range messages {
			log.Printf("Received a message: %s", message.Body)

			// Парсим JSON-сообщение
			var taskMessage Task
			err := json.Unmarshal(message.Body, &taskMessage)
			if err != nil {
				log.Printf("Error decoding JSON: %v", err)
				message.Reject(false) // Отказ от сообщения, так как оно невалидно
				continue
			}

			// Выводим данные из сообщения
			log.Printf("Parsed Task - FileID: %s, FileName: %s, URL: %s",
				taskMessage.FileID, taskMessage.FileName, taskMessage.URL)

			message.Ack(false) // Подтвердить обработку
			log.Println("Message acknowledged.")
		}
	}()

	log.Println(" [*] Waiting for messages. To exit press CTRL+C")
	forever := make(chan bool)
	<-forever

	return nil
}
