package main

import (
	"context"
	"encoding/json"
	amqp "github.com/rabbitmq/amqp091-go"
	"io/ioutil"
	"log"
	"time"
)

type Config struct {
	ServerURL string `json:"serverURL"`
	Queue     struct {
		QueueName  string `json:"queueName"`
		Durable    bool   `json:"durable"`
		AutoDelete bool   `json:"autoDelete"`
		Exclusive  bool   `json:"exclusive"`
		NoWait     bool   `json:"noWait"`
	} `json:"queue"`
	Consume struct {
		Consumer  string `json:"consumer"`
		AutoAck   bool   `json:"autoAck"`
		Exclusive bool   `json:"exclusive"`
		NoLocal   bool   `json:"noLocal"`
		NoWait    bool   `json:"noWait"`
	} `json:"consume"`
	Send struct {
		Frequency time.Duration `json:"frequency"`
		Message   string        `json:"message"`
		Exchange  string        `json:"exchange"`
		Mandatory bool          `json:"mandatory"`
		Immediate bool          `json:"immediate"`
	} `json:"send"`
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func main() {
	//Read configuration and put to var "c"
	file, _ := ioutil.ReadFile("config.json")
	c := Config{}
	_ = json.Unmarshal([]byte(file), &c)

	conn, err := amqp.Dial(c.ServerURL)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		c.Queue.QueueName,  // name
		c.Queue.Durable,    // durable
		c.Queue.AutoDelete, // delete when unused
		c.Queue.Exclusive,  // exclusive
		c.Queue.NoWait,     // no-wait
		nil,                // arguments
	)
	failOnError(err, "Failed to declare a queue")

	ctx, cancel := context.WithTimeout(context.Background(), c.Send.Frequency*time.Second)
	defer cancel()

	body := c.Send.Message
	err = ch.PublishWithContext(ctx,
		c.Send.Exchange,  // exchange
		q.Name,           // routing key
		c.Send.Mandatory, // mandatory
		c.Send.Immediate, // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})
	failOnError(err, "Failed to publish a message")
	log.Printf(" [x] Sent %s\n", body)
}
