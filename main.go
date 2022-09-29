package main

import (
	"context"
	"encoding/json"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"io/ioutil"
	"log"
	"strconv"
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
		Retries   int           `json:"retries"`
		Delay     time.Duration `json:"delay"`
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
	//Read JSON configuration and put to var "c"
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

	ctx, cancel := context.WithTimeout(context.Background(), c.Send.Delay*time.Second)
	defer cancel()

	t1 := time.Now()

	for i := 1; i <= c.Send.Retries; i++ {
		body := c.Send.Message + " #" + strconv.Itoa(i)
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
		if c.Send.Retries == i {
			fmt.Println("Start: ", t1.Format("15:01:05.000000"))
			fmt.Println("End: ", time.Now().Format("15:01:05.000000"))
			fmt.Println("Duration: ", time.Now().Sub(t1))
		}
	}
}
