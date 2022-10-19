/**
 * Author: Mitch Allen
 * File: go-lib.go
 */

package rabbitUtils

import (
	"context"
	"fmt"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

type RabbitRep struct {
	connection    *amqp091.Connection
	exchange      *RabbitExchange
	channel       *amqp091.Channel
	queueInstance []amqp091.Queue
}

type RabbitQueue struct {
	name           string
	durable        bool          `default:"false"`
	deleteWhenUsed bool          `default:"false"`
	exclusive      bool          `default:"false"`
	noWait         bool          `default:"false"`
	args           amqp091.Table `default:"nil"`
}
type RabbitExchange struct {
	name         string
	exchangeType string        `default:"direct"`
	durable      bool          `default:"false"`
	autoDelete   bool          `default:"false"`
	internal     bool          `default:"false"`
	noWait       bool          `default:"false"`
	args         amqp091.Table `default:"nil"`
}

type fn func([]byte)

func CreateExchange(rabbit *RabbitRep) error {
	fmt.Printf("\n\nimostafa : %+v\n", rabbit.exchange)
	err := rabbit.channel.ExchangeDeclare(rabbit.exchange.name,
		rabbit.exchange.exchangeType,
		rabbit.exchange.durable,
		rabbit.exchange.autoDelete,
		rabbit.exchange.internal,
		rabbit.exchange.noWait, nil)
	return err
}

func BoundQueueToExchange(rabbit *RabbitRep, routingKey string) error {
	for _, q := range rabbit.queueInstance {
		err := rabbit.channel.QueueBind(q.Name,
			routingKey,
			rabbit.exchange.name,
			false, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func InitializeRabbit(uri string, exchange RabbitExchange, queues []RabbitQueue) (*RabbitRep, error) {
	//conn, err := amqp091.Dial("amqp://user:bitnami@localhost:5672/")
	conn, err := amqp091.Dial(uri)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()

	if err != nil {
		return nil, err
	}

	var qList []amqp091.Queue
	for _, q := range queues {
		fmt.Printf("\n\n\t\tqData : %+v\n", q)
		queue, err := ch.QueueDeclare(q.name, q.durable, q.deleteWhenUsed, q.exclusive, q.noWait, nil)
		if err != nil {
			fmt.Printf("err: %v\n", err)
			return nil, err
		}
		qList = append(qList, queue)
	}
	return &RabbitRep{exchange: &exchange, connection: conn, channel: ch, queueInstance: qList}, nil
}

func Send(rabbit *RabbitRep, data string, ct string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// fmt.Printf("\n\n\t\teslami : %+v\n", rabbit)

	err := rabbit.channel.PublishWithContext(ctx,
		rabbit.exchange.name,
		"",
		false,
		false,
		amqp091.Publishing{
			ContentType: ct,
			Body:        []byte(data),
		})

	if err != nil {
		return err
	}
	return nil
}

func Receive(rabbit *RabbitRep, queueName string, function fn) {
	msgs, _ := rabbit.channel.Consume(
		queueName,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	var forever chan struct{}
	go func() {
		for d := range msgs {
			function([]byte(d.Body))
		}
	}()
	<-forever
}
