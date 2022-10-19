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
	Connection    *amqp091.Connection
	Exchange      *RabbitExchange
	Channel       *amqp091.Channel
	QueueInstance []amqp091.Queue
}

type RabbitQueue struct {
	Name           string
	Durable        bool          `default:"false"`
	DeleteWhenUsed bool          `default:"false"`
	Exclusive      bool          `default:"false"`
	NoWait         bool          `default:"false"`
	Args           amqp091.Table `default:"nil"`
}
type RabbitExchange struct {
	Name         string
	ExchangeType string        `default:"direct"`
	Durable      bool          `default:"false"`
	AutoDelete   bool          `default:"false"`
	Internal     bool          `default:"false"`
	NoWait       bool          `default:"false"`
	Args         amqp091.Table `default:"nil"`
}

type RabbitFunction func([]byte)

func CreateExchange(rabbit *RabbitRep) error {
	// fmt.Printf("\n\nimostafa : %+v\n", rabbit.Exchange)
	err := rabbit.Channel.ExchangeDeclare(rabbit.Exchange.Name,
		rabbit.Exchange.ExchangeType,
		rabbit.Exchange.Durable,
		rabbit.Exchange.AutoDelete,
		rabbit.Exchange.Internal,
		rabbit.Exchange.NoWait, nil)
	return err
}

func BoundQueueToExchange(rabbit *RabbitRep, routingKey string) error {
	for _, q := range rabbit.QueueInstance {
		err := rabbit.Channel.QueueBind(q.Name,
			routingKey,
			rabbit.Exchange.Name,
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
		// fmt.Printf("\n\n\t\tqData : %+v\n", q)
		queue, err := ch.QueueDeclare(q.Name, q.Durable, q.DeleteWhenUsed, q.Exclusive, q.NoWait, nil)
		if err != nil {
			fmt.Printf("err: %v\n", err)
			return nil, err
		}
		qList = append(qList, queue)
	}
	return &RabbitRep{Exchange: &exchange, Connection: conn, Channel: ch, QueueInstance: qList}, nil
}

func Send(rabbit *RabbitRep, data string, ct string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// fmt.Printf("\n\n\t\teslami : %+v\n", rabbit)

	err := rabbit.Channel.PublishWithContext(ctx,
		rabbit.Exchange.Name,
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

func Receive(rabbit *RabbitRep, queueName string, function RabbitFunction) {
	msgs, _ := rabbit.Channel.Consume(
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
