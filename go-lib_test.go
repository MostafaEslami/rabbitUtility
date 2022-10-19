/**
 * Author: Mitch Allen
 * File: go-lib_test.go
 */

package rabbitUtils

import (
	"fmt"
	"testing"
)

func TestInitialize(t *testing.T) {
	qList := []RabbitQueue{
		RabbitQueue{
			name: "SMS",
		},
		RabbitQueue{
			name: "EMAIL",
		},
	}
	rabbit, err := InitializeRabbit("amqp://user:bitnami@localhost:5672/",
		RabbitExchange{name: "eslami", exchangeType: "fanout"},
		qList)

	if err != nil {
		t.Errorf("rabbit initialize failed")
	}
	rabbit.exchange = &RabbitExchange{name: "eslami", exchangeType: "fanout"}
	err = CreateExchange(rabbit)
	if err != nil {
		t.Errorf("Create exchange failed")
	}

	err = BoundQueueToExchange(rabbit, "")
	if err != nil {
		t.Errorf("Bound queue to exchange failed")
	}
}

func TestSend(t *testing.T) {
	qList := []RabbitQueue{
		RabbitQueue{
			name: "SMS",
		},
		RabbitQueue{
			name: "EMAIL",
		},
	}
	rabbit, err := InitializeRabbit("amqp://user:bitnami@localhost:5672/",
		RabbitExchange{name: "eslami", exchangeType: "fanout"},
		qList)

	if err != nil {
		t.Errorf("rabbit initialize failed")
		return
	}
	//err = Send(rabbit, "mostafa", "text/plain")
	body := "{name:arvind, message:hello}"
	err = Send(rabbit, body, "applicesion/json")

	if err != nil {
		t.Errorf("rabbit send failed")
		return
	}
}

func receiveFunction(data []byte) {
	fmt.Println("data received : ", string(data))
}
func TestReceive(t *testing.T) {
	qList := []RabbitQueue{
		RabbitQueue{
			name: "SMS",
		},
		RabbitQueue{
			name: "EMAIL",
		},
	}
	rabbit, err := InitializeRabbit("amqp://user:bitnami@localhost:5672/",
		RabbitExchange{name: "eslami", exchangeType: "fanout"},
		qList)

	if err != nil {
		t.Errorf("rabbit initialize failed")
		return
	}
	Receive(rabbit, "SMS", receiveFunction)
}
