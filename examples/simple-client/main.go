package main

import (
	"errors"
	"github.com/stuartaroth/cueball"
	"time"
)

var (
	NameQueue       = "cueball-names"
	HelloQueue      = "cueball-hello"
	GoodbyeQueue    = "cueball-goodbye"
	DeadLetterQueue = "cueball-names-dead-letter"
)

type CueballClient struct{}

func (rc CueballClient) Config() cueball.Config {
	return cueball.Config{
		Uri:           "amqp://guest:guest@localhost:5672/",
		Exchange:      "simple-cueball-exchange",
		ExchangeType:  "direct",
		ConsumerQueue: NameQueue,
		PublisherQueues: []string{
			HelloQueue,
			GoodbyeQueue,
		},
		DeadLetterQueue: DeadLetterQueue,
		BindingKey:      "simple-cueball-key",
		ConsumerTag:     "simple-consumer",
		Debug:           true,
		ExitAfterEveryMessage: false,
		ExitAfterXMinutesIdle: 1,
	}
}

func (rc CueballClient) Handle(message cueball.Message) (map[string]cueball.Message, error) {
	name := string(message.Body)

	if name == "Deadletter" {
		return map[string]cueball.Message{}, errors.New("Received name that cannot be handled")
	}

	helloMessage := cueball.Message{
		ContentType:     message.ContentType,
		ContentEncoding: message.ContentEncoding,
		DeliveryMode:    cueball.DeliveryModeNonPersistent,
		Priority:        message.Priority,
		Body:            []byte("Hello " + name),
	}

	goodbyeMessage := cueball.Message{
		ContentType:     message.ContentType,
		ContentEncoding: message.ContentEncoding,
		DeliveryMode:    cueball.DeliveryModeNonPersistent,
		Priority:        message.Priority,
		Body:            []byte("Goodbye " + name),
	}

	publishQueueMessages := map[string]cueball.Message{
		HelloQueue:   helloMessage,
		GoodbyeQueue: goodbyeMessage,
	}

	time.Sleep(5 * time.Second)

	return publishQueueMessages, nil
}

func main() {
	rc := CueballClient{}
	cueball.Start(rc)
}
