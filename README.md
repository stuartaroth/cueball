# cueball

*cueball* is a small abstraction layer for consuming and publishing messages to RabbitMQ. Internally it uses [github.com/streadway/amqp](https://github.com/streadway/amqp).

### Installation

Install the library using `go get`

```sh
$ go get github.com/stuartaroth/cueball
```

### Usage

[GoDoc](https://godoc.org/github.com/stuartaroth/cueball)

*cueball* requires clients to implement a simple interface:

[Cueball interface](https://godoc.org/github.com/stuartaroth/cueball#Cueball)

```go
type Cueball interface {
	Config() Config
	Handle(message Message) (map[string]Message, error)
}
```
[Config](https://godoc.org/github.com/stuartaroth/cueball#Config) is a struct that contains the RabbitMQ connection information, which includes which queues the program will consume and publish to. 

`Handle` is a function that will receive a message from the ConsumerQueue. It returns a map of string and Message, which which publish a Message to its corresponding string.

For an example of use, see below:

### Example Usage

You can find this code at https://github.com/stuartaroth/cueball/blob/master/examples/simple-client/main.go

```go
package main

import (
	"errors"
	"github.com/stuartaroth/cueball"
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

	return publishQueueMessages, nil
}

func main() {
	rc := CueballClient{}
	cueball.Start(rc)
}
```

### License

BSD-2 clause - see LICENSE for more details