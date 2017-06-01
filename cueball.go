package cueball

import (
	"encoding/json"
	"fmt"
	"github.com/streadway/amqp"
	"log"
)

// Cueball is the interface that must be implemented to `Start` using this RabbitMQ abstraction.
// Config() returns a cueball.Config.
// Handle(message Message) receives a cueball.Message and returns
// a map[string]cueball.Message and an error.
// If the error is unequal to nil, the original message will be published to the specified DeadLetterQueue.
// The map[string]cueball.Message should reflect the string name of the queue the corresponding message
// should be published to.
type Cueball interface {
	Config() Config
	Handle(message Message) (map[string]Message, error)
}

// Config manages the RabbitMQ connection data.
// The Exchange, ConsumerQueue, PublisherQueues, and DeadLetterQueue are all
// declared in RabbitMQ if they don't exist.
type Config struct {
	Uri             string   `json:"uri"`
	Exchange        string   `json:"exchange"`
	ExchangeType    string   `json:"exchangeType"`
	ConsumerQueue   string   `json:"consumerQueue"`
	PublisherQueues []string `json:"publisherQueues"`
	DeadLetterQueue string   `json:"deadLetterQueue"`
	BindingKey      string   `json:"bindingKey"`
	ConsumerTag     string   `json:"consumerTag"`
	Debug           bool     `json:"debug"`
}

// Message contains the data from the RabbitMQ message.
// DeliveryMode can be non-persistent (1) or persistent (2).
// Priority must be a value in range from (0) to (9).
type Message struct {
	ContentType     string
	ContentEncoding string
	DeliveryMode    uint8
	Priority        uint8
	Body            []byte
}

// Start requires a struct that implements the `Cueball` interface.
// It will create the exchanges and queues as needed and then listen forever.
func Start(cueball Cueball) {
	publisherChannelConfirmations, err := getPublisherChannelConfirmations(cueball)
	if err != nil {
		log.Println("Error getting publisherChannelConfirmations:", err)
		return
	}

	deadLetterChannel, err := getDeadLetterChannel(cueball)
	if err != nil {
		log.Println("Error getting deadLetterChannel:", err)
		return
	}

	messages, err := getMessages(cueball)
	if err != nil {
		log.Println("Error getting messages:", err)
		return
	}

	go handle(cueball, messages, publisherChannelConfirmations, deadLetterChannel)
	select {}
}

var (
	DeliveryModeNonPersistent = uint8(1)
	DeliveryModePersistent    = uint8(2)
)

type channelConfirmations struct {
	Channel       *amqp.Channel
	Confirmations <-chan amqp.Confirmation
}

func publishMessageToDeadLetter(message amqp.Delivery, deadLetterChannel *amqp.Channel, deadLetterQueue string) {
	err := deadLetterChannel.Publish("", deadLetterQueue, false, false, amqp.Publishing{
		Headers:         amqp.Table{},
		ContentType:     message.ContentType,
		ContentEncoding: message.ContentEncoding,
		Body:            message.Body,
		DeliveryMode:    message.DeliveryMode,
		Priority:        message.Priority,
	})
	if err != nil {
		log.Println("Error in deadLetterChannel publish:", err)
		bits, err := json.Marshal(message)
		if err == nil {
			log.Println("Message that failed to publish to deadLetterChannel:", string(bits))
		}
	}
}

func printMessage(config Config, message amqp.Delivery) {
	if config.Debug {
		log.Println("Received message from RabbitMQ:", fmt.Sprintf(`{queue='%v', body='%v'}`, config.ConsumerQueue, string(message.Body)))
	}
}

func printPublishQueueMessages(config Config, messages map[string]Message) {
	if config.Debug {
		stringMessages := []string{}
		for key, message := range messages {
			stringMessages = append(stringMessages, fmt.Sprintf(`{queue='%v', body='%v'}`, key, string(message.Body)))
		}

		log.Println("Received messages to write to RabbitMQ:", stringMessages)
	}
}

func handle(cueball Cueball, messages <-chan amqp.Delivery, publisherChannelConfirmations map[string]channelConfirmations, deadLetterChannel *amqp.Channel) {
	config := cueball.Config()

	for message := range messages {
		printMessage(config, message)

		publishQueueMessages, err := cueball.Handle(convertMessage(message))
		if err != nil {
			log.Println("Error in Handle function:", err)
			publishMessageToDeadLetter(message, deadLetterChannel, config.DeadLetterQueue)
			message.Ack(false)
			continue
		}

		printPublishQueueMessages(config, publishQueueMessages)

		publishFailure := false
		ackFailure := false

		for publishQueue, publishMessage := range publishQueueMessages {
			err = publisherChannelConfirmations[publishQueue].Channel.Publish("", publishQueue, false, false, amqp.Publishing{
				Headers:         amqp.Table{},
				ContentType:     publishMessage.ContentType,
				ContentEncoding: publishMessage.ContentEncoding,
				Body:            publishMessage.Body,
				DeliveryMode:    publishMessage.DeliveryMode,
				Priority:        publishMessage.Priority,
			})
			if err != nil {
				log.Println("Error in publishing message:", err)
				publishFailure = true
			}

			confirmed := <-publisherChannelConfirmations[publishQueue].Confirmations
			if !confirmed.Ack {
				ackFailure = true
			}
		}

		if publishFailure || ackFailure {
			publishMessageToDeadLetter(message, deadLetterChannel, config.DeadLetterQueue)
		}

		message.Ack(false)
	}
}

func convertMessage(message amqp.Delivery) Message {
	return Message{
		ContentType:     message.ContentType,
		ContentEncoding: message.ContentEncoding,
		DeliveryMode:    message.DeliveryMode,
		Priority:        message.Priority,
		Body:            message.Body,
	}
}

func getMessages(cueball Cueball) (<-chan amqp.Delivery, error) {
	config := cueball.Config()

	connection, err := amqp.Dial(config.Uri)
	if err != nil {
		return nil, err
	}

	channel, err := connection.Channel()
	if err != nil {
		return nil, err
	}

	err = channel.ExchangeDeclare(
		config.Exchange,
		config.ExchangeType,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	queue, err := channel.QueueDeclare(
		config.ConsumerQueue,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	err = channel.QueueBind(
		queue.Name,
		config.BindingKey,
		config.Exchange,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	messages, err := channel.Consume(
		queue.Name,
		config.ConsumerTag,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	return messages, nil
}

func getPublisherChannelConfirmations(cueball Cueball) (map[string]channelConfirmations, error) {
	config := cueball.Config()

	mappy := make(map[string]channelConfirmations)

	for _, publisherQueue := range config.PublisherQueues {
		connection, err := amqp.Dial(config.Uri)
		if err != nil {
			return mappy, err
		}

		channel, err := connection.Channel()
		if err != nil {
			return mappy, err
		}

		err = channel.ExchangeDeclare(
			config.Exchange,
			config.ExchangeType,
			true,
			false,
			false,
			false,
			nil,
		)
		if err != nil {
			return mappy, err
		}

		queue, err := channel.QueueDeclare(
			publisherQueue,
			true,
			false,
			false,
			false,
			nil,
		)
		if err != nil {
			return mappy, err
		}

		err = channel.Confirm(false)
		if err != nil {
			return mappy, err
		}

		confirmations := channel.NotifyPublish(make(chan amqp.Confirmation, 1))

		mappy[queue.Name] = channelConfirmations{channel, confirmations}
	}

	return mappy, nil
}

func getDeadLetterChannel(cueball Cueball) (*amqp.Channel, error) {
	config := cueball.Config()

	connection, err := amqp.Dial(config.Uri)
	if err != nil {
		return nil, err
	}

	channel, err := connection.Channel()
	if err != nil {
		return nil, err
	}

	err = channel.ExchangeDeclare(
		config.Exchange,
		config.ExchangeType,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	_, err = channel.QueueDeclare(
		config.DeadLetterQueue,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	return channel, nil
}
