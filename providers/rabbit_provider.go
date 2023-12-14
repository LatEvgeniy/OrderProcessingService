package providers

import (
	"OrderProcessingService/utils"

	"github.com/google/uuid"
	logger "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	googleProto "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

var (
	rabbitUrl = "amqp://guest:guest@rabbitmq:5672/"

	orderProcessingDeclaredExMsg   = "OrderProcessingService declared ex: %s"
	orderProcessingCreatedQueueMsg = "OrderProcessingService created queue: %s in ex: %s with rk: %s"
	orderProcessingGotResponseMsg  = "OrderProcessingService got response: %+v"
	orderProcessingSentMsg         = "OrderProcessingService sent message to ex: %s with rk: %s"
	successfullyDeletedQueueMsg    = "Successfully deleted queue: %s"
)

type RabbitProvider struct {
	Connection *amqp.Connection
}

func NewRabbitProvider() *RabbitProvider {
	conn, err := amqp.Dial(rabbitUrl)

	utils.CheckErrorWithPanic(err)
	rabbitProvider := &RabbitProvider{Connection: conn}

	return rabbitProvider
}

func (r *RabbitProvider) getNewChannel() *amqp.Channel {
	ch, err := r.Connection.Channel()
	utils.CheckErrorWithPanic(err)

	return ch
}

func (r *RabbitProvider) GetQueueConsumer(exName string, rk string, queueName string) (<-chan amqp.Delivery, *amqp.Channel) {
	ch := r.getNewChannel()
	_, err := ch.QueueDeclare(
		queueName,
		false,
		false,
		false,
		false,
		nil,
	)
	utils.CheckErrorWithPanic(err)

	err = ch.QueueBind(
		queueName,
		rk,
		exName,
		false,
		nil,
	)
	utils.CheckErrorWithPanic(err)

	msgs, err := ch.Consume(
		queueName,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	utils.CheckErrorWithPanic(err)

	logger.Infof(orderProcessingCreatedQueueMsg, queueName, exName, rk)
	return msgs, ch
}

func (r *RabbitProvider) DeclareExchange(exName string) {
	err := r.getNewChannel().ExchangeDeclare(
		exName,
		"topic",
		false,
		false,
		false,
		false,
		nil,
	)

	utils.CheckErrorWithPanic(err)
	logger.Infof(orderProcessingDeclaredExMsg, exName)
}

func (r *RabbitProvider) SendMessage(exName string, rk string, message []byte) {
	err := r.getNewChannel().Publish(
		exName,
		rk,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		},
	)
	utils.CheckErrorWithPanic(err)
	logger.Infof(orderProcessingSentMsg, exName, rk)
}

func (r *RabbitProvider) RunListener(msgs <-chan amqp.Delivery, ch *amqp.Channel, OrderEntrypointFunc func([]byte)) {
	defer ch.Close()

	forever := make(chan bool)
	go func() {
		for msg := range msgs {
			OrderEntrypointFunc(msg.Body)
		}
	}()

	<-forever
}

func (r *RabbitProvider) SendMessageAndHandleResponse(exName, rkSend, rkListen, queueName string, request []byte, response protoreflect.ProtoMessage) error {
	uniqueQueueName := queueName + "." + uuid.NewString()
	msgs, ch := r.GetQueueConsumer(exName, rkListen, uniqueQueueName)

	r.SendMessage(exName, rkSend, request)

	defer ch.Close()
	for msg := range msgs {
		err := googleProto.Unmarshal(msg.Body, response)
		if err != nil {
			return err
		}
		logger.Infof(orderProcessingGotResponseMsg, response)

		if _, err := ch.QueueDelete(uniqueQueueName, false, false, false); err != nil {
			return err
		}
		logger.Debugf(successfullyDeletedQueueMsg, uniqueQueueName)
		return nil
	}
	return nil
}
