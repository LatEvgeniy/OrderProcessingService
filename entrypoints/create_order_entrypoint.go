package entrypoints

import (
	"OrderProcessingService/components"
	"OrderProcessingService/proto"
	"OrderProcessingService/validators"

	logger "github.com/sirupsen/logrus"
	googleProto "google.golang.org/protobuf/proto"
)

var (
	orderProcessingGotCreateOrderRequestMsg = "OrderProcessingService got CreateOrderRequest: %s\n"
)

type OrderEntrypoint struct {
	OrderComponent *components.OrderComponent
}

func (e *OrderEntrypoint) CreateOrder(byteRequest []byte) {
	var createOrderRequest proto.CreateOrderRequest

	if err := googleProto.Unmarshal(byteRequest, &createOrderRequest); err != nil {
		e.OrderComponent.CreateOrder(nil, err)
		return
	}

	logger.Infof(orderProcessingGotCreateOrderRequestMsg, createOrderRequest.String())

	if err := validators.ValidateCreateOrderRequest(&createOrderRequest); err != nil {
		e.OrderComponent.CreateOrder(nil, err)
		return
	}

	e.OrderComponent.CreateOrder(&createOrderRequest, nil)
}
