package entrypoints

import (
	"OrderProcessingService/proto"
	"OrderProcessingService/validators"

	logger "github.com/sirupsen/logrus"
	googleProto "google.golang.org/protobuf/proto"
)

var (
	orderProcessingGotGetUserOrdersRequestMsg = "OrderProcessingService got GetUserOrdersRequest: %s\n"
)

func (e *OrderEntrypoint) GetUserOrders(byteRequest []byte) {
	var getUserOrdersRequest proto.GetUserOrdersRequest
	if err := googleProto.Unmarshal(byteRequest, &getUserOrdersRequest); err != nil {
		e.OrderComponent.GetUserOrders(nil, err)
		return
	}

	logger.Infof(orderProcessingGotGetUserOrdersRequestMsg, getUserOrdersRequest.String())

	if err := validators.ValidateGetUserOrdersRequest(&getUserOrdersRequest); err != nil {
		e.OrderComponent.GetUserOrders(nil, err)
		return
	}

	e.OrderComponent.GetUserOrders(&getUserOrdersRequest, nil)
}
