package entrypoints

import (
	"OrderProcessingService/proto"
	"OrderProcessingService/validators"

	logger "github.com/sirupsen/logrus"
	googleProto "google.golang.org/protobuf/proto"
)

var (
	orderProcessingGotRemoveOrderRequestMsg = "OrderProcessingService got RemoveOrderRequest: %s\n"
)

func (e *OrderEntrypoint) RemoveOrder(byteRequest []byte) {
	var removeOrderRequest proto.RemoveOrderRequest
	if err := googleProto.Unmarshal(byteRequest, &removeOrderRequest); err != nil {
		e.OrderComponent.RemoveOrder(nil, err)
		return
	}

	logger.Infof(orderProcessingGotRemoveOrderRequestMsg, removeOrderRequest.String())

	if err := validators.ValidateRemoveOrderRequest(&removeOrderRequest); err != nil {
		e.OrderComponent.RemoveOrder(nil, err)
		return
	}

	e.OrderComponent.RemoveOrder(&removeOrderRequest, nil)
}
