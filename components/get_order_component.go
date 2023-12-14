package components

import (
	"OrderProcessingService/proto"

	logger "github.com/sirupsen/logrus"
	googleProto "google.golang.org/protobuf/proto"
)

var (
	getUserOrdersResponseRk = "rk.GetUserOrdersResponse"

	publishedGetUserOrdersResponseErrMsg = "OrderProcessingService published GetUserOrdersResponse error msg: %+v"
	publishedGetUserOrdersResponseMsg    = "OrderProcessingService published GetUserOrdersResponse msg: %+v"
)

func (o *OrderComponent) GetUserOrders(getUserOrdersRequest *proto.GetUserOrdersRequest, validationErr error) {
	if validationErr != nil {
		o.sendGetUserOrdersErrResponse(&proto.ErrorDto{Code: proto.ErrorCode_ERROR_INVALID_REQUEST, Message: validationErr.Error()})
		return
	}

	orders, err := o.OrderProcessing.GetUserOrders(getUserOrdersRequest.UserId)
	if err != nil {
		o.sendGetUserOrdersErrResponse(&proto.ErrorDto{Code: proto.ErrorCode_ERROR_REDIS_PROCESSING, Message: err.Error()})
		return
	}
	getUserOrdersResponse := proto.GetUserOrdersResponse{Orders: orders}
	sendBody, err := googleProto.Marshal(&getUserOrdersResponse)
	if err != nil {
		o.sendGetUserOrdersErrResponse(&proto.ErrorDto{Code: proto.ErrorCode_ERROR_INTERNAL, Message: err.Error()})
		return
	}

	logger.Infof(publishedGetUserOrdersResponseMsg, getUserOrdersResponse.String())
	o.RabbitProvider.SendMessage(orderProcessingServiceExName, getUserOrdersResponseRk, sendBody)
}

func (o *OrderComponent) sendGetUserOrdersErrResponse(err *proto.ErrorDto) {
	getUserOrdersErrResponse := &proto.GetUserOrdersResponse{Error: err}
	logger.Errorf(publishedGetUserOrdersResponseErrMsg, getUserOrdersErrResponse)
	sendBody, _ := googleProto.Marshal(getUserOrdersErrResponse)
	o.RabbitProvider.SendMessage(orderProcessingServiceExName, getUserOrdersResponseRk, sendBody)
}
