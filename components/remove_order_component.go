package components

import (
	"OrderProcessingService/converters"
	"OrderProcessingService/proto"
	"OrderProcessingService/utils"

	logger "github.com/sirupsen/logrus"
	googleProto "google.golang.org/protobuf/proto"
)

var (
	RemoveOrderResponseRk = "rk.RemoveOrderResponse"

	publishedRemoveOrderResponseErrMsg = "OrderProcessingService published RemoveOrderResponse error msg: %+v"
	publishedRemoveOrderResponseMsg    = "OrderProcessingService published RemoveOrderResponse msg: %+v"
)

func (o *OrderComponent) RemoveOrder(request *proto.RemoveOrderRequest, validationErr error) {
	if validationErr != nil {
		o.sendRemoveOrderErrResponse(&proto.ErrorDto{Code: proto.ErrorCode_ERROR_INVALID_REQUEST, Message: validationErr.Error()})
		return
	}

	removeOrderModel, err := o.OrderProcessing.GetUserOrder(request.UserId, request.OrderId)
	if err != nil {
		o.sendRemoveOrderErrResponse(&proto.ErrorDto{Code: proto.ErrorCode_ERROR_REDIS_PROCESSING, Message: err.Error()})
		return
	}

	if removeOrderModel.LockedUserBalance != 0 {
		o.unlockUserBalance(request.UserId, removeOrderModel.Direction, removeOrderModel.Pair, removeOrderModel.LockedUserBalance)
	}

	if err := o.OrderProcessing.RemoveOrder(request); err != nil {
		o.sendRemoveOrderErrResponse(&proto.ErrorDto{Code: proto.ErrorCode_ERROR_INTERNAL, Message: err.Error()})
		return
	}

	protoRemoveOrder, err := converters.ConverOrderModelToProtoOrder(removeOrderModel)
	if err != nil {
		o.sendRemoveOrderErrResponse(&proto.ErrorDto{Code: proto.ErrorCode_ERROR_INTERNAL, Message: err.Error()})
		return
	}

	removeOrderResponse := proto.RemoveOrderResponse{RemovedOrder: protoRemoveOrder}
	sendBody, err := googleProto.Marshal(&removeOrderResponse)
	if err != nil {
		o.sendGetUserOrdersErrResponse(&proto.ErrorDto{Code: proto.ErrorCode_ERROR_INTERNAL, Message: err.Error()})
		return
	}
	logger.Infof(publishedRemoveOrderResponseMsg, removeOrderResponse.String())
	o.RabbitProvider.SendMessage(orderProcessingServiceExName, RemoveOrderResponseRk, sendBody)
}

func (o *OrderComponent) sendRemoveOrderErrResponse(err *proto.ErrorDto) {
	removeOrderErrResponse := &proto.RemoveOrderResponse{Error: err}
	logger.Errorf(publishedRemoveOrderResponseErrMsg, removeOrderErrResponse)
	sendBody, _ := googleProto.Marshal(removeOrderErrResponse)
	o.RabbitProvider.SendMessage(orderProcessingServiceExName, RemoveOrderResponseRk, sendBody)
}

func (o *OrderComponent) unlockUserBalance(userId, direction, pair string, volume float64) {
	unlockBalanceRequest, err := o.getUnlockBalanceRequest(userId, direction, pair, volume)
	if err != nil {
		o.sendRemoveOrderErrResponse(&proto.ErrorDto{Code: proto.ErrorCode_ERROR_INTERNAL, Message: err.Error()})
		return
	}
	if err := o.OrderProcessing.UnlockBalance(unlockBalanceRequest); err != nil {
		o.sendRemoveOrderErrResponse(err)
		return
	}
}

func (o *OrderComponent) getUnlockBalanceRequest(userId, direction, pair string, volume float64) (*proto.UnlockBalanceRequest, error) {
	currency, err := utils.GetProtoCurrency(direction, pair)
	if err != nil {
		return nil, err
	}
	return &proto.UnlockBalanceRequest{
		UserId:   userId,
		Volume:   volume,
		Currency: currency,
	}, nil
}
