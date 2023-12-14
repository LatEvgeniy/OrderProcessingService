package components

import (
	"OrderProcessingService/converters"
	"OrderProcessingService/processing"
	"OrderProcessingService/proto"
	"OrderProcessingService/providers"
	"OrderProcessingService/utils"

	logger "github.com/sirupsen/logrus"
	googleProto "google.golang.org/protobuf/proto"
)

var (
	orderProcessingServiceExName = "ex.OrderProcessingService"

	createOrderReponseRk = "rk.CreateOrderResponse"

	publishedCreateOrderResponseErrMsg = "OrderProcessingService published CreateOrderResponse error msg: %+v"
	publishedCreateOrderResponseMsg    = "OrderProcessingService published CreateOrderResponse msg: %+v"
)

type OrderComponent struct {
	RabbitProvider  *providers.RabbitProvider
	OrderProcessing *processing.OrderProcessing
}

func (o *OrderComponent) CreateOrder(createOrderRequest *proto.CreateOrderRequest, validationErr error) {
	if validationErr != nil {
		o.sendCreateOrderErrResponse(&proto.ErrorDto{Code: proto.ErrorCode_ERROR_INVALID_REQUEST, Message: validationErr.Error()})
		return
	}

	var lockedUserBalance float64
	var errDto *proto.ErrorDto
	lockBalanceRequest, err := o.getLockBalanceRequest(createOrderRequest)
	if err != nil {
		return
	}
	if createOrderRequest.Type == proto.OrderType_LIMIT {
		lockedUserBalance, errDto = o.OrderProcessing.LockBalance(lockBalanceRequest)
		if errDto != nil {
			o.sendCreateOrderErrResponse(errDto)
			return
		}
	}

	createdOrderModel, err := o.OrderProcessing.CreateOrder(createOrderRequest, lockedUserBalance)
	if err != nil {
		o.sendCreateOrderErrResponse(&proto.ErrorDto{Code: proto.ErrorCode_ERROR_REDIS_PROCESSING, Message: err.Error()})
		return
	}
	createdOrderProto, err := converters.ConverOrderModelToProtoOrder(createdOrderModel)
	if err != nil {
		o.sendCreateOrderErrResponse(&proto.ErrorDto{Code: proto.ErrorCode_ERROR_INTERNAL, Message: err.Error()})
		return
	}
	createOrderResponse := &proto.CreateOrderResponse{CreatedOrder: createdOrderProto}
	sendBody, err := googleProto.Marshal(createOrderResponse)
	if err != nil {
		o.sendCreateOrderErrResponse(&proto.ErrorDto{Code: proto.ErrorCode_ERROR_INTERNAL, Message: err.Error()})
		return
	}

	logger.Infof(publishedCreateOrderResponseMsg, createOrderResponse)
	o.RabbitProvider.SendMessage(orderProcessingServiceExName, createOrderReponseRk, sendBody)
}

func (o *OrderComponent) sendCreateOrderErrResponse(err *proto.ErrorDto) {
	createOrderErrResponse := &proto.CreateOrderResponse{Error: err}
	logger.Errorf(publishedCreateOrderResponseErrMsg, createOrderErrResponse)
	sendBody, _ := googleProto.Marshal(createOrderErrResponse)
	o.RabbitProvider.SendMessage(orderProcessingServiceExName, createOrderReponseRk, sendBody)
}

func (o *OrderComponent) getLockBalanceRequest(createOrderRequest *proto.CreateOrderRequest) (*proto.LockBalanceRequest, error) {
	currency, err := utils.GetProtoCurrency(createOrderRequest.Direction.String(), createOrderRequest.Pair.String())
	if err != nil {
		return nil, err
	}
	lockBalanceRequest := proto.LockBalanceRequest{
		UserId:   createOrderRequest.UserId,
		Currency: currency,
	}

	if createOrderRequest.Direction == proto.OrderDirection_SELL {
		lockBalanceRequest.Volume = createOrderRequest.Volume
		return &lockBalanceRequest, nil
	}
	lockBalanceRequest.Volume = createOrderRequest.Volume * createOrderRequest.Price
	return &lockBalanceRequest, nil
}
