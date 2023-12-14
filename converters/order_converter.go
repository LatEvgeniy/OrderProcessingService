package converters

import (
	"OrderProcessingService/models"
	"OrderProcessingService/proto"
	"fmt"

	"time"

	"github.com/google/uuid"
)

var (
	expireTime = 10 * time.Minute

	invalidOrderPairErrMsg      = "redis has invalid order pair: %s"
	invalidOrderDirectionErrMsg = "redis has invalid order direction: %s"
	invalidOrderTypeErrMsg      = "redis has invalid order type: %s"
)

func ConvertRequestToOrderShortInfoModel(request *proto.CreateOrderRequest, orderId *uuid.UUID) (*models.OrderShortInfoModel, error) {
	return &models.OrderShortInfoModel{
		OrderId:   *orderId,
		Pair:      request.Pair.String(),
		Direction: request.Direction.String(),
		Type:      request.Type.String(),
	}, nil
}

func ConverRequestToOrderModel(request *proto.CreateOrderRequest, lockedVolume float64) (*models.OrderModel, error) {
	orderId, err := uuid.NewUUID()
	if err != nil {
		return nil, err
	}

	userId, parseErr := uuid.Parse(request.UserId)
	if parseErr != nil {
		return nil, parseErr
	}

	initPrice := request.Price
	if request.Type == proto.OrderType_MARKET {
		initPrice = 0
	}

	return &models.OrderModel{
		OrderId:           orderId,
		UserId:            userId,
		Pair:              request.Pair.String(),
		Direction:         request.Direction.String(),
		Type:              request.Type.String(),
		InitVolume:        request.Volume,
		InitPrice:         initPrice,
		FilledPrice:       0,
		FilledVolume:      0,
		ExpirationDate:    time.Now().Add(expireTime),
		CreationDate:      time.Now(),
		UpdatedDate:       time.Now(),
		LockedUserBalance: lockedVolume,
	}, nil
}

func ConverOrderModelToProtoOrder(orderModel *models.OrderModel) (*proto.Order, error) {
	pairValue, exists := proto.OrderPair_value[orderModel.Pair]
	if !exists {
		return nil, fmt.Errorf(invalidOrderPairErrMsg, orderModel.Pair)
	}
	directionValue, exists := proto.OrderDirection_value[orderModel.Direction]
	if !exists {
		return nil, fmt.Errorf(invalidOrderDirectionErrMsg, orderModel.Direction)
	}
	typeValue, exists := proto.OrderType_value[orderModel.Type]
	if !exists {
		return nil, fmt.Errorf(invalidOrderTypeErrMsg, orderModel.Type)
	}

	return &proto.Order{
		UserId:         orderModel.UserId.String(),
		OrderId:        orderModel.OrderId.String(),
		Pair:           proto.OrderPair(pairValue),
		Direction:      proto.OrderDirection(directionValue),
		Type:           proto.OrderType(typeValue),
		InitVolume:     orderModel.InitVolume,
		InitPrice:      orderModel.InitPrice,
		FilledPrice:    orderModel.FilledPrice,
		FilledVolume:   orderModel.FilledVolume,
		LockedVolume:   orderModel.LockedUserBalance,
		ExpirationDate: orderModel.ExpirationDate.Unix(),
		CreationDate:   orderModel.CreationDate.Unix(),
		UpdatedDate:    orderModel.CreationDate.Unix(),
	}, nil
}

func ConvertOrderProtoToOrderModel(protoOrder *proto.Order) *models.OrderModel {
	orderId, _ := uuid.Parse(protoOrder.OrderId)
	userId, _ := uuid.Parse(protoOrder.UserId)

	return &models.OrderModel{
		OrderId:           orderId,
		UserId:            userId,
		Pair:              protoOrder.Pair.String(),
		Direction:         protoOrder.Direction.String(),
		Type:              protoOrder.Type.String(),
		InitVolume:        protoOrder.InitVolume,
		InitPrice:         protoOrder.InitPrice,
		FilledPrice:       protoOrder.FilledPrice,
		FilledVolume:      protoOrder.FilledVolume,
		ExpirationDate:    time.Unix(protoOrder.ExpirationDate, 0),
		CreationDate:      time.Unix(protoOrder.CreationDate, 0),
		UpdatedDate:       time.Unix(protoOrder.UpdatedDate, 0),
		LockedUserBalance: protoOrder.LockedVolume,
	}
}
