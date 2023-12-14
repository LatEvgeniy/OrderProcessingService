package processing

import (
	"OrderProcessingService/converters"
	"OrderProcessingService/models"
	"OrderProcessingService/proto"
	"OrderProcessingService/providers"
	"context"
	"encoding/json"

	logger "github.com/sirupsen/logrus"
)

var (
	successfullyCreateOrderMsg = "Successfully created order: %+v"
)

func (o *OrderProcessing) CreateOrder(request *proto.CreateOrderRequest, lockedUserBalance float64) (*models.OrderModel, error) {
	redisClient := providers.NewRedisClient()
	defer redisClient.Close()

	orderModel, err := converters.ConverRequestToOrderModel(request, lockedUserBalance)
	if err != nil {
		return nil, err
	}

	if request.Type == proto.OrderType_MARKET {
		return orderModel, nil
	}

	orderShortInfoModel, err := converters.ConvertRequestToOrderShortInfoModel(request, &orderModel.OrderId)
	if err != nil {
		return nil, err
	}

	if err := o.addOrderShortInfoModel(orderShortInfoModel, orderModel.UserId.String()); err != nil {
		return nil, err
	}

	if err := o.addOrderModel(orderModel); err != nil {
		return nil, err
	}

	if err := o.RedisClient.HIncrByFloat(context.Background(), availableForMatchVolumeIndexName, "orderid:"+orderModel.OrderId.String(), orderModel.InitVolume).Err(); err != nil {
		return nil, err
	}

	logger.Infof(successfullyCreateOrderMsg, orderModel)
	return orderModel, nil
}

func (o *OrderProcessing) addOrderShortInfoModel(orderShortInfoModel *models.OrderShortInfoModel, userId string) error {
	if err := o.RedisClient.HMSet(context.Background(), "userid:"+userId, map[string]interface{}{
		"orderid:" + orderShortInfoModel.OrderId.String(): orderShortInfoModel.String(),
	}).Err(); err != nil {
		return err
	}

	return nil
}

func (o *OrderProcessing) addOrderModel(orderModel *models.OrderModel) error {
	orderJson, err := json.Marshal(orderModel)
	if err != nil {
		return err
	}

	if err = o.RedisClient.Set(context.Background(), orderModel.GetKey(), orderJson, 0).Err(); err != nil {
		return err
	}

	return nil
}
