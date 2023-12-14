package processing

import (
	"OrderProcessingService/converters"
	"OrderProcessingService/models"
	"OrderProcessingService/proto"
	"context"
	"encoding/json"
	"fmt"

	logger "github.com/sirupsen/logrus"
)

var (
	userDoesntHaveOrderErrMsg = "User with id %s doesn`t have order with id %s"
	userDosntHaveOrdersErrMsg = "User with id %s doesn`t have any order"

	OrderProcessingReceivedModelMsg = "OrderProcessing recevied model: %+v"
)

func (o *OrderProcessing) GetUserOrders(userId string) ([]*proto.Order, error) {
	var orders []*proto.Order

	exists, err := o.RedisClient.Exists(context.Background(), "userid:"+userId).Result()
	if err != nil {
		return nil, err
	}
	if exists == 0 {
		return nil, fmt.Errorf(userDosntHaveOrdersErrMsg, userId)
	}

	userOrders, err := o.RedisClient.HGetAll(context.Background(), "userid:"+userId).Result()
	if err != nil {
		return nil, err
	}
	for _, orderKey := range userOrders {
		jsonStr, err := o.RedisClient.Get(context.Background(), orderKey).Result()
		if err != nil {
			return nil, err
		}

		var orderModel models.OrderModel
		if err = json.Unmarshal([]byte(jsonStr), &orderModel); err != nil {
			return nil, err
		}

		protoOrder, err := converters.ConverOrderModelToProtoOrder(&orderModel)
		if err != nil {
			return nil, err
		}

		orders = append(orders, protoOrder)
	}

	return orders, nil
}

func (o *OrderProcessing) GetUserOrder(userId, orderId string) (*models.OrderModel, error) {
	exists, err := o.RedisClient.HExists(context.Background(), "userid:"+userId, "orderid:"+orderId).Result()
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf(userDoesntHaveOrderErrMsg, userId, orderId)
	}

	orderKey, err := o.RedisClient.HGet(context.Background(), "userid:"+userId, "orderid:"+orderId).Result()
	if err != nil {
		return nil, err
	}

	jsonStr, err := o.RedisClient.Get(context.Background(), orderKey).Result()
	if err != nil {
		return nil, err
	}

	var orderModel models.OrderModel
	if err = json.Unmarshal([]byte(jsonStr), &orderModel); err != nil {
		return nil, err
	}

	logger.Infof(OrderProcessingReceivedModelMsg, orderModel)

	return &orderModel, err
}
