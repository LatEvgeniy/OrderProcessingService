package processing

import (
	"OrderProcessingService/models"
	"OrderProcessingService/proto"
	"context"
	"encoding/json"
	"time"

	logger "github.com/sirupsen/logrus"
	googleProto "google.golang.org/protobuf/proto"
)

var (
	ordersIndexName = "order:*"

	removeOrderRequestRkName = "rk.RemoveOrderRequest"

	foundExpiredOrderMsg         = "Found expired order: %+v"
	sendingRemoveOrderRequestMsg = "Sending RemoveOrderRequest: %+v to delete expired order"

	jsonUnmarshalErrMsg = "Error while unmarshal json order: %s from redis to orderModel"
	getOrdersErrMsg     = "Error while get orders from redis with key: %s"
)

func (o *OrderProcessing) CheckExpiredOrdersBySchedule(checkExpiredOrdersScheduleTime time.Duration) {
	for {
		time.Sleep(checkExpiredOrdersScheduleTime)

		orderKeys, err := o.RedisClient.Keys(context.Background(), ordersIndexName).Result()
		if err != nil {
			logger.Errorf(getOrdersErrMsg, ordersIndexName)
			return
		}

		for _, orderKey := range orderKeys {
			jsonOrder, err := o.RedisClient.Get(context.Background(), orderKey).Result()
			if err != nil {
				logger.Errorf(getOrdersErrMsg, ordersIndexName)
				return
			}

			var orderModel models.OrderModel
			if err = json.Unmarshal([]byte(jsonOrder), &orderModel); err != nil {
				logger.Errorf(jsonUnmarshalErrMsg, jsonOrder)
				return
			}

			if time.Now().Before(orderModel.ExpirationDate) {
				continue
			}
			logger.Debugf(foundExpiredOrderMsg, orderModel)

			removeOrderRequest := &proto.RemoveOrderRequest{UserId: orderModel.UserId.String(), OrderId: orderModel.OrderId.String()}
			sendBody, _ := googleProto.Marshal(removeOrderRequest)

			logger.Infof(sendingRemoveOrderRequestMsg, removeOrderRequest)
			o.RabbitProvider.SendMessage(orderProcessingServiceExName, removeOrderRequestRkName, sendBody)
		}
	}
}
