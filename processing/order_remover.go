package processing

import (
	"OrderProcessingService/proto"
	"context"
)

func (o *OrderProcessing) RemoveOrder(request *proto.RemoveOrderRequest) error {
	orderKey, err := o.RedisClient.HGet(context.Background(), "userid:"+request.UserId, "orderid:"+request.OrderId).Result()
	if err != nil {
		return err
	}

	if err := o.RedisClient.Del(context.Background(), orderKey).Err(); err != nil {
		return err
	}
	if err := o.RedisClient.HDel(context.Background(), "userid:"+request.UserId, "orderid:"+request.OrderId).Err(); err != nil {
		return err
	}
	if err := o.RedisClient.HDel(context.Background(), availableForMatchVolumeIndexName, "orderid:"+request.OrderId).Err(); err != nil {
		return err
	}

	return nil
}
