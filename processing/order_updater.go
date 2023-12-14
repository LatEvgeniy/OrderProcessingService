package processing

import (
	"context"
)

func (o *OrderProcessing) SetOrder(key string, orderJson []byte) error {
	if err := o.RedisClient.Set(context.Background(), key, orderJson, 0).Err(); err != nil {
		return err
	}
	return nil
}
