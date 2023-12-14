package validators

import (
	"OrderProcessingService/proto"
	"errors"
	"fmt"

	"github.com/google/uuid"
)

func ValidateGetUserOrdersRequest(request *proto.GetUserOrdersRequest) error {
	if _, err := uuid.Parse(request.UserId); err != nil {
		return err
	}
	return nil
}

func ValidateCreateOrderRequest(request *proto.CreateOrderRequest) error {
	if _, err := uuid.Parse(request.UserId); err != nil {
		return err
	}
	if int(request.Pair) > len(proto.OrderPair_value)-1 || int(request.Pair) < 0 {
		return fmt.Errorf("invalid pair: %d", int(request.Pair))
	}
	if int(request.Type) > len(proto.OrderType_value)-1 || int(request.Type) < 0 {
		return fmt.Errorf("invalid type: %d", int(request.Type))
	}
	if int(request.Direction) > len(proto.OrderDirection_value)-1 || int(request.Direction) < 0 {
		return fmt.Errorf("invalid direction: %d", int(request.Direction))
	}
	if request.Price <= 0 && request.Type != proto.OrderType_MARKET {
		return errors.New("price must be greater than 0")
	}
	if request.Volume <= 0 {
		return errors.New("volume must be greater than 0")
	}
	return nil
}

func ValidateRemoveOrderRequest(request *proto.RemoveOrderRequest) error {
	if _, err := uuid.Parse(request.UserId); err != nil {
		return err
	}
	if _, err := uuid.Parse(request.OrderId); err != nil {
		return err
	}
	return nil
}
