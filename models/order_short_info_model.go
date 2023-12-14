package models

import (
	"fmt"

	"github.com/google/uuid"
)

type OrderShortInfoModel struct {
	OrderId   uuid.UUID `json:"orderId"`
	Pair      string    `json:"pair"`
	Direction string    `json:"direction"`
	Type      string    `json:"type"`
}

func (o *OrderShortInfoModel) String() string {
	return fmt.Sprintf("order:%s:%s:%s:%s", o.Pair, o.Direction, o.Type, o.OrderId)
}
