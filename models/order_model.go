package models

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

type OrderModel struct {
	OrderId   uuid.UUID `json:"orderId"`
	UserId    uuid.UUID `json:"userid"`
	Pair      string    `json:"pair"`
	Direction string    `json:"direction"`
	Type      string    `json:"type"`

	InitVolume        float64   `json:"initVolume"`
	InitPrice         float64   `json:"initPrice"`
	FilledPrice       float64   `json:"filledPrice"`
	FilledVolume      float64   `json:"filledVolume"`
	LockedUserBalance float64   `json:"lockedUserBalance"`
	ExpirationDate    time.Time `json:"expirationDate"`
	CreationDate      time.Time `json:"creationDate"`
	UpdatedDate       time.Time `json:"updatedDate"`
}

func (o *OrderModel) GetKey() string {
	return fmt.Sprintf("order:%s:%s:%s:%s", o.Pair, o.Direction, o.Type, o.OrderId.String())
}
