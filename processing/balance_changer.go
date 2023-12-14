package processing

import (
	"OrderProcessingService/converters"
	"OrderProcessingService/models"
	"OrderProcessingService/proto"
	"OrderProcessingService/providers"
	"OrderProcessingService/utils"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
	logger "github.com/sirupsen/logrus"
	googleProto "google.golang.org/protobuf/proto"
)

var (
	balanceServiceExchangeName = "ex.BalanceService"

	lockBalanceResponseListenerQueueName = "q.OrderProcessingService.LockBalanceResponse.Listener"
	lockBalanceRequestRk                 = "rk.LockBalanceRequest"
	lockBalanceReponseRk                 = "rk.LockBalanceResponse"

	unlockBalanceResponseListenerQueueName = "q.OrderProcessingService.UnlockBalanceResponse.Listener"
	unlockBalanceRequestRk                 = "rk.UnlockBalanceRequest"
	unlockBalanceReponseRk                 = "rk.UnlockBalanceResponse"

	sendingLockBalanceRequestMsg   = "Sending LockBalanceRequest: %+v"
	sendingUnlockBalanceRequestMsg = "Sending UnlockBalanceRequest: %+v"
	sendingSubtractMoneyRequestMsg = "Sending SubtractMoneyRequest: %+v"
	sendingAddMoneyRequestMsg      = "Sending AddMoneyRequest: %+v"
	updateOrderMsg                 = "Update order: %+v"

	startingChangeBalanceForCreatedOrderMsg = "%s:Starting change user balance for CreatedMatchOrderModel"
	startingChangeBalanceForMatchOrderMsg   = "%s:Starting change user balance for OrderForMatch"

	publishedMatchOrdersEventErrMsg = "OrderProcessingService published MatchOrdersEvent error msg: %+v"
	publishedMatchOrdersEventMsg    = "OrderProcessingService published MatchOrdersEvent msg: %+v"
)

type OrderProcessing struct {
	RabbitProvider *providers.RabbitProvider
	RedisClient    *redis.Client
}

func (o *OrderProcessing) LockBalance(lockBalanceRequest *proto.LockBalanceRequest) (float64, *proto.ErrorDto) {
	sendBody, marshalErr := googleProto.Marshal(lockBalanceRequest)
	if marshalErr != nil {
		return 0, &proto.ErrorDto{Code: proto.ErrorCode_ERROR_INTERNAL, Message: marshalErr.Error()}
	}

	logger.Infof(sendingLockBalanceRequestMsg, lockBalanceRequest.String())
	var lockBalanceResponse proto.LockBalanceResponse
	if err := o.RabbitProvider.SendMessageAndHandleResponse(balanceServiceExchangeName, lockBalanceRequestRk,
		lockBalanceReponseRk, lockBalanceResponseListenerQueueName, sendBody, &lockBalanceResponse); err != nil {
		return 0, &proto.ErrorDto{Code: proto.ErrorCode_ERROR_INTERNAL, Message: err.Error()}
	}

	if lockBalanceResponse.Error != nil {
		return 0, lockBalanceResponse.Error
	}

	return lockBalanceRequest.Volume, nil
}

func (o *OrderProcessing) UnlockBalance(unlockBalanceRequest *proto.UnlockBalanceRequest) *proto.ErrorDto {
	sendBody, marshalErr := googleProto.Marshal(unlockBalanceRequest)
	if marshalErr != nil {
		return &proto.ErrorDto{Code: proto.ErrorCode_ERROR_INTERNAL, Message: marshalErr.Error()}
	}

	logger.Infof(sendingUnlockBalanceRequestMsg, unlockBalanceRequest.String())
	var unlockBalanceResponse proto.UnlockBalanceResponse
	if err := o.RabbitProvider.SendMessageAndHandleResponse(balanceServiceExchangeName, unlockBalanceRequestRk,
		unlockBalanceReponseRk, unlockBalanceResponseListenerQueueName, sendBody, &unlockBalanceResponse); err != nil {
		return &proto.ErrorDto{Code: proto.ErrorCode_ERROR_INTERNAL, Message: err.Error()}
	}

	if unlockBalanceResponse.Error != nil {
		return unlockBalanceResponse.Error
	}

	return nil
}

// ------------ Change balance after find matched orders ------------

func (o *OrderProcessing) matchOrders(createdMatchOrderModel *models.OrderModel, orderForMatch *models.OrderModel, matchedVolume float64, err error) {
	if err != nil {
		o.sendMatchOrdersErrResponse(&proto.ErrorDto{Code: proto.ErrorCode_ERROR_INTERNAL, Message: err.Error()})
		return
	}

	logger.Debugf(startingChangeBalanceForCreatedOrderMsg, createdMatchOrderModel.OrderId)
	if err := o.changeUserBalance(createdMatchOrderModel, matchedVolume, orderForMatch.InitPrice); err != nil {
		o.sendMatchOrdersErrResponse(err)
	}

	logger.Debugf(startingChangeBalanceForMatchOrderMsg, createdMatchOrderModel.OrderId)
	if err := o.changeUserBalance(orderForMatch, matchedVolume, orderForMatch.InitPrice); err != nil {
		o.sendMatchOrdersErrResponse(err)
	}

	if err = o.updateOrders(createdMatchOrderModel, orderForMatch, matchedVolume); err != nil {
		o.sendMatchOrdersErrResponse(&proto.ErrorDto{Code: proto.ErrorCode_ERROR_INTERNAL, Message: err.Error()})
		return
	}

	matchOrdersEvent, err := o.getMatchOrdersEvent(*createdMatchOrderModel, *orderForMatch, matchedVolume)
	if err != nil {
		o.sendMatchOrdersErrResponse(&proto.ErrorDto{Code: proto.ErrorCode_ERROR_INTERNAL, Message: err.Error()})
		return
	}

	sendBody, err := googleProto.Marshal(matchOrdersEvent)
	if err != nil {
		o.sendMatchOrdersErrResponse(&proto.ErrorDto{Code: proto.ErrorCode_ERROR_INTERNAL, Message: err.Error()})
		return
	}
	logger.Infof(publishedMatchOrdersEventMsg, matchOrdersEvent.String())
	o.RabbitProvider.SendMessage(orderProcessingServiceExName, matchOrdersEventRk, sendBody)
}

func (o *OrderProcessing) changeUserBalance(order *models.OrderModel, matchedVolume, matchedPrice float64) *proto.ErrorDto {
	if err := o.unlockBalance(order, matchedVolume, matchedPrice); err != nil {
		return err
	}

	if err := o.subtractMoney(order, matchedVolume, matchedPrice); err != nil {
		return err
	}

	if err := o.addMoney(order, matchedVolume, matchedPrice); err != nil {
		return err
	}

	return nil
}

func (o *OrderProcessing) unlockBalance(order *models.OrderModel, matchedVolume, matchedPrice float64) *proto.ErrorDto {
	firstCurrency, secondCurrency, err := utils.GetProtoCurrenciesByPair(order.Pair)
	if err != nil {
		return &proto.ErrorDto{Code: proto.ErrorCode_ERROR_INTERNAL, Message: err.Error()}
	}
	unlockBalanceRequest := &proto.UnlockBalanceRequest{
		UserId:   order.UserId.String(),
		Volume:   matchedVolume,
		Currency: firstCurrency,
	}
	if order.Direction == proto.OrderDirection_BUY.String() {
		unlockBalanceRequest.Volume = matchedVolume * matchedPrice
		unlockBalanceRequest.Currency = secondCurrency
	}

	if err := o.UnlockBalance(unlockBalanceRequest); err != nil {
		return err
	}

	return nil
}

func (o *OrderProcessing) subtractMoney(order *models.OrderModel, matchedVolume, matchedPrice float64) *proto.ErrorDto {
	firstCurrency, secondCurrency, err := utils.GetProtoCurrenciesByPair(order.Pair)
	if err != nil {
		return &proto.ErrorDto{Code: proto.ErrorCode_ERROR_INTERNAL, Message: err.Error()}
	}

	subtractMoneyRequest := &proto.SubtractMoneyRequest{
		UserId:   order.UserId.String(),
		Volume:   matchedVolume,
		Currency: firstCurrency,
	}
	if order.Direction == proto.OrderDirection_BUY.String() {
		subtractMoneyRequest.Volume = matchedVolume * matchedPrice
		subtractMoneyRequest.Currency = secondCurrency
	}

	sendBody, marshalErr := googleProto.Marshal(subtractMoneyRequest)
	if marshalErr != nil {
		return &proto.ErrorDto{Code: proto.ErrorCode_ERROR_INTERNAL, Message: marshalErr.Error()}
	}

	logger.Debugf(sendingSubtractMoneyRequestMsg, order.OrderId, subtractMoneyRequest.String())
	var subtractMoneyResponse proto.SubtractMoneyResponse
	if err := o.RabbitProvider.SendMessageAndHandleResponse(balanceServiceExchangeName, subtractMoneyRequestRk,
		subtractMoneyResponseRk, subtractMoneyResponseListenerQueueName, sendBody, &subtractMoneyResponse); err != nil {
		return &proto.ErrorDto{Code: proto.ErrorCode_ERROR_INTERNAL, Message: err.Error()}
	}

	if subtractMoneyResponse.Error != nil {
		return subtractMoneyResponse.Error
	}

	return nil
}

func (o *OrderProcessing) addMoney(order *models.OrderModel, matchedVolume, matchedPrice float64) *proto.ErrorDto {
	firstCurrency, secondCurrency, err := utils.GetProtoCurrenciesByPair(order.Pair)
	if err != nil {
		return &proto.ErrorDto{Code: proto.ErrorCode_ERROR_INTERNAL, Message: err.Error()}
	}

	addMoneyRequest := &proto.AddMoneyRequest{
		UserId:   order.UserId.String(),
		Volume:   matchedVolume * matchedPrice,
		Currency: secondCurrency,
	}
	if order.Direction == proto.OrderDirection_BUY.String() {
		addMoneyRequest.Volume = matchedVolume
		addMoneyRequest.Currency = firstCurrency
	}

	sendBody, marshalErr := googleProto.Marshal(addMoneyRequest)
	if marshalErr != nil {
		return &proto.ErrorDto{Code: proto.ErrorCode_ERROR_INTERNAL, Message: marshalErr.Error()}
	}

	logger.Debugf(sendingAddMoneyRequestMsg, addMoneyRequest.String())
	var addMoneyResponse proto.AddMoneyResponse
	if err := o.RabbitProvider.SendMessageAndHandleResponse(balanceServiceExchangeName, addMoneyRequestRk,
		addMoneyResponseRk, addMoneyResponseListenerQueueName, sendBody, &addMoneyResponse); err != nil {
		return &proto.ErrorDto{Code: proto.ErrorCode_ERROR_INTERNAL, Message: err.Error()}
	}

	if addMoneyResponse.Error != nil {
		return addMoneyResponse.Error
	}

	return nil
}

func (o *OrderProcessing) updateOrders(createdMatchOrderModel *models.OrderModel, orderForMatch *models.OrderModel, matchedVolume float64) error {
	limitOrders := []*models.OrderModel{orderForMatch}
	matchedPrice := orderForMatch.InitPrice

	if createdMatchOrderModel.Type == proto.OrderType_LIMIT.String() {
		limitOrders = append(limitOrders, createdMatchOrderModel)
	}

	for _, order := range limitOrders {
		lockUserBalanceDecrement := matchedVolume * orderForMatch.InitPrice
		if order.Direction == proto.OrderDirection_SELL.String() {
			lockUserBalanceDecrement = matchedVolume
		}

		order.FilledPrice = order.FilledPrice*order.FilledVolume/(order.FilledVolume+matchedVolume) + matchedPrice*matchedVolume/(order.FilledVolume+matchedVolume)
		order.FilledVolume += matchedVolume
		order.LockedUserBalance -= lockUserBalanceDecrement
		order.UpdatedDate = time.Now()

		orderJson, err := json.Marshal(order)
		if err != nil {
			return err
		}

		if err = o.SetOrder(order.GetKey(), orderJson); err != nil {
			return err
		}
		logger.Debugf(updateOrderMsg, *order)

		if order.InitVolume == order.FilledVolume {
			if err := o.RemoveOrder(&proto.RemoveOrderRequest{UserId: order.UserId.String(), OrderId: order.OrderId.String()}); err != nil {
				return err
			}
		}
	}

	return nil
}

func (o *OrderProcessing) sendMatchOrdersErrResponse(err *proto.ErrorDto) {
	matchOrdersErrEvent := &proto.MatchOrdersEvent{Error: err}
	logger.Errorf(publishedMatchOrdersEventErrMsg, matchOrdersErrEvent)
	sendBody, _ := googleProto.Marshal(matchOrdersErrEvent)
	o.RabbitProvider.SendMessage(orderProcessingServiceExName, matchOrdersEventRk, sendBody)
}

func (o *OrderProcessing) getMatchOrdersEvent(createdOrderModel, orderModelForMatch models.OrderModel, matchedVolume float64) (*proto.MatchOrdersEvent, error) {
	createdOrderProto, err := converters.ConverOrderModelToProtoOrder(&createdOrderModel)
	if err != nil {
		return nil, err
	}
	orderProtoForMatch, err := converters.ConverOrderModelToProtoOrder(&orderModelForMatch)
	if err != nil {
		return nil, err
	}

	return &proto.MatchOrdersEvent{
		CreatedMatchedOrder: createdOrderProto,
		LimitMatchedOrder:   orderProtoForMatch,
		MatchedVolume:       matchedVolume,
	}, nil
}
