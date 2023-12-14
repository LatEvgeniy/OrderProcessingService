package processing

import (
	"OrderProcessingService/converters"
	"OrderProcessingService/models"
	"OrderProcessingService/proto"
	"OrderProcessingService/utils"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	logger "github.com/sirupsen/logrus"
	googleProto "google.golang.org/protobuf/proto"
)

var (
	orderProcessingServiceExName = "ex.OrderProcessingService"

	addMoneyResponseListenerQueueName = "q.OrderProcessingService.AddMoneyResponse.Listener"
	addMoneyResponseRk                = "rk.AddMoneyResponse"
	addMoneyRequestRk                 = "rk.AddMoneyRequest"

	subtractMoneyResponseListenerQueueName = "q.OrderProcessingService.SubtractMoneyResponse.Listener"
	subtractMoneyResponseRk                = "rk.SubtractMoneyResponse"
	subtractMoneyRequestRk                 = "rk.SubtractMoneyRequest"

	getUserByIdResponseListenerQueueName = "q.OrderProcessingService.GetUserByIdResponse.Listener"
	getUserByIdResponseRk                = "rk.GetUserByIdResponse"
	getUserRequestRk                     = "rk.GetUserByIdRequest"

	matchOrdersEventRk = "rk.MatchOrdersEvent"

	availableForMatchVolumeIndexName = "AvailableForMatchVolume"

	matchKeyPattern = "order:%s:%s:*"
	orderKeyPattern = "orderid:%s"

	buyDirection  = "BUY"
	sellDirection = "SELL"

	// ---- Log Msgs ----

	OrderProcessingGotErrResponseMsg         = "OrderProcessing got error response"
	OrderProcessingGotCreateOrderResponseMsg = "OrderProcessing got CreateOrderResponse: %s"

	createdOrderModelIsMsg = "%s:CreatedOrderModel is:\n%+v"
	matchKeyIsMsg          = "%s:Match key is: %s"
	OrderModelsForMatchMsg = "%s:OrderModelsForMatch:\n%+v"
	noOrdersToMatchMsg     = "%s:No orders to match"
	noAvailableBalanceMsg  = "%s:User: %s hasn`t available %s balance to match order: %s"

	sendingGetUserRequestMsg                 = "%s:Send GetUserRequest with userId: %s"
	sendMatchLimitOrdersMsg                  = "%s:Send match order:\n%+v \nWith limit createdOrder:\n%+v \nMatchedVolume: %f"
	sendMatchLimitWithMarketOrderMsg         = "%s:Send match order:\n%+v \nWith market createdOrder:\n%+v \nMatchedVolume: %f"
	availableVolumeForMatchOrderMsg          = "%s:For match order with id: %s Available volume: %f"
	availableBalanceForCreatedOrderMsg       = "%s:For created order with id: %s Available balance: %f"
	decrementedAvailableVolumeMsg            = "%s:For match order: %s decremented available volume for match: %f"
	availableBalanceGreaterAvilabelVolumeMsg = "%s:Created order available balance: %f greater than init-fill volume: %f"

	errToFindBalanceErrMsg                 = "%s:error to find balance for pair %s in user: %+v"
	availableForMatchVolumeLessZeroErrMsg  = "%s:available for match volume cannot be less than 0 but was: %f"
	errWhileDecrementAvailableVolumeErrMsg = "%s:err while decrement available volume for order: %s Available was less than 0: %f"
	errWhileLockBalanceErrMsg              = "%s:err while lock balance: %+v"
	errGetUserBalanceErrMsg                = "%s:error get user balance: %+v"
)

func (o *OrderProcessing) MatchOrders(byteResponse []byte) {
	createdOrder, err := o.getCreatedOrderProto(byteResponse)
	if err != nil {
		o.matchOrders(nil, nil, 0, err)
	}
	if createdOrder == nil {
		logger.Debug(OrderProcessingGotErrResponseMsg)
		return
	}

	createdOrderModel := converters.ConvertOrderProtoToOrderModel(createdOrder)
	logger.Debugf(createdOrderModelIsMsg, createdOrderModel.OrderId, *createdOrderModel)

	logger.Debugf(matchKeyIsMsg, createdOrderModel.OrderId, o.getMatchKey(createdOrder.Direction, createdOrder.Pair))
	orderKeys, err := o.getKeysByPattern(o.getMatchKey(createdOrder.Direction, createdOrder.Pair))
	if err != nil {
		o.matchOrders(nil, nil, 0, err)
	}

	orderModelsForMatch, err := o.getSortedOrders(createdOrder.Direction, createdOrderModel.UserId, orderKeys)
	if err != nil {
		o.matchOrders(nil, nil, 0, err)
	}
	if len(orderModelsForMatch) == 0 {
		logger.Debugf(noOrdersToMatchMsg, createdOrder.OrderId)
		return
	}
	logger.Debugf(OrderModelsForMatchMsg, createdOrderModel.OrderId, orderModelsForMatch)
	o.runMatchLoop(createdOrderModel, orderModelsForMatch)
}

func (o *OrderProcessing) runMatchLoop(createdOrderModel *models.OrderModel, orderModelsForMatch []models.OrderModel) {
	for _, orderModelForMatch := range orderModelsForMatch {
		if createdOrderModel.Type == proto.OrderType_MARKET.String() {
			if err := o.matchMarketWithLimitOrder(createdOrderModel, &orderModelForMatch); err != nil {
				return
			}
			continue
		}
		o.matchLimitOrders(createdOrderModel, &orderModelForMatch)
	}
}

// ---- Limit Orders ----

func (o *OrderProcessing) matchLimitOrders(createdOrderModel *models.OrderModel, orderModelForMatch *models.OrderModel) {
	if (createdOrderModel.Direction == proto.OrderDirection_SELL.String() && createdOrderModel.InitPrice > orderModelForMatch.InitPrice) ||
		(createdOrderModel.Direction == proto.OrderDirection_BUY.String() && createdOrderModel.InitPrice < orderModelForMatch.InitPrice) {
		return
	}

	matchedVolume, err := o.getMatchVolumeForLimitOrders(createdOrderModel, orderModelForMatch)
	if err != nil {
		o.matchOrders(nil, nil, 0, err)
	}

	if err := o.decrementAvailableVolume([]*models.OrderModel{createdOrderModel, orderModelForMatch}, matchedVolume, createdOrderModel.OrderId); err != nil {
		o.matchOrders(nil, nil, 0, err)
	}

	logger.Debugf(sendMatchLimitOrdersMsg, createdOrderModel.OrderId, *orderModelForMatch, *createdOrderModel, matchedVolume)
	o.matchOrders(createdOrderModel, orderModelForMatch, matchedVolume, nil)
}

func (o *OrderProcessing) getMatchVolumeForLimitOrders(createdOrderModel *models.OrderModel, orderForMatch *models.OrderModel) (float64, error) {
	availableForMatchVolume, err := o.RedisClient.HIncrByFloat(context.Background(), availableForMatchVolumeIndexName,
		fmt.Sprintf(orderKeyPattern, orderForMatch.OrderId.String()), 0).Result()
	if err != nil {
		return 0, err
	}
	if availableForMatchVolume < 0 {
		return 0, fmt.Errorf(availableForMatchVolumeLessZeroErrMsg, createdOrderModel.OrderId, availableForMatchVolume)
	}

	if availableForMatchVolume <= createdOrderModel.InitVolume {
		return availableForMatchVolume, nil
	}
	return createdOrderModel.InitVolume, nil
}

// ---- Market with Limit order ----

func (o *OrderProcessing) matchMarketWithLimitOrder(createdOrderModel *models.OrderModel, orderModelForMatch *models.OrderModel) error {
	userBalance, err := o.getUserBalance(createdOrderModel)
	if err != nil {
		o.matchOrders(nil, nil, 0, err)
		return err
	}
	if userBalance.Balance == userBalance.LockedBalance {
		logger.Debugf(noAvailableBalanceMsg, createdOrderModel.OrderId, createdOrderModel.UserId, userBalance.CurrencyName, createdOrderModel.OrderId)
		return fmt.Errorf(noAvailableBalanceMsg, createdOrderModel.OrderId, createdOrderModel.UserId, userBalance.CurrencyName, createdOrderModel.OrderId)
	}

	matchedVolume, err := o.getMatchVolumeForMarketAndLimitOrders(createdOrderModel, orderModelForMatch, userBalance)
	if err != nil {
		o.matchOrders(nil, nil, 0, err)
		return err
	}

	if err := o.decrementAvailableVolume([]*models.OrderModel{orderModelForMatch}, matchedVolume, createdOrderModel.OrderId); err != nil {
		o.matchOrders(nil, nil, 0, err)
	}
	logger.Debugf(decrementedAvailableVolumeMsg, createdOrderModel.OrderId, orderModelForMatch.OrderId, matchedVolume)

	if err := o.lockBalanceForMarketOrder(createdOrderModel, matchedVolume, orderModelForMatch.InitPrice); err != nil {
		o.matchOrders(nil, nil, 0, fmt.Errorf(errWhileLockBalanceErrMsg, createdOrderModel.OrderId, err))
		return fmt.Errorf(errWhileLockBalanceErrMsg, createdOrderModel.OrderId, err)
	}
	createdOrderModel.FilledVolume += matchedVolume

	logger.Debugf(sendMatchLimitWithMarketOrderMsg, createdOrderModel.OrderId, *orderModelForMatch, *createdOrderModel, matchedVolume)
	o.matchOrders(createdOrderModel, orderModelForMatch, matchedVolume, nil)
	return nil
}

func (o *OrderProcessing) getMatchVolumeForMarketAndLimitOrders(createdOrderModel *models.OrderModel, orderModelForMatch *models.OrderModel, userBalance *proto.Balance) (float64, error) {
	orderForMatchAvailableVolume, err := o.RedisClient.HIncrByFloat(context.Background(),
		availableForMatchVolumeIndexName, fmt.Sprintf(orderKeyPattern, orderModelForMatch.OrderId.String()), 0).Result()
	if err != nil {
		return 0, err
	}
	logger.Debugf(availableVolumeForMatchOrderMsg, createdOrderModel.OrderId, orderModelForMatch.OrderId.String(), orderForMatchAvailableVolume)

	createdOrderMaxMatchVolume := o.getCreatedOrderMaxMatchVolume(createdOrderModel, orderModelForMatch, userBalance)

	matchedVolume := orderForMatchAvailableVolume
	if orderForMatchAvailableVolume > createdOrderMaxMatchVolume {
		matchedVolume = createdOrderMaxMatchVolume
	}

	return matchedVolume, nil
}

func (o *OrderProcessing) getCreatedOrderMaxMatchVolume(createdOrderModel *models.OrderModel, orderModelForMatch *models.OrderModel, userBalance *proto.Balance) float64 {
	createdOrderAvailableBalance := userBalance.Balance - userBalance.LockedBalance
	if createdOrderModel.Direction == proto.OrderDirection_BUY.String() {
		createdOrderAvailableBalance = (userBalance.Balance - userBalance.LockedBalance) / orderModelForMatch.InitPrice
	}
	logger.Debugf(availableBalanceForCreatedOrderMsg, createdOrderModel.OrderId, createdOrderModel.OrderId.String(), createdOrderAvailableBalance)

	createdOrderMaxMatchVolume := createdOrderAvailableBalance
	if createdOrderAvailableBalance > createdOrderModel.InitVolume-createdOrderModel.FilledVolume {
		logger.Debugf(availableBalanceGreaterAvilabelVolumeMsg, createdOrderModel.OrderId, createdOrderAvailableBalance, createdOrderModel.InitVolume)
		createdOrderMaxMatchVolume = createdOrderModel.InitVolume - createdOrderModel.FilledVolume
	}

	return createdOrderMaxMatchVolume
}

func (o *OrderProcessing) lockBalanceForMarketOrder(createdOrderModel *models.OrderModel, matchedVolume, matchedPrice float64) *proto.ErrorDto {
	currency, err := utils.GetProtoCurrency(createdOrderModel.Direction, createdOrderModel.Pair)
	if err != nil {
		return &proto.ErrorDto{Code: proto.ErrorCode_ERROR_INTERNAL, Message: err.Error()}
	}

	lockUserBalanceVolume := matchedVolume * matchedPrice
	if createdOrderModel.Direction == proto.OrderDirection_SELL.String() {
		lockUserBalanceVolume = matchedVolume
	}

	if _, errDto := o.LockBalance(
		&proto.LockBalanceRequest{UserId: createdOrderModel.UserId.String(), Currency: currency, Volume: lockUserBalanceVolume}); errDto != nil {
		return errDto
	}
	return nil
}

func (o *OrderProcessing) getUserBalance(createdOrderModel *models.OrderModel) (*proto.Balance, error) {
	logger.Debugf(sendingGetUserRequestMsg, createdOrderModel.OrderId, createdOrderModel.UserId.String())
	var response proto.GetUserByIdResponse
	if err := o.RabbitProvider.SendMessageAndHandleResponse(balanceServiceExchangeName, getUserRequestRk,
		getUserByIdResponseRk, getUserByIdResponseListenerQueueName, []byte(createdOrderModel.UserId.String()), &response); err != nil {
		return nil, err
	}

	if err := response.GetError(); err != nil {
		return nil, fmt.Errorf(errGetUserBalanceErrMsg, createdOrderModel.OrderId, err)
	}

	matchCurrencyName, err := utils.GetStringCurrency(createdOrderModel.Direction, createdOrderModel.Pair)
	if err != nil {
		return nil, err
	}

	for _, balance := range response.User.Balances {
		if balance.CurrencyName == matchCurrencyName {
			return balance, nil
		}
	}
	return nil, fmt.Errorf(errToFindBalanceErrMsg, createdOrderModel.OrderId, createdOrderModel.Pair, response.User)
}

func (o *OrderProcessing) getMatchKey(createdOrderDirection proto.OrderDirection, createdOrderPair proto.OrderPair) string {
	if createdOrderDirection == proto.OrderDirection_SELL {
		return fmt.Sprintf(matchKeyPattern, createdOrderPair.String(), buyDirection)
	}
	return fmt.Sprintf(matchKeyPattern, createdOrderPair.String(), sellDirection)
}

func (o *OrderProcessing) getKeysByPattern(pattern string) ([]string, error) {
	var keys []string
	ctx := context.Background()

	iter := o.RedisClient.Scan(ctx, 0, pattern, 0).Iterator()
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}

	if err := iter.Err(); err != nil {
		return nil, err
	}

	return keys, nil
}

func (o *OrderProcessing) getSortedOrders(createdOrderDirection proto.OrderDirection, createdOrderUserId uuid.UUID, orderKeys []string) ([]models.OrderModel, error) {
	var orders []models.OrderModel

	for _, orderKey := range orderKeys {
		jsonStr, err := o.RedisClient.Get(context.Background(), orderKey).Result()
		if err != nil {
			return nil, err
		}

		var orderModel models.OrderModel
		if err = json.Unmarshal([]byte(jsonStr), &orderModel); err != nil {
			return nil, err
		}

		if orderModel.UserId != createdOrderUserId && orderModel.ExpirationDate.After(time.Now()) {
			orders = append(orders, orderModel)
		}
	}

	sort.SliceStable(orders, func(i, j int) bool {
		return orders[i].CreationDate.Before(orders[j].CreationDate)
	})

	sort.SliceStable(orders, func(i, j int) bool {
		if createdOrderDirection == proto.OrderDirection_SELL {
			return orders[i].InitPrice > orders[j].InitPrice
		}
		return orders[i].InitPrice < orders[j].InitPrice
	})

	return orders, nil
}

func (o *OrderProcessing) decrementAvailableVolume(limitOrders []*models.OrderModel, volumeToLock float64, createdOrderModelId uuid.UUID) error {
	for _, orderModel := range limitOrders {
		availableVolume, err := o.RedisClient.HIncrByFloat(context.Background(),
			availableForMatchVolumeIndexName, fmt.Sprintf(orderKeyPattern, orderModel.OrderId.String()), -volumeToLock).Result()
		if err != nil {
			return err
		}

		if availableVolume < 0 {
			o.RedisClient.HIncrByFloat(context.Background(), availableForMatchVolumeIndexName,
				fmt.Sprintf(orderKeyPattern, orderModel.OrderId.String()), +volumeToLock)
			return fmt.Errorf(errWhileDecrementAvailableVolumeErrMsg, createdOrderModelId, orderModel.OrderId.String(), availableVolume)
		}
	}

	return nil
}

func (o *OrderProcessing) getCreatedOrderProto(byteResponse []byte) (*proto.Order, error) {
	var createOrderResponse proto.CreateOrderResponse
	if err := googleProto.Unmarshal(byteResponse, &createOrderResponse); err != nil {
		return nil, err
	}

	logger.Infof(OrderProcessingGotCreateOrderResponseMsg, createOrderResponse.String())
	return createOrderResponse.CreatedOrder, nil
}
