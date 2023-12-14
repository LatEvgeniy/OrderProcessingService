package main

import (
	"OrderProcessingService/components"
	"OrderProcessingService/entrypoints"
	"OrderProcessingService/processing"
	"OrderProcessingService/providers"
	"time"
)

var (
	checkExpiredOrdersScheduleTime = time.Second

	orderProcessingExchangeName = "ex.OrderProcessingService"

	createOrderRequestRkName   = "rk.CreateOrdeRequest"
	getUserOrdersRequestRkName = "rk.GetUserOrdersRequest"
	removeOrderRequestRkName   = "rk.RemoveOrderRequest"
	createOrderResponseRkName  = "rk.CreateOrderResponse"

	createOrderRequestListenerQueueName   = "q.OrderProcessingService.CreateOrderRequest.Listener"
	getUserOrdersRequestListenerQueueName = "q.OrderProcessingService.GetUserOrdersRequest.Listener"
	removeOrderRequestListenerQueueName   = "q.OrderProcessingService.RemoveOrderRequest.Listener"
	createOrderResponseListenerQueueName  = "q.OrderProcessingService.CreateOrderResponse.Listener"
)

func main() {
	rabbitProvider := providers.NewRabbitProvider()
	redisClient := providers.NewRedisClient()
	defer redisClient.Close()

	orderProcessing := &processing.OrderProcessing{RabbitProvider: rabbitProvider, RedisClient: redisClient}
	orderComponent := &components.OrderComponent{
		RabbitProvider:  rabbitProvider,
		OrderProcessing: orderProcessing,
	}
	orderEntrypoint := &entrypoints.OrderEntrypoint{OrderComponent: orderComponent}

	rabbitProvider.DeclareExchange(orderProcessingExchangeName)

	go orderProcessing.CheckExpiredOrdersBySchedule(checkExpiredOrdersScheduleTime)

	runListeners(rabbitProvider, orderEntrypoint, orderProcessing)
}

func runListeners(rabbitProvider *providers.RabbitProvider, orderEntrypoint *entrypoints.OrderEntrypoint, orderProcessing *processing.OrderProcessing) {
	msgs, ch := rabbitProvider.GetQueueConsumer(orderProcessingExchangeName, createOrderRequestRkName, createOrderRequestListenerQueueName)
	go rabbitProvider.RunListener(msgs, ch, orderEntrypoint.CreateOrder)

	msgs, ch = rabbitProvider.GetQueueConsumer(orderProcessingExchangeName, getUserOrdersRequestRkName, getUserOrdersRequestListenerQueueName)
	go rabbitProvider.RunListener(msgs, ch, orderEntrypoint.GetUserOrders)

	msgs, ch = rabbitProvider.GetQueueConsumer(orderProcessingExchangeName, removeOrderRequestRkName, removeOrderRequestListenerQueueName)
	go rabbitProvider.RunListener(msgs, ch, orderEntrypoint.RemoveOrder)

	msgs, ch = rabbitProvider.GetQueueConsumer(orderProcessingExchangeName, createOrderResponseRkName, createOrderResponseListenerQueueName)
	rabbitProvider.RunListener(msgs, ch, orderProcessing.MatchOrders)
}
