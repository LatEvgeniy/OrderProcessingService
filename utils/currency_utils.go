package utils

import (
	"OrderProcessingService/proto"
	"fmt"
)

var (
	notImplementedPairErrMsg = "not implemented pair: %s"
)

func GetProtoCurrency(direction string, pair string) (proto.Currency, error) {
	if direction == proto.OrderDirection_SELL.String() {
		switch pair {
		case proto.OrderPair_USD_EUR.String():
			return proto.Currency_CURRENCY_USD, nil
		case proto.OrderPair_USD_UAH.String():
			return proto.Currency_CURRENCY_USD, nil
		case proto.OrderPair_UAH_EUR.String():
			return proto.Currency_CURRENCY_UAH, nil
		default:
			return proto.Currency_CURRENCY_NONE, fmt.Errorf(notImplementedPairErrMsg, pair)
		}
	}
	switch pair {
	case proto.OrderPair_USD_EUR.String():
		return proto.Currency_CURRENCY_EUR, nil
	case proto.OrderPair_USD_UAH.String():
		return proto.Currency_CURRENCY_UAH, nil
	case proto.OrderPair_UAH_EUR.String():
		return proto.Currency_CURRENCY_EUR, nil
	default:
		return proto.Currency_CURRENCY_NONE, fmt.Errorf(notImplementedPairErrMsg, pair)
	}
}

func GetStringCurrency(direction string, pair string) (string, error) {
	if direction == proto.OrderDirection_SELL.String() {
		switch pair {
		case proto.OrderPair_USD_EUR.String():
			return "USD", nil
		case proto.OrderPair_USD_UAH.String():
			return "USD", nil
		case proto.OrderPair_UAH_EUR.String():
			return "UAH", nil
		default:
			return "", fmt.Errorf(notImplementedPairErrMsg, pair)
		}
	}
	switch pair {
	case proto.OrderPair_USD_EUR.String():
		return "EUR", nil
	case proto.OrderPair_USD_UAH.String():
		return "UAH", nil
	case proto.OrderPair_UAH_EUR.String():
		return "EUR", nil
	default:
		return "", fmt.Errorf(notImplementedPairErrMsg, pair)
	}
}

func GetProtoCurrenciesByPair(pair string) (proto.Currency, proto.Currency, error) {
	switch pair {
	case proto.OrderPair_USD_EUR.String():
		return proto.Currency_CURRENCY_USD, proto.Currency_CURRENCY_EUR, nil
	case proto.OrderPair_USD_UAH.String():
		return proto.Currency_CURRENCY_USD, proto.Currency_CURRENCY_UAH, nil
	case proto.OrderPair_UAH_EUR.String():
		return proto.Currency_CURRENCY_UAH, proto.Currency_CURRENCY_EUR, nil
	default:
		return proto.Currency_CURRENCY_NONE, proto.Currency_CURRENCY_NONE, fmt.Errorf(notImplementedPairErrMsg, pair)
	}
}
