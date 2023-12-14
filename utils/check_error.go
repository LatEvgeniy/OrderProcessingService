package utils

import logger "github.com/sirupsen/logrus"

func CheckErrorWithPanic(err error) {
	if err != nil {
		logger.Panic(err)
		panic(err)
	}
}
