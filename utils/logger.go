package utils

import (
	"os"

	logger "github.com/sirupsen/logrus"
)

func init() {
	logger.SetOutput(os.Stdout)
	logger.SetLevel(logger.DebugLevel)
	logger.SetFormatter(&logger.TextFormatter{FullTimestamp: true})
}
