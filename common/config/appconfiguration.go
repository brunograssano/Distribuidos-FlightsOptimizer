package config

import (
	"github.com/sirupsen/logrus"
)

// InitLogger Receives the log level to be set in logrus as a string. This method
// parses the string and set the level to the logger. If the level string is not
// valid an error is returned
func InitLogger(logLevel string) error {
	level, err := logrus.ParseLevel(logLevel)
	if err != nil {
		return err
	}

	customFormatter := &logrus.TextFormatter{
		TimestampFormat: "2006-01-02 15:04:05",
		FullTimestamp:   false,
	}
	logrus.SetFormatter(customFormatter)
	logrus.SetLevel(level)
	return nil
}
