package middleware

import log "github.com/sirupsen/logrus"

func FailOnError(err error, msg string) {
	if err != nil {
		log.Panicf("FailOnError | %s | %s", msg, err)
	}
}
