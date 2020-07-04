package errors

import (
	"strings"

	"github.com/sirupsen/logrus"
)

// Handle error messages
func HandleError(message string, err error, fatal bool) bool {

	if err != nil {
		// if fatal is enable
		if fatal {
			logrus.WithFields(logrus.Fields{
				"err": err,
			}).Fatal(message)
		}

		logrus.WithFields(logrus.Fields{
			"err": err,
		}).Error(message)

		return true
	}

	return false
}

// VeryfyVerifyError403
func VerifyError403(err error) (bool, error) {

	if err != nil && strings.Contains(err.Error(), "403") {

		logrus.Warning("Error 403")
		return true, err
	}

	logrus.Error(err)
	return false, err

}
