package dao

import (
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
)

// TokenRecoveryService service
type TokenRecoveryService struct{}

// TokenRecovery model
type TokenRecovery struct {
	ID         int
	NextToken  string
	InsertedAt time.Time
}

// NewTokenRecoveryService new service
func NewTokenRecoveryService() TokenRecoveryService {
	return TokenRecoveryService{}
}

// Index from database
func (s TokenRecoveryService) Index() []TokenRecovery {

	// start connection
	db := dbConn()

	selSQL, err := db.Query("SELECT * FROM token_recovery")

	if err != nil {
		logrus.WithFields(logrus.Fields{
			"err": err.Error(),
			"dao": "token_recovery",
		}).Error("Error to retrieve data")
	}

	tkr := TokenRecovery{}
	res := []TokenRecovery{}

	for selSQL.Next() {

		var id int
		var token string
		var insertAt time.Time

		err = selSQL.Scan(&id, &token, &insertAt)

		if err != nil {
			logrus.WithFields(logrus.Fields{
				"err":   err.Error(),
				"id":    id,
				"token": token,
				"dao":   "token_recovery",
			}).Error("Error running for")
		}

		tkr.ID = id
		tkr.NextToken = token
		res = append(res, tkr)
	}

	defer db.Close()

	return res

}

// Show by id
func (s TokenRecoveryService) Show(tokenID string) TokenRecovery {

	// start connection
	db := dbConn()

	selSQL, err := db.Query("SELECT * FROM token_recovery WHERE id=?", tokenID)

	if err != nil {
		logrus.WithFields(logrus.Fields{
			"err": err.Error(),
			"id":  tokenID,
			"dao": "token_recovery",
		}).Error("Error to retrieve by ID")
	}

	tkr := TokenRecovery{}

	for selSQL.Next() {

		var id int
		var token string
		var insertAt time.Time

		err = selSQL.Scan(&id, &token, &insertAt)

		if err != nil {
			logrus.WithFields(logrus.Fields{
				"err":   err.Error(),
				"id":    id,
				"token": token,
				"dao":   "token_recovery",
			}).Error("Error running by id")
		}

		tkr.ID = id
		tkr.NextToken = token
		tkr.InsertedAt = insertAt
	}

	defer db.Close()

	return tkr

}

// Insert blacklist
func (s TokenRecoveryService) Insert(tkr TokenRecovery) error {

	db := dbConn()

	insForm, err := db.Prepare("INSERT INTO token_recovery(next_token) VALUES(?)")

	if err != nil {
		logrus.WithFields(logrus.Fields{
			"err": err.Error(),
			"dao": "token_recovery",
		}).Error("error to prepare sql statment")

		return err
	}

	insForm.Exec(tkr.NextToken)

	defer db.Close()

	return nil
}
