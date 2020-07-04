package dao

import (
	"errors"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
)

// ChannelsService struct
type AuthorizationService struct{}

// Channel Model
type Authorization struct {
	ID        int
	Token     string
	ExpiredAt time.Time
	CreatedAt time.Time
}

// NewChannelService method
func NewAuthorizationService() AuthorizationService {
	return AuthorizationService{}
}

var (
	page_name_auth = "authorization_keys"
	table_auth     = "authorization_keys"
)

// Truncate method
func (s AuthorizationService) Truncate() error {

	db := dbConn()

	_, err := db.Exec("TRUNCATE TABLE " + table)

	if err != nil {
		logrus.WithFields(logrus.Fields{
			"err": err.Error(),
			"dao": page_name_auth,
		}).Error("Error to truncate the data")

		return err
	}

	return nil
}

// Index method
func (s AuthorizationService) Index() []Authorization {

	db := dbConn()

	selSQL, err := db.Query("SELECT * FROM " + table_auth)

	if err != nil {
		logrus.WithFields(logrus.Fields{
			"err": err.Error(),
			"dao": page_name_auth,
		}).Error("Error to retrieve data")
	}

	auth := Authorization{}
	res := []Authorization{}

	for selSQL.Next() {

		var id int
		var token string
		var expiredAt time.Time
		var createdAt time.Time

		err = selSQL.Scan(&id, &token, &expiredAt, &createdAt)

		if err != nil {
			logrus.WithFields(logrus.Fields{
				"err": err.Error(),
				"id":  id,
				"dao": page_name_auth,
			}).Error("Error to scan database results")
		}

		auth.ID = id
		auth.Token = token
		auth.ExpiredAt = expiredAt
		auth.CreatedAt = createdAt

		res = append(res, auth)
	}

	defer db.Close()

	return res

}

func (s AuthorizationService) Show(token string) Authorization {

	// start connection
	db := dbConn()

	selSQL, err := db.Query("SELECT * FROM "+table_auth+" WHERE token=?", token)

	if err != nil {
		logrus.WithFields(logrus.Fields{
			"err":   err.Error(),
			"token": token,
			"dao":   page_name_auth,
		}).Error("Error to retrieve by token")
	}

	auth := Authorization{}

	for selSQL.Next() {

		var id int
		var token string
		var expiredAt time.Time
		var createdAt time.Time

		err = selSQL.Scan(&id, &token, &expiredAt, &createdAt)

		if err != nil {
			logrus.WithFields(logrus.Fields{
				"err": err.Error(),
				"id":  id,
				"dao": page_name_auth,
			}).Error("Error to scan database results")
		}

		auth.ID = id
		auth.Token = token
		auth.ExpiredAt = expiredAt
		auth.CreatedAt = createdAt
	}

	defer db.Close()

	return auth

}

// Insert key
func (s AuthorizationService) Insert(auth Authorization) error {

	db := dbConn()

	insForm, err := db.Prepare("INSERT INTO " + table_auth + "(token, expired_at) VALUES(?,?)")

	if err != nil {
		logrus.WithFields(logrus.Fields{
			"err": err.Error(),
			"dao": page_name_auth,
		}).Error("error to prepare sql statment")

		return err
	}

	insForm.Exec(auth.Token, auth.ExpiredAt)

	defer db.Close()

	return nil
}

// Update key
func (s AuthorizationService) UpdateExpiredAt(token string) error {

	db := dbConn()

	insForm, err := db.Prepare("UPDATE " + table_auth + " SET expired_at=NOW() WHERE token=?")

	if err != nil {
		logrus.WithFields(logrus.Fields{
			"err": err.Error(),
			"dao": page_name_auth,
		}).Error("error to prepare sql statment to update")

		return err
	}

	insForm.Exec(token)

	defer db.Close()

	return nil
}

// GetGetNewKey method
func GetNewKey(keys []string, expired bool) (string, error, []string) {

	if len(keys) <= 0 {
		return "", errors.New("Keys has zero or less elements"), keys
	}

	// get a key
	key := keys[0]

	// remove from the array
	if expired {
		keys = keys[1:]

		NewAuthorizationService().UpdateExpiredAt(key)
	}

	return key, nil, keys
}
