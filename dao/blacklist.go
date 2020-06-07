package dao

import (
	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
)

// BlackListResponse model
type BlackListResponse struct {
	ChannelID string
}

// func dbConn() (db *sql.DB) {
// 	dbDriver := "mysql"
// 	dbUser := "root"
// 	dbPass := ""
// 	dbName := "soliveboa"
// 	db, err := sql.Open(dbDriver, dbUser+":"+dbPass+"@/"+dbName)
// 	if err != nil {
// 		panic(err.Error())
// 	}
// 	return db
// }

// BlacklistService service
type BlacklistService struct{}

// Blacklist model
type Blacklist struct {
	ID        int
	ChannelID string
}

// NewBlacklistService new service
func NewBlacklistService() BlacklistService {
	return BlacklistService{}
}

// Index from database
func (s BlacklistService) Index() []Blacklist {

	// start connection
	db := dbConn()

	selSQL, err := db.Query("SELECT * FROM blacklist")

	if err != nil {
		logrus.WithFields(logrus.Fields{
			"err": err.Error(),
			"dao": "blacklist",
		}).Error("Error to retrieve blacklist")
	}

	bck := Blacklist{}
	res := []Blacklist{}

	for selSQL.Next() {

		var id int
		var channelID string

		err = selSQL.Scan(&id, &channelID)

		if err != nil {
			logrus.WithFields(logrus.Fields{
				"err":       err.Error(),
				"channelID": channelID,
				"dao":       "blacklist",
			}).Error("Error running for")
		}

		bck.ID = id
		bck.ChannelID = channelID
		res = append(res, bck)
	}

	defer db.Close()

	return res

}

// Show by id
func (s BlacklistService) Show(chanID string) Blacklist {

	// start connection
	db := dbConn()

	selSQL, err := db.Query("SELECT * FROM blacklist WHERE channel_id=?", chanID)

	if err != nil {
		logrus.WithFields(logrus.Fields{
			"err":        err.Error(),
			"channel_id": chanID,
			"dao":        "blacklist",
		}).Error("Error to retrieve blacklist by channel ID")
	}

	bck := Blacklist{}

	for selSQL.Next() {

		var id int
		var channelID string

		err = selSQL.Scan(&id, &channelID)

		if err != nil {
			logrus.WithFields(logrus.Fields{
				"err":       err.Error(),
				"channelID": channelID,
				"dao":       "blacklist",
			}).Error("Error running for - channel ID")
		}

		bck.ID = id
		bck.ChannelID = channelID
	}

	defer db.Close()

	return bck

}

// Insert blacklist
func (s BlacklistService) Insert(bck Blacklist) error {

	db := dbConn()

	insForm, err := db.Prepare("INSERT INTO blacklist(channel_id) VALUES(?)")

	if err != nil {
		logrus.WithFields(logrus.Fields{
			"err": err.Error(),
			"dao": "blacklist",
		}).Error("error to prepare sql statment")

		return err
	}

	insForm.Exec(bck.ChannelID)

	defer db.Close()

	return nil
}
