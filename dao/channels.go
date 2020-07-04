package dao

import (
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
)

// ChannelsService struct
type ChannelService struct{}

// Channel Model
type Channel struct {
	ID         int
	ChannelID  string
	FullSynced bool
	CreatedAt  time.Time
}

// NewChannelService method
func NewChannelService() ChannelService {
	return ChannelService{}
}

var (
	page_name = "channels"
	table     = "channels"
)

// Truncate method
func (s ChannelService) Truncate() error {

	db := dbConn()

	_, err := db.Exec("TRUNCATE TABLE " + table)

	if err != nil {
		logrus.WithFields(logrus.Fields{
			"err": err.Error(),
			"dao": page_name,
		}).Error("Error to truncate the data")

		return err
	}

	return nil
}

// Index method
func (s ChannelService) Index() []Channel {

	db := dbConn()

	selSQL, err := db.Query("SELECT * FROM " + table)

	if err != nil {
		logrus.WithFields(logrus.Fields{
			"err": err.Error(),
			"dao": page_name,
		}).Error("Error to retrieve data")
	}

	channel := Channel{}
	res := []Channel{}

	for selSQL.Next() {

		var id int
		var channelID string
		var fullSynced bool
		var createdAt time.Time

		err = selSQL.Scan(&id, &channelID, &fullSynced, &createdAt)

		if err != nil {
			logrus.WithFields(logrus.Fields{
				"err": err.Error(),
				"id":  id,
				"dao": page_name,
			}).Error("Error to scan database results")
		}

		channel.ID = id
		channel.ChannelID = channelID
		channel.FullSynced = fullSynced
		channel.CreatedAt = createdAt

		res = append(res, channel)
	}

	defer db.Close()

	return res

}

// Insert channel
func (s ChannelService) Insert(channel Channel) error {

	db := dbConn()

	insForm, err := db.Prepare("INSERT INTO videos(channel_id, full_synced) VALUES(?,?)")

	if err != nil {
		logrus.WithFields(logrus.Fields{
			"err": err.Error(),
			"dao": page_name,
		}).Error("error to prepare sql statment")

		return err
	}

	insForm.Exec(channel.ChannelID, channel.FullSynced)

	defer db.Close()

	return nil
}
