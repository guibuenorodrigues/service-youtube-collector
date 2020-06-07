package dao

import (
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
)

// VideosService service
type VideosService struct{}

// Videos model
type Videos struct {
	ID         int
	VideoID    string
	ChannelID  string
	InsertedAt time.Time
}

// NewVideosService new service
func NewVideosService() VideosService {
	return VideosService{}
}

// Index from database
func (s VideosService) Index() []Videos {

	// start connection
	db := dbConn()

	selSQL, err := db.Query("SELECT * FROM videos")

	if err != nil {
		logrus.WithFields(logrus.Fields{
			"err": err.Error(),
			"dao": "videos",
		}).Error("Error to retrieve data")
	}

	video := Videos{}
	res := []Videos{}

	for selSQL.Next() {

		var id int
		var vID string
		var cID string
		var insertAt time.Time

		err = selSQL.Scan(&id, &vID, &cID, &insertAt)

		if err != nil {
			logrus.WithFields(logrus.Fields{
				"err": err.Error(),
				"id":  id,
				"dao": "videos",
			}).Error("Error running for")
		}

		video.ID = id
		video.VideoID = vID
		video.ChannelID = cID
		res = append(res, video)
	}

	defer db.Close()

	return res

}

// Show by id
func (s VideosService) Show(videoID string) Videos {

	// start connection
	db := dbConn()

	selSQL, err := db.Query("SELECT * FROM videos WHERE id=?", videoID)

	if err != nil {
		logrus.WithFields(logrus.Fields{
			"err": err.Error(),
			"id":  videoID,
			"dao": "videos",
		}).Error("Error to retrieve by ID")
	}

	video := Videos{}

	for selSQL.Next() {

		var id int
		var vID string
		var cID string
		var insertAt time.Time

		err = selSQL.Scan(&id, &vID, &cID, &insertAt)

		if err != nil {
			logrus.WithFields(logrus.Fields{
				"err": err.Error(),
				"id":  id,
				"dao": "videos",
			}).Error("Error running by id")
		}

		video.ID = id
		video.VideoID = vID
		video.ChannelID = cID
	}

	defer db.Close()

	return video

}

// Insert blacklist
func (s VideosService) Insert(videos Videos) error {

	db := dbConn()

	insForm, err := db.Prepare("INSERT INTO videos(video_id, channel_id) VALUES(?,?)")

	if err != nil {
		logrus.WithFields(logrus.Fields{
			"err": err.Error(),
			"dao": "videos",
		}).Error("error to prepare sql statment")

		return err
	}

	insForm.Exec(videos.VideoID, videos.ChannelID)

	defer db.Close()

	return nil
}
