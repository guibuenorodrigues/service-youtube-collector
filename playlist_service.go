package main

import (
	"soliveboa/youtuber/v2/dao"
	"soliveboa/youtuber/v2/entities"
	_errors "soliveboa/youtuber/v2/errors"

	"github.com/sirupsen/logrus"
)

func startPlaylist() {

	logrus.Info("[  *  ] Searching for videos on playlist.list {upcoming videos} by ...")

	// key a key to use
	key, err, _ := dao.GetNewKey(authKeys, false)

	if err != nil {
		failOnError(err, "error to get key from list")

	}

	p := entities.GetPlaylists()

	for _, val := range p {
		ys := NewYotubeService()
		err := ys.RunByPlaylist(key, val)
		_errors.HandleError("Error to retrieve data from playlist", err, false)

		is403, _ := _errors.VerifyError403(err)
		if is403 {
			// remove the key from the array
			_, _, keys := dao.GetNewKey(authKeys, true)
			authKeys = keys
			break
		}
	}

}
