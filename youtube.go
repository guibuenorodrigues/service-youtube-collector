package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"soliveboa/youtuber/v2/dao"
	"soliveboa/youtuber/v2/entities"
	"soliveboa/youtuber/v2/rabbit"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/api/option"
	"google.golang.org/api/youtube/v3"
)

var apiKey string

// MessageResponseVideo from youtuber
type MessageResponseVideo struct {
	LocationName string                    `json:"location"`
	Videos       youtube.VideoListResponse `json:"videos"`
}

// Youtube structu for the service
type Youtube struct {
}

// NewYotubeService - creates a new instance
func NewYotubeService() Youtube {
	return Youtube{}
}

var totalAlreadyProcessed = 0
var categoryID = ""

// RunService - retrieve the videos from search list
func (y Youtube) RunService(k string, videoCategory string) error {

	// set the keys
	apiKey = k
	categoryID = videoCategory

	flag.Parse()

	ctx := context.Background()

	youtubeService, err := youtube.NewService(ctx, option.WithAPIKey(apiKey))

	if err != nil {
		return err
	}

	// get parameters list
	pl := entities.GetParametersList()

	// create the call actions
	call := youtubeService.Search.List(pl.Part)
	call.RegionCode(pl.RegionCode)
	call.Type(pl.VideoType)
	call.EventType(pl.EventType)
	call.MaxResults(pl.MaxResults)
	call.RelevanceLanguage(pl.Language)
	call.PublishedAfter(pl.PublishedAfter)
	call.Order(pl.Order)
	call.VideoCategoryId(videoCategory)
	call.Fields("prevPageToken,nextPageToken,items(id(videoId))")

	// run paged result
	err = call.Pages(ctx, addPaginedResults)

	if err != nil {
		return err
	}

	// before move on , we delete the remaining data on the search control collection. It means that all the page were looked up properly

	var d = dao.SearchResultControl{}
	d.Connect("mongodb://127.0.0.1:27017", "soliveboa")
	err = d.RemoveAll()

	if err != nil {
		logrus.Warning("Error to clean collection search control")
	}

	logrus.WithFields(logrus.Fields{
		"Category": categoryID,
	}).Info("Finished pages from search list")

	return nil

}

// ListOfIdsFromSearch struct
type ListOfIdsFromSearch struct {
	IDs []string `json:"ids"`
}

func addPaginedResults(values *youtube.SearchListResponse) error {

	totalAlreadyProcessed++

	logrus.WithFields(logrus.Fields{
		"Category": categoryID,
	}).Info("Processing pages from search list")

	var dd = dao.SearchResultControl{}
	dd.Connect("mongodb://127.0.0.1:27017", "soliveboa")
	_ = dd.RemoveAll()

	if len(values.Items) < 0 {
		logrus.Warn("The message receive from API doesnt't have any item")
		return nil
	}

	// define id array
	var id []string

	// looping through the items
	for key := range values.Items {

		if values.Items[key].Id.VideoId == "" {
			logrus.Warning("ATTENTION: the video ID is null")
		}

		id = append(id, values.Items[key].Id.VideoId)
	}

	// define the list
	listID := &ListOfIdsFromSearch{IDs: id}

	fmt.Printf("%+v\n", listID)
	fmt.Println("")
	fmt.Println(values.NextPageToken)

	// send the message to rabbit
	err := sendResponse(listID)

	if err != nil {

		logrus.WithFields(logrus.Fields{
			"error": err,
		}).Error("Error to send message to queue")

	}

	var next = values.NextPageToken
	var prev = values.PrevPageToken

	// justString := strings.Join(id, ",")

	var d = dao.SearchResultControl{}
	data := dao.SearchResultControlModel{NextPageToken: next, PrevPageToken: prev, InsertedAt: time.Now()}

	d.Connect("mongodb://127.0.0.1:27017", "soliveboa")
	_, _ = d.Create(data)

	return nil
}

func sendResponse(a *ListOfIdsFromSearch) error {

	v, err := json.Marshal(a)

	if err != nil {
		logrus.WithFields(logrus.Fields{
			"error": err,
		}).Error("Error to serialize video list")

		return err
	}

	service := rabbit.New()
	conn, err := service.Connect()

	if err != nil {
		logrus.WithFields(logrus.Fields{
			"error": err,
		}).Error("Error to connect to the broker")

		return err
	}

	exchange, err := conn.Exchange("to.youtuber.videos")

	if err != nil {
		logrus.WithFields(logrus.Fields{
			"error":    err,
			"exchange": "to.youtuber.videos",
		}).Error("Error to declare exchange")

		return err
	}

	_, err = exchange.Publish(v)

	if err != nil {
		logrus.WithFields(logrus.Fields{
			"error": err,
		}).Error("Error to publish the message")

		return err
	}

	logrus.Info("Page has been sent to queue")

	return nil

}

// SearchVideoByID - list video details by ID
func (y Youtube) SearchVideoByID(videoID string, k string) error {

	apiKey = k

	flag.Parse()

	ctx := context.Background()

	youtubeService, err := youtube.NewService(ctx, option.WithAPIKey(apiKey))

	if err != nil {
		// define the which kind of error
		logrus.WithFields(logrus.Fields{
			"ids":    videoID,
			"action": "video",
			"err":    err.Error(),
		}).Error("Erro to connect to youtube service - video")

		return err
	}

	p := entities.GetParametersVideo()

	call := youtubeService.Videos.List(p.Part)
	call.Id(videoID)
	call.MaxResults(50)
	call.Fields("items(id,snippet(publishedAt,channelId,title,description,thumbnails,channelTitle,categoryId,liveBroadcastContent),statistics,player,liveStreamingDetails)")

	response, err := call.Do()

	if err != nil {
		return err
	}

	for key := range response.Items {

		if response.Items[key].Id != "" {

			var v youtube.VideoListResponse
			v.Items = append(v.Items, response.Items[key])

			// send the message
			message := &MessageResponseVideo{
				LocationName: "undefined",
				Videos:       v,
			}

			_ = y.ProcessVideo(message)
		}
	}

	//return *response, nil
	return nil

}

// ProcessVideo - method
func (y Youtube) ProcessVideo(r *MessageResponseVideo) error {

	v, err := json.Marshal(r)

	if err != nil {
		logrus.WithFields(logrus.Fields{
			"error":  err,
			"action": "video",
		}).Error("Error to serialize video items")

		return err
	}

	service := rabbit.New()
	conn, err := service.Connect()

	if err != nil {
		logrus.WithFields(logrus.Fields{
			"error":  err,
			"action": "video",
		}).Error("Error to connect to the broker")

		return err
	}

	exchange, err := conn.Exchange("to.processor.post")

	if err != nil {
		logrus.WithFields(logrus.Fields{
			"error":    err,
			"action":   "video",
			"exchange": "to.processor.post",
		}).Error("Error to declare exchange")

		return err
	}

	_, err = exchange.Publish(v)

	if err != nil {
		logrus.WithFields(logrus.Fields{
			"error":  err,
			"action": "video",
		}).Error("Error to publish the message")

		return err
	}

	logrus.WithFields(logrus.Fields{
		"id":     r.Videos.Items[0].Id,
		"action": "video",
	}).Info("Page has been sent to queue")

	return nil
}
