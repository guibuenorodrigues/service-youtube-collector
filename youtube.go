package main

import (
	"context"
	"encoding/json"
	"flag"
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
func NewYotubeService(k string) Youtube {
	// set the keys
	apiKey = k

	return Youtube{}
}

// RunService - retrieve the videos from search list
func (y Youtube) RunService(location string, radius string, opt ...string) error {

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
	call.Fields("nextPageToken,items(id(videoId))")
	call.Location(location)
	call.LocationRadius(radius)

	// if there is published before
	if len(opt) > 0 {
		call.PageToken(opt[0])
	}

	// run paged result
	err = call.Pages(ctx, addPaginedResults)

	if err != nil {
		return err
	}

	return nil

}

// ListOfIdsFromSearch struct
type ListOfIdsFromSearch struct {
	IDs []string `json:"ids"`
}

func addPaginedResults(values *youtube.SearchListResponse) error {

	if len(values.Items) < 0 {
		logrus.Warn("The message receive from API doesnt't have any item")
		return nil
	}

	// define id array
	var id []string

	// looping through the items
	for key := range values.Items {
		id = append(id, values.Items[key].Id.VideoId)
	}

	// define the list
	listID := &ListOfIdsFromSearch{IDs: id}

	// send the message to rabbit
	err := sendResponse(listID)

	if err != nil {

		logrus.WithFields(logrus.Fields{
			"error": err,
		}).Error("Error to send message to queue")

	}

	// as datas vem por ordem da mais recente para a mais antiga
	// 2020-05-20
	// 2020-05-19
	// 2020-05-18
	// 2020-05-17
	// 2020-05-16

	// salvar a ultima data no banco de dados
	last := values.NextPageToken

	var d = dao.SearchResultControl{}
	data := dao.SearchResultControlModel{NextPageToken: last, InsertedAt: time.Now()}

	d.Connect("mongodb://127.0.0.1:27017", "soliveboa")
	insertedID, _ := d.Create(data)

	logrus.WithFields(logrus.Fields{
		"insertedID":    insertedID,
		"nextPageToken": last,
	}).Info("youtube last next token has been inserted")

	time.Sleep(1 * time.Hour)

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
func (y Youtube) SearchVideoByID(videoID string) error {

	flag.Parse()

	ctx := context.Background()

	youtubeService, err := youtube.NewService(ctx, option.WithAPIKey(apiKey))

	if err != nil {
		// define the which kind of error
		logrus.WithFields(logrus.Fields{
			"ids": videoID,
			"err": err.Error(),
		}).Error("Erro to connect to youtube service - video")

		return err
	}

	p := entities.GetParametersVideo()

	call := youtubeService.Videos.List(p.Part)
	call.Id(videoID)
	call.Fields("items(id,snippet(publishedAt,channelId,title,description,thumbnails,channelTitle,categoryId,liveBroadcastContent),statistics,player,liveStreamingDetails)")

	response, err := call.Do()

	if err != nil {
		// define the which kind of error
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

	time.Sleep(1 * time.Hour)

	//return *response, nil
	return nil

}

// ProcessVideo - method
func (y Youtube) ProcessVideo(r *MessageResponseVideo) error {

	v, err := json.Marshal(r)

	if err != nil {
		logrus.WithFields(logrus.Fields{
			"error": err,
		}).Error("Error to serialize video items")

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

	exchange, err := conn.Exchange("to.processor.post")

	if err != nil {
		logrus.WithFields(logrus.Fields{
			"error":    err,
			"exchange": "to.processor.post",
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

	logrus.WithFields(logrus.Fields{
		"id": r.Videos.Items[0].Id,
	}).Info("Page has been sent to queue")

	return nil
}
