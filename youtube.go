package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"soliveboa/youtuber/v2/dao"
	"soliveboa/youtuber/v2/entities"
	_errors "soliveboa/youtuber/v2/errors"
	"soliveboa/youtuber/v2/rabbit"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/api/option"
	"google.golang.org/api/youtube/v3"
)

var apiKey string

// MessageResponseVideo from youtuber
type MessageResponseVideo struct {
	Source string                    `json:"source"`
	Videos youtube.VideoListResponse `json:"videos"`
}

// Youtube structu for the service
type Youtube struct {
}

// ListOfIdsFromSearch struct
type ListOfIdsFromSearch struct {
	Source string   `json:"source"`
	IDs    []string `json:"ids"`
}

// NewYotubeService - creates a new instance
func NewYotubeService() Youtube {
	return Youtube{}
}

var totalAlreadyProcessed = 0
var categoryID = ""
var totalNull = 0

// RunByPlaylist search by playlist ID
func (y Youtube) RunByPlaylist(k string, playlistID string) error {

	flag.Parse()

	apiKey = k

	ctx := context.Background()

	youtubeService, err := youtube.NewService(ctx, option.WithAPIKey(apiKey))

	if err != nil {
		return err
	}

	call := youtubeService.PlaylistItems.List("snippet")
	call.PlaylistId(playlistID)
	call.MaxResults(50)

	err = call.Pages(ctx, addPlaylistPaginedResults)

	_errors.HandleError("Error call.Pages() playlist", err, false)

	return err

}

func addPlaylistPaginedResults(values *youtube.PlaylistItemListResponse) error {

	totalAlreadyProcessed++

	if len(values.Items) < 0 {
		logrus.Warn("The message receive from API doesnt't have any item: Playlist!!")
		return nil
	}

	//define tokens
	// var nextToken = values.NextPageToken
	//var prev = values.PrevPageToken

	// define id array
	var id []string

	// looping through the items
	for key := range values.Items {

		vid := values.Items[key].Snippet.ResourceId.VideoId

		if vid == "" {
			logrus.Warning("ATTENTION: the video ID is null")
		}

		id = append(id, vid)
	}

	// define the list
	listID := &ListOfIdsFromSearch{Source: "playlist", IDs: id}

	// check if the return is null
	if len(listID.IDs) <= 0 {

		// increment
		totalNull++
		fmt.Printf("null count: %v\n", totalNull)

		// tokenRec := dao.NewTokenRecoveryService()
		// b := dao.TokenRecovery{NextToken: nextToken}
		// tokenRec.Insert(b)

		// is more than 5
		if totalNull >= 5 {
			return errors.New("AP001 - Reached null api response")
		}

		return nil
	}

	// reset count null var because the last one was not empty
	totalNull = 0

	// send the message to rabbit
	err := sendResponse(listID)

	if err != nil {

		logrus.WithFields(logrus.Fields{
			"error": err,
		}).Error("Error to send message to queue")

	}

	logrus.WithFields(logrus.Fields{
		"Proccessed": totalAlreadyProcessed,
	}).Info(" [~] Total of videos proccessed ...")

	return nil

}

// RunService - retrieve the videos from search list
func (y Youtube) RunService(k string, videoCategory string) error {

	totalNull = 0

	// set the keys
	apiKey = k
	categoryID = videoCategory

	logrus.WithFields(logrus.Fields{
		"Category": categoryID,
	}).Info("[<] Started pages from search list")

	flag.Parse()

	ctx := context.Background()

	youtubeService, err := youtube.NewService(ctx, option.WithAPIKey(apiKey))

	if err != nil {
		return err
	}

	// get parameters list
	pl := entities.GetParametersList()
	publishedAfter := time.Now().AddDate(0, 0, -1).Format(time.RFC3339)

	if pl.PublishedAfter != "" {
		publishedAfter = pl.PublishedAfter
	}

	// create the call actions
	call := youtubeService.Search.List(pl.Part)
	call.RegionCode(pl.RegionCode)
	call.Type(pl.VideoType)
	call.EventType(pl.EventType)
	call.MaxResults(pl.MaxResults)
	call.RelevanceLanguage(pl.Language)
	call.PublishedAfter(publishedAfter)
	call.Order(pl.Order)
	call.VideoCategoryId(videoCategory)
	call.Fields("prevPageToken,nextPageToken,items(id(videoId),snippet(channelId))")

	// run paged result
	err = call.Pages(ctx, addPaginedResults)

	if err != nil {
		return err
	}

	// before move on , we delete the remaining data on the search control collection. It means that all the page were looked up properly

	logrus.WithFields(logrus.Fields{
		"Category": categoryID,
	}).Info("[>] Finished pages from search list")

	return nil

}

func addPaginedResults(values *youtube.SearchListResponse) error {

	totalAlreadyProcessed++

	if len(values.Items) < 0 {
		logrus.Warn("The message receive from API doesnt't have any item")
		return nil
	}

	// define id array
	var id []string

	// looping through the items
	for key := range values.Items {

		vid := values.Items[key].Id.VideoId

		if vid == "" {
			logrus.Warning("ATTENTION: the video ID is null")
		}

		id = append(id, vid)
	}

	// define the list
	listID := &ListOfIdsFromSearch{IDs: id}

	// check if the return is null
	if len(listID.IDs) <= 0 {

		// increment
		totalNull++
		fmt.Printf("null count: %v\n", totalNull)

		// is more than 5
		if totalNull >= 5 {
			return errors.New("AP001 - Reached null api response")
		}

		return nil
	}

	// reset count null var because the last one was not empty
	totalNull = 0

	// send the message to rabbit
	err := sendResponse(listID)

	if err != nil {

		logrus.WithFields(logrus.Fields{
			"error": err,
		}).Error("Error to send message to queue")

	}

	logrus.WithFields(logrus.Fields{
		"Proccessed": totalAlreadyProcessed,
	}).Info(" [~] Total of list proccessed ...")

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
func (y Youtube) SearchVideoByID(source string, videoID string, k string) error {

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
				Source: source,
				Videos: v,
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

	// salva no banco de dados o ID
	videoService := dao.NewVideosService()
	video := dao.Videos{VideoID: r.Videos.Items[0].Id, ChannelID: r.Videos.Items[0].Snippet.ChannelId}
	videoService.Insert(video)

	logrus.WithFields(logrus.Fields{
		"id":     r.Videos.Items[0].Id,
		"action": "video",
	}).Info("Page has been sent to queue")

	return nil
}
