package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"soliveboa/youtuber/v2/dao"
	"soliveboa/youtuber/v2/entities"
	_errors "soliveboa/youtuber/v2/errors"
	"soliveboa/youtuber/v2/rabbit"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/api/option"
	"google.golang.org/api/youtube/v3"
)

type ChannelWebListService struct{}

type ChannelWebList struct {
	ID         int    `json:"id"`
	ChannelID  string `json:"id_canal"`
	FullSynced bool   `json:"fullSynced"`
}

func NewChannelWebListService() ChannelWebListService {

	webserver = entities.GetWebServer().BaseURL
	endpoints = entities.GetWebServerEndpoints()

	return ChannelWebListService{}
}

// UpdateChannelsFromWebServer method
// Get the channel from the web server and save them into local database
func (s ChannelWebListService) UpdateChannelsFromWebServer() error {

	response, err := http.Get(webserver + endpoints.Channels)

	if err != nil {
		logrus.WithFields(logrus.Fields{
			"err":      err.Error(),
			"endpoint": webserver + endpoints.Channels,
		}).Error("Error to GET data from the api")

		return err
	}

	// close connection
	defer response.Body.Close()

	contents, err := ioutil.ReadAll(response.Body)

	if err != nil {
		logrus.WithFields(logrus.Fields{
			"err":      err.Error(),
			"endpoint": webserver + endpoints.Channels,
		}).Error("Error to ready body from the api")

		return err
	}

	data := []ChannelWebList{}

	err = json.Unmarshal(contents, &data)

	if err != nil {

		logrus.WithFields(logrus.Fields{
			"err":      err.Error(),
			"endpoint": webserver + endpoints.Channels,
		}).Error("Error to unmarshall body from api data")

		return err
	}

	if len(data) <= 0 {
		logrus.WithFields(logrus.Fields{
			"endpoint": webserver + endpoints.Channels,
		}).Info("No data received from the web server")
	}

	// create the service
	channelService := dao.NewChannelService()
	// trucate the data from database
	channelService.Truncate()

	// insert the new data
	c := 0
	for key := range data {

		insert := dao.Channel{ChannelID: data[key].ChannelID, FullSynced: data[key].FullSynced}
		err = channelService.Insert(insert)

		if err != nil {
			logrus.WithFields(logrus.Fields{
				"err":       err.Error(),
				"channelId": data[key].ChannelID,
				"endpoint":  webserver + endpoints.Channels,
			}).Error("Error to insert channel")

			continue
		}

		c++
	}

	logrus.Info(strconv.Itoa(c) + " channels inserted...")
	return nil
}

var pageToken = ""

// SearcSearchVideosByChannels method
func (s ChannelWebListService) SearchVideosByChannels() error {

	// obtem todos os canais do banco de dados
	channels := dao.NewChannelService().Index()

	if len(channels) <= 0 {
		return errors.New("There are no channels registered in the dabase")
	}

	// executa looping por cada canal no endpoint search para obter os videos upcomings naquele canal
	for key := range channels {
		err := NewChannelWebListService().RetrieveVideos(channels[key], pageToken)
		_errors.HandleError("Error to retrieve data from playlist", err, false)

		is403, _ := _errors.VerifyError403(err)
		if is403 {
			// remove the key from the array
			_, err, keys := dao.GetNewKey(authKeys, true)
			authKeys = keys

			if err != nil {
				break
			}

		}
	}

	// verifica se o canal é full sync ou não para definir data de published after

	// envia todos os videos para a lista de processamento de videos.

	return nil
}

// RetrieveVideos method
func (s ChannelWebListService) RetrieveVideos(channel dao.Channel, nextToken string) error {

	if channel.ID == 0 {
		return nil
	}

	// key a key to use
	key, err, _ := dao.GetNewKey(authKeys, false)

	if err != nil {
		failOnError(err, "error to get key from list")
	}

	flag.Parse()

	ctx := context.Background()

	youtubeService, err := youtube.NewService(ctx, option.WithAPIKey(key))

	if err != nil {
		return err
	}

	params := entities.GetParametersList()

	call := youtubeService.Search.List(params.Part)
	call.Type(params.VideoType)
	call.MaxResults(params.MaxResults)
	call.EventType(params.EventType)
	call.Order(params.Order)
	call.Fields("prevPageToken,nextPageToken,items(id(videoId),snippet(channelId))")

	if nextToken != "" {
		call.PageToken(nextToken)
	}

	if channel.FullSynced {
		publishedAfter := time.Now().AddDate(0, 0, -1).Format(time.RFC3339)
		call.PublishedAfter(publishedAfter)
	}

	err = call.Pages(ctx, addChannelPagedResult)

	if err != nil {
		return err
	}

	pageToken = ""
	return nil
}

var countChannelNullResults = 0

func addChannelPagedResult(values *youtube.SearchListResponse) error {

	totalAlreadyProcessed++

	if len(values.Items) < 0 {
		logrus.Warn("[!] The message receive from API doesnt't have any item")
		return nil
	}

	pageToken = values.NextPageToken

	// define id array
	var id []string

	// looping through the items
	for key := range values.Items {

		vid := values.Items[key].Id.VideoId

		if vid == "" {
			logrus.Warning("[!] video item is empty")
			continue
		}

		id = append(id, vid)
	}

	// define the list
	listID := &ListOfIdsFromSearch{Source: "channel", IDs: id}

	err := handleNullChannelsResult(len(listID.IDs))

	if err != nil {
		return err
	}

	countChannelNullResults = 0

	// send the message to rabbit
	err = sendChannelMessage(listID)

	if err != nil {
		return err
	}

	return nil
}

func sendChannelMessage(a *ListOfIdsFromSearch) error {

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

func handleNullChannelsResult(totalResults int) error {

	// increment
	countChannelNullResults++
	fmt.Printf("null count: %v\n", countChannelNullResults)

	// is more than 5
	if totalNull >= 5 {
		return errors.New("AP001 - Reached null api response")
	}

	return nil

}
