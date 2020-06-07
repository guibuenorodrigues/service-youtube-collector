package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"soliveboa/youtuber/v2/dao"
	"soliveboa/youtuber/v2/entities"
	"strconv"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

var (
	logPath     = "log.txt"
	environment = "development"
	authKeys    []string
	amqURL      = "amqp://guest:guest@127.0.0.1:5672"
	webserver   = "http://soliveboa.com.br"

	wg sync.WaitGroup
)

func main() {

	// init the service
	initService()

	// init log service
	logInit()

	// consultar black list no webserver (buscar no endpoint a ser disponibilizado)
	// TODO
	logrus.Info("[  *  ] Searching for videos on blacklist web ...")
	startWebBlacklistSearch()

	// buscar videos list de upcomings
	logrus.Info("[  *  ] Searching for videos on search.list by upcoming videos ...")
	startSearcher()

	logrus.Info("[  *  ] Searching for videos on playlist.list by ...")
	startPlaylist()

	// trata videos recebidos
	logrus.Info("[  *  ] Processing all the videos from the queue ...")
	consumeVideo()
}

func initService() {

	fmt.Println("")
	fmt.Println("=============================================")
	fmt.Println("The service has been started ...")
	fmt.Println("=============================================")
	fmt.Println("")
}

// BlacklistWEB struct for web blacklist
type BlacklistWEB struct {
	ID      string `json:"id"`
	IDCanal string `json:"id_canal"`
}

func startWebBlacklistSearch() {

	response, err := http.Get(webserver + "/services/get-blacklist")

	if err != nil {
		logrus.WithFields(logrus.Fields{
			"err":      err.Error(),
			"endpoint": webserver + "/services/get-blacklist",
		}).Error("Error to GET api data")

		return
	}

	// close connection
	defer response.Body.Close()

	contents, err := ioutil.ReadAll(response.Body)

	if err != nil {
		logrus.WithFields(logrus.Fields{
			"err":      err.Error(),
			"endpoint": webserver + "/services/get-blacklist",
		}).Error("Error to ready body from api data")

		return
	}

	blacklistWeb := []BlacklistWEB{}

	err = json.Unmarshal(contents, &blacklistWeb)

	if err != nil {

		logrus.WithFields(logrus.Fields{
			"err":      err.Error(),
			"endpoint": webserver + "/services/get-blacklist",
		}).Error("Error to unmarshall body from api data")

		return
	}

	// run all the data received and insert into the database
	blackListService := dao.NewBlacklistService()

	c := 0

	for key := range blacklistWeb {

		// search the channel id in the database
		search := blackListService.Show(blacklistWeb[key].IDCanal)

		// if do not found, then insert it
		if len(search.ChannelID) <= 0 {
			bl := dao.Blacklist{ChannelID: blacklistWeb[key].IDCanal}
			blackListService.Insert(bl)

			c++
		}

	}

	logrus.Info(strconv.Itoa(c) + " new blacklist item(s) ...")

}

func consumeVideo() {

	conn, err := amqp.Dial(amqURL)
	failOnError(err, "Failed to connect to RabbitMQ")

	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"to.youtuber.videos", // name
		true,                 // durable
		false,                // delete when unused
		false,                // exclusive
		false,                // no-wait
		nil,                  // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.Qos(1, 0, false)

	failOnError(err, "Failed to set QoS")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto ack
		false,  // exclusive
		false,  // no local
		false,  // no wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			err := receivedVideoData(d)

			if err == nil {
				d.Ack(false)
			} else {
				d.Reject(true)
			}
		}
	}()

	log.Printf(" [*] Waiting for logs. To exit press CTRL+C")
	<-forever
}

func receivedVideoData(d amqp.Delivery) error {

	logrus.Info("Video received from queue")

	message := ListOfIdsFromSearch{}

	if string(d.Body) == "" {
		return errors.New("The string received is empty")
	}

	err := json.Unmarshal(d.Body, &message)

	if err != nil {
		return err
	}

	// valida se o video já foi coletado em algum momento no passado
	// consulto o id no banco de dados, e se já estiver lá , não adiciono a lista de ids.
	// Assim a API irá buscar apenas os videos necessários
	// pra isso defino o dao service
	videoDao := dao.NewVideosService()
	var videos []string

	// looping pelos ids recebidos pela mensage
	for key := range message.IDs {
		// consulto no banco de dados
		v := videoDao.Show(message.IDs[key])
		// se existir então pulo o video
		if v.ID > 0 {

			logrus.WithFields(logrus.Fields{
				"id":       v.ID,
				"video_id": v.VideoID,
			}).Info("Video refused because it has already been sent!!")

			continue
		}

		// adiciono na lista de videos
		videos = append(videos, v.VideoID)
	}

	// convert into string a lista de videos. Será utilizada no campo de pesquisa do youtube
	justString := strings.Join(videos, ",")
	// justString := strings.Join(message.IDs, ",")

	// call the service
	// ys := NewYotubeService(authKeys[0])
	ys := NewYotubeService()

	err = ys.SearchVideoByID(justString, "AIzaSyDuHifom-_BKMQOMVWcbDRZkC_I3H3CgWc")

	if err != nil {
		t, err := verifyError403(err)

		if t {
			logrus.Panic("Error 403 - need to be threated")
		}

		return err
	}

	return nil

}

func startSearcher() {

	// obtem as keys de acesso da base somente na primeira execução, nas demais utiliza a variavel armazenada
	authKeys = entities.GetAuthKeys()

	if len(authKeys) <= 0 {
		logrus.WithFields(logrus.Fields{
			"autheKeysCount": "0",
		}).Error("There are no more auth keys available")

		panic(1)
	}

	is403 := false
	categoryList := []string{"10", "24", "23", "27", "34", "17"}
	//categoryList := []string{"24", "23", "27", "34", "17"}
	stoppedKey := 0

	for key, val := range categoryList {

		ys := NewYotubeService()
		err := ys.RunService(authKeys[0], val)

		if err != nil {

			if strings.Contains(err.Error(), "AP001") {
				continue
			} else {
				// check if the error is 403
				t, _ := verifyError403(err)
				// define if is or not
				is403 = t
				// save exactly the key that got 403
				stoppedKey = key
			}
		}
	}

	if is403 {
		retryList(categoryList, stoppedKey)
	}
}

func startPlaylist() {

	authKeys = entities.GetAuthKeys()

	if len(authKeys) <= 0 {
		logrus.WithFields(logrus.Fields{
			"autheKeysCount": "0",
		}).Error("There are no more auth keys available")

		panic(1)
	}

	ys := NewYotubeService()
	err := ys.RunByPlaylist(authKeys[0], "PLU12uITxBEPHVRSbtjyfmi1Klzam7qQkv")

	if err != nil {
		logrus.Error(err)

	}

}

func retryList(category []string, key int) error {

	// validate amount of keys
	if len(authKeys) <= 0 {
		logrus.WithFields(logrus.Fields{
			"autheKeysCount": "0",
		}).Error("There are no more auth keys available")

		return errors.New("There are no more auth keys available")
	}

	// retrieve the last published date
	var s dao.SearchResultControl
	s.Connect("mongodb://127.0.0.1:27017", "soliveboa")

	_, err := s.GetNextPageToken()

	if err != nil {

		logrus.Error("No last published value found on DB")
		return err
	}

	//remove key
	authKeys = authKeys[:len(authKeys)-1]

	ys := NewYotubeService()

	is403 := false
	stoppedKey := 0

	for k, v := range category {

		if k >= key {
			err = ys.RunService(authKeys[0], v)

			if err != nil {
				t, _ := verifyError403(err)
				is403 = t
				stoppedKey = key

				break
			}
		}
	}

	if is403 {
		retryList(category, stoppedKey)
	}

	// call the method to list

	return err

}

func verifyError403(err error) (bool, error) {

	if err != nil && strings.Contains(err.Error(), "403") {

		logrus.WithFields(logrus.Fields{
			"error": err,
		}).Warning("Error 403")

		return true, err

	}

	logrus.WithFields(logrus.Fields{
		"error": err,
	}).Error("Error on youtube actions")

	return false, err

}

func logInit() {

	if environment == "development" {

		Formatter := new(logrus.TextFormatter)
		Formatter.TimestampFormat = "02-01-2006 15:04:05"
		Formatter.FullTimestamp = true
		logrus.SetFormatter(Formatter)

	} else {

		f, err := os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
		Formatter := new(logrus.TextFormatter)

		Formatter.TimestampFormat = "02-01-2006 15:04:05"
		Formatter.FullTimestamp = true
		logrus.SetFormatter(Formatter)
		if err != nil {
			// Cannot open log file. Logging to stderr
			fmt.Println(err)
		} else {
			logrus.SetOutput(f)
		}

	}

	// ===================
	// EXAMPLES
	// ===================
	// logrus.Info("Some info. Earth is not flat.")
	// log.Warning("This is a warning")
	// log.Error("Not fatal. An error. Won't stop execution")
	// log.Fatal("MAYDAY MAYDAY MAYDAY. Execution will be stopped here")
	// log.Panic("Do not panic")

	// log.WithFields(log.Fields{
	// 	"animal": "walrus",
	// 	"size":   10,
	// }).Info("A group of walrus emerges from the ocean")

}

func failOnError(err error, msg string) {
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"err": err.Error(),
		}).Error(msg)
	}
}
