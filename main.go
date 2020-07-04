package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"soliveboa/youtuber/v2/dao"
	"soliveboa/youtuber/v2/entities"
	_errors "soliveboa/youtuber/v2/errors"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

var (
	logPath      = "log_youtuber.log"
	environment  = "development"
	authKeys     []string
	amqURL       string
	webserver    string
	endpoints    entities.WebServerEndpoints
	nonFlag      = false
	flagPlaylist bool
	flagSearcher bool
	flagVideo    bool
)

func main() {

	// init the service
	initService()

	// init log service
	logInit()

	// handle channels from web server
	channelsWebServer()

	if nonFlag || flagPlaylist {
		startPlaylist()
	}

	// buscar videos list de upcomings
	if nonFlag || flagSearcher {
		logrus.Info("[  *  ] Searching for videos on search.list by upcoming videos and channels ...")
		channelsSearch()
	}

	// // trata videos recebidos
	if nonFlag || flagVideo {
		logrus.Info("[  *  ] Processing all the videos from the queue ...")
		consumeVideo()
	}
}

func initService() {

	flag.BoolVar(&flagPlaylist, "p", false, "runs only the playlist")
	flag.BoolVar(&flagSearcher, "s", false, "runs only the searcher")
	flag.BoolVar(&flagVideo, "v", false, "runs only the videos available in the consumer")
	flag.Parse()

	// if there is no flag , consider as running all the robot
	if !flagPlaylist && !flagSearcher && !flagVideo {
		fmt.Println("--> no flag defined. Run full application")
		nonFlag = true
	}

	fmt.Println("")
	fmt.Println("=============================================")
	fmt.Println("        The service has been started         ")
	fmt.Println("=============================================")
	fmt.Println("")

	fmt.Println("loading rabbit settings...")
	amqURL = entities.GetRabbitConnString()

	fmt.Println("loading environment...")
	environment = entities.GetEnv()

	fmt.Println("loading web server settings...")
	webserver = entities.GetWebServer().BaseURL
	endpoints = entities.GetWebServerEndpoints()

	fmt.Println("loading authoziation keys...")
	authorization := dao.NewAuthorizationService()
	tokens := authorization.Index()

	for _, val := range tokens {
		authKeys = append(authKeys, val.Token)
	}

}

func channelsWebServer() {
	logrus.Info("[  *  ] Searching for channels in the web server ...")
	err := NewChannelWebListService().UpdateChannelsFromWebServer()
	_errors.HandleError("Error to update channels from web server", err, false)
}

func channelsSearch() {
	logrus.Info("[  *  ] Searching videos from channel list")
	err := NewChannelWebListService().SearchVideosByChannels()
	_errors.HandleError("Error to retrieve channel videos from youtube", err, false)
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

			d.Ack(false)

			if err != nil {
				//d.Reject(true)
				logrus.WithFields(logrus.Fields{
					"err": err,
				}).Warning("Message Rejected due to an error!!!")
			}
		}
	}()

	log.Printf(" [*] Waiting for logs. To exit press CTRL+C")
	<-forever
}

func receivedVideoData(d amqp.Delivery) error {

	key, err, _ := dao.GetNewKey(authKeys, false)

	if err != nil {
		return err
	}

	logrus.Info("---------------------------------> Video received from queue <---------------------------------")

	message := ListOfIdsFromSearch{}

	if string(d.Body) == "" {
		return errors.New("The string received is empty")
	}

	err = json.Unmarshal(d.Body, &message)

	if err != nil {
		return err
	}

	// valida se o video já foi coletado em algum momento no passado
	// consulto o id no banco de dados, e se já estiver lá , não adiciono a lista de ids.
	// Assim a API irá buscar apenas os videos necessários
	// pra isso defino o dao service
	videoDao := dao.NewVideosService()
	var videos []string

	i := 0
	t := 0

	// looping pelos ids recebidos pela mensage
	for _, val := range message.IDs {
		// consulto no banco de dados
		v := videoDao.Show(val)
		// se existir então pulo o video
		if v.ID > 0 {

			logrus.WithFields(logrus.Fields{
				"id":       v.ID,
				"video_id": v.VideoID,
			}).Info("Video refused because it has already been sent!!")
		} else {
			// adiciono na lista de videos
			videos = append(videos, val)
			i++
		}

		t++
	}

	logrus.WithFields(logrus.Fields{
		"total":     strconv.Itoa(t),
		"proccesed": strconv.Itoa(i),
	}).Info("[==] videos to be search after remove duplicates")

	// convert into string a lista de videos. Será utilizada no campo de pesquisa do youtube
	justString := strings.Join(videos, ",")
	// justString := strings.Join(message.IDs, ",")

	// call the service
	// ys := NewYotubeService(authKeys[0])
	ys := NewYotubeService()

	err = ys.SearchVideoByID(message.Source, justString, key)

	if err != nil {
		t, err := _errors.VerifyError403(err)

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
	}

	categoryList := entities.GetCategories()

	for _, val := range categoryList {

		ys := NewYotubeService()
		err := ys.RunService(authKeys[0], val)

		if err != nil {

			if strings.Contains(err.Error(), "AP001") {
				continue
			} else {
				// check if the error is 403
				t, _ := _errors.VerifyError403(err)
				// define if is or not
				if t {
					break
				}
			}
		}
	}
}

func logInit() {

	fmt.Println("setting log configuration for: " + environment + " environment")

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

	logrus.SetLevel(logrus.DebugLevel)

	fmt.Println("")
	fmt.Println("")
	fmt.Println("")
	fmt.Println("")
	fmt.Println("")
	fmt.Println("")

	// ===================
	// EXAMPLES
	// ===================
	// logrus.Trace("Something very low level.")
	// logrus.Debug("Useful debugging information.")
	// logrus.Info("Something noteworthy happened!")
	// logrus.Warn("You should probably take a look at this.")
	// logrus.Error("Something failed but I'm not quitting.")

	// Calls os.Exit(1) after logging
	// logrus.Fatal("Bye.")

	// Calls panic() after logging
	// logrus.Panic("I'm bailing.")

}

func failOnError(err error, msg string) {
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"err": err.Error(),
		}).Error(msg)
	}
}
