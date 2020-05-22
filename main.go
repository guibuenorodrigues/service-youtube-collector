package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"soliveboa/youtuber/v2/dao"
	"soliveboa/youtuber/v2/entities"
	"soliveboa/youtuber/v2/rabbit"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

var (
	logPath     = "log.txt"
	environment = "development"
	authKeys    []string
	amqURL      = "amqp://guest:guest@127.0.0.1:5672"
	wg          sync.WaitGroup
)

func main() {

	logInit()

	logrus.Info("Service has been started")

	wg.Add(1)
	go startSearcher()
	//go teste()
	wg.Add(1)
	go consumeVideo()

	wg.Wait()

}

func consumeVideo() {

	defer wg.Done()

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
			}
		}
	}()

	log.Printf(" [*] Waiting for logs. To exit press CTRL+C")
	<-forever
}

func receivedVideoData(d amqp.Delivery) error {
	// d ListOfIdsFromSearch

	message := ListOfIdsFromSearch{}

	if string(d.Body) == "" {
		return errors.New("The string received is empty")
	}

	err := json.Unmarshal(d.Body, &message)

	if err != nil {
		return err
	}

	// obtem as keys de acesso da base somente na primeira execução, nas demais utiliza a variavel armazenada
	authKeys = entities.GetAuthKeys()

	if len(authKeys) <= 0 {
		logrus.WithFields(logrus.Fields{
			"autheKeysCount": "0",
		}).Error("There are no more auth keys available")

		return errors.New("There are no more auth keys available")
	}

	// convert into string
	justString := strings.Join(message.IDs, ",")

	// call the service
	ys := NewYotubeService(authKeys[0])
	err = ys.SearchVideoByID(justString)

	return err

}

// TODO REMOVER DEPOIS
func teste() {

	for {

		service := rabbit.New()
		conn, err := service.Connect()

		if err != nil {
			logrus.WithFields(logrus.Fields{
				"error": err,
			}).Error("Error to connect to the broker")
		}

		exchange, err := conn.Exchange("to.youtuber.videos")

		if err != nil {
			logrus.WithFields(logrus.Fields{
				"error":    err,
				"exchange": "to.youtuber.videos",
			}).Error("Error to declare exchange")
		}

		_, err = exchange.Publish([]byte("{\"ids\": [\"BiGh9VXC53M\",\"_jBNp7Vrc5s\",\"P20K3YmfmnQ\"]}"))

		if err != nil {
			logrus.WithFields(logrus.Fields{
				"error": err,
			}).Error("Error to publish the message")
		}

		logrus.Info("Page has been sent to queue")

		time.Sleep(500 * time.Millisecond)
	}

}

func startSearcher() {

	defer wg.Done()

	logrus.Info("() Starter ...")

	// obtem as keys de acesso da base somente na primeira execução, nas demais utiliza a variavel armazenada
	authKeys = entities.GetAuthKeys()

	// looping for regions
	loc := entities.GetLocations()

	// looping for every location
	for locKey := range loc {

		logrus.Info("[*] Region: " + loc[locKey].Name)

		// time.Sleep(1 * time.Hour)

		if len(authKeys) <= 0 {
			logrus.WithFields(logrus.Fields{
				"autheKeysCount": "0",
			}).Error("There are no more auth keys available")

			panic(1)
		}

		// call the service
		ys := NewYotubeService(authKeys[0])

		// call the method to list
		err := ys.RunService(loc[locKey].Coordenates, loc[locKey].Radius)

		// validate error
		if err != nil {
			// check if is 403
			t, _ := verifyError403(err)

			// yes, it's
			if t {
				_ = retryList(loc[locKey].Coordenates, loc[locKey].Radius)

			} else {
				logrus.WithFields(logrus.Fields{
					"error": err,
				}).Warning("Error looping locations")
			}

		}

	}

}

func retryList(location string, radius string) error {

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

	last, err := s.GetNextPageToken()

	if err != nil {

		logrus.Error("No last published value found on DB")
		return err
	}

	//remove key
	authKeys = authKeys[:len(authKeys)-1]

	ys := NewYotubeService(authKeys[0])

	// call the method to list
	err = ys.RunService(location, radius, last)

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
