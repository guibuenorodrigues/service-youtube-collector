package entities

import (
	"encoding/json"
	"io/ioutil"
	"os"

	"github.com/sirupsen/logrus"
)

// Settings strcut
type Settings struct {
	List       ListParameters  `json:"list"`
	Video      VideoParameters `json:"video"`
	Auth       []string        `json:"auth"`
	Categories []string        `json:"categories"`
	Rabbit     RabbitSettings  `json:"rabbit"`
}

// ListParameters - Define the parameters to return the list
type ListParameters struct {
	Part           string `json:"part"`
	RegionCode     string `json:"regionCode"`
	VideoType      string `json:"videoType"`
	EventType      string `json:"eventType"`
	MaxResults     int64  `json:"maxResults"`
	NextToken      string `json:"nextToken"`
	Language       string `json:"language"`
	Order          string `json:"order"`
	PublishedAfter string `json:"publishedAfter"`
	Query          string `json:"query"`
	Location       string `json:"location"`
	LocationRadius string `json:"locationRadius"`
}

// VideoParameters - Define the parameters to return vide
type VideoParameters struct {
	Part string `json:"part"`
}

// RabRabbitSettings model
type RabbitSettings struct {
	Hostname string `json:"hostname"`
	Port     string `json:"port"`
	User     string `json:"user"`
	Pass     string `json:"pass"`
}

var dataSettings Settings

func loadData() {
	jsonFile, err := os.Open("settings.json")
	// if we os.Open returns an error then handle it
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"file":  "settings.json",
			"error": err.Error(),
		}).Error("Error to open settings.json file")

	}

	// defer the closing of our jsonFile so that we can parse it later on
	defer jsonFile.Close()

	// retrieve json settings
	byteValue, err := ioutil.ReadAll(jsonFile)

	// fmt.Println(string(byteValue))

	if err != nil {
		logrus.WithFields(logrus.Fields{
			"file":  "settings.json",
			"error": err.Error(),
		}).Error("Error to read the file content")
	}

	jsonFile.Close()

	// var data Settings

	err = json.Unmarshal(byteValue, &dataSettings)

	if err != nil {
		logrus.WithFields(logrus.Fields{
			"error": err.Error(),
		}).Error("Error to unmarshall data")
	}
}

// GetParametersList method
// retrieve all the parameters to run a search on a list endpoint
func GetParametersList() ListParameters {

	loadData()
	return dataSettings.List
}

// GetParametersVideo method
func GetParametersVideo() VideoParameters {

	loadData()
	return dataSettings.Video
}

// GetAuthKeys method
func GetAuthKeys() []string {
	loadData()
	return dataSettings.Auth
}

func GetCategories() []string {
	loadData()
	return dataSettings.Categories
}

func GetRabbitSettings() RabbitSettings {
	loadData()
	return dataSettings.Rabbit
}

func GetRabbitConnString() string {

	rabbit := GetRabbitSettings()

	return "amqp://" + rabbit.User + ":" + rabbit.Pass + "@" + rabbit.Hostname + ":" + rabbit.Port

}
