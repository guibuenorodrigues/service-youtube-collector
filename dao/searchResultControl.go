package dao

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"
	"gopkg.in/mgo.v2/bson"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	uri        = "mongodb://127.0.0.1:27017"
	dbName     = "soliveboa"
	authSource = ""
	username   = "root"
	password   = "RDCBraZil2015"
)

// SearchResultControlModel model
type SearchResultControlModel struct {
	ID            primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	NextPageToken string             `bson:"nextPageToken" json:"nextPageToken"`
	PrevPageToken string             `bson:"prevPageToken" json:"prevPageToken"`
	InsertedAt    time.Time          `bson:"insertedAt" json:"insertedAt"`
}

// SearchResultControl service
type SearchResultControl struct {
	// LastPublishedAfter string
}

var db *mongo.Database

const (
	//COLLECTION exports
	COLLECTION = "search_result_control"
)

// MongoService type exported

// Connect returns a new connection
func (s *SearchResultControl) Connect(dbURI string, d string) {

	uri = dbURI
	dbName = d

	// define credentials
	var cred options.Credential
	cred.Username = username
	cred.Password = password

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

	defer cancel()

	clientOption := options.Client().ApplyURI(uri).SetAuth(cred)

	client, err := mongo.Connect(ctx, clientOption)

	if err != nil {
		logrus.WithFields(logrus.Fields{"uri": uri}).Error(err)
	}

	err = client.Ping(ctx, nil)

	if err != nil {
		logrus.WithFields(logrus.Fields{"uri": uri}).Error(err)
	}

	// define db
	db = client.Database(dbName)

}

// youtube_search_result_control

// Create add the last published after processed
func (s *SearchResultControl) Create(d SearchResultControlModel) (interface{}, error) {

	collection := db.Collection(COLLECTION)

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)

	insertResult, err := collection.InsertOne(ctx, d)

	if err != nil {
		logrus.WithFields(logrus.Fields{"data": d}).Error(err)
		return nil, nil
	}

	i := insertResult.InsertedID

	return i, nil

}

// RemoveAll methods
func (s *SearchResultControl) RemoveAll() error {
	collection := db.Collection(COLLECTION)

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)

	err := collection.Drop(ctx)

	if err != nil {
		logrus.WithFields(logrus.Fields{
			"err":        err.Error(),
			"collection": COLLECTION,
		}).Error("Error removing all from collection")
		return nil
	}

	return nil
}

// GetNextPageToken method
func (s *SearchResultControl) GetNextPageToken() (string, error) {

	collection := db.Collection(COLLECTION)
	var result SearchResultControlModel

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)

	err := collection.FindOne(ctx, bson.M{}).Decode(&result)

	if err != nil {
		logrus.Error(err)
		return "", err
	}

	return result.NextPageToken, nil

}
