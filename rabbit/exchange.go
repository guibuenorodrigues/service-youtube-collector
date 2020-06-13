package rabbit

import (
	"soliveboa/youtuber/v2/entities"

	guuid "github.com/google/uuid"

	"github.com/streadway/amqp"
)

// ServiceCall - Contains the service structure
type ServiceCall struct {
	Publisher *PublisherCall
}

// PublisherCall - Contains the publisher structure
type PublisherCall struct {
	conn         *amqp.Connection
	channel      *amqp.Channel
	url          string
	exchangeName string
	routeKey     string
}

// New - Generaters a new service methodos
func New() *ServiceCall {

	return &ServiceCall{}
}

// Connect - Create the connection
// You can pass different parameters to connect
// (hostname, port, username, password)
func (p *ServiceCall) Connect() (*PublisherCall, error) {

	u := preapreURL()

	// connect
	conn, err := amqp.Dial(u)

	// validating error
	if err != nil {
		return &PublisherCall{}, err
	}

	// create channel
	c, err := conn.Channel()

	// validating error
	if err != nil {
		return &PublisherCall{}, err
	}

	r := &PublisherCall{
		url:     u,
		conn:    conn,
		channel: c,
	}

	return r, nil
}

// Exchange declare the exchange that will be used
func (p *PublisherCall) Exchange(routeKey string) (*PublisherCall, error) {

	err := p.channel.ExchangeDeclare(
		"youtuber", // name
		"direct",   // type
		true,       // durable
		false,      // auto-deleted
		false,      // internal
		false,      // no-wait
		nil,        // arguments
	)

	if err != nil {
		return p, err
	}

	p.exchangeName = "youtuber"
	p.routeKey = routeKey

	return p, nil
}

// Publish message to the Exchange
func (p *PublisherCall) Publish(body []byte) (*PublisherCall, error) {

	err := p.channel.Publish(
		"youtuber", // exchange
		p.routeKey, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			DeliveryMode:  amqp.Persistent,
			CorrelationId: guuid.New().String(),
			AppId:         "service.youtuber",
			ContentType:   "application/json",
			Body:          body,
		})

	return p, err

}

func preapreURL() string {

	return entities.GetRabbitConnString()

}
