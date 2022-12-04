package amqp_dsl

import (
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type (
	Queue struct {
		Name       string
		Durable    bool
		Exclusive  bool
		AutoDelete bool
		NoWait     bool

		// amqp.Table arguments
		Lazy                 bool          // "x-queue-mode": "lazy",
		Mode                 string        // "x-queue-mode": "lazy",
		MessageTTL           time.Duration // "x-message-ttl": 100, in seconds
		DeadLetterExchange   *Exchange     // "x-dead-letter-exchange": "name",
		DeadLetterRoutingKey string        // "x-dead-letter-routing-key": "key value",
		DeadLetterStrategy   string        // "dead-letter-strategy": "at-least-once"
		Overflow             string        // "x-overflow": "reject-publish"

		Args amqp.Table
	}

	QueueBind struct {
		//either one is required
		Queue     *Queue
		QueueName string

		//either one is required
		Exchange     *Exchange
		ExchangeName string

		RoutingKey string
		NoWait     bool
		Args       amqp.Table
	}
)

func (spec Queue) Apply(channel *amqp.Channel) error {
	if spec.Args == nil {
		spec.Args = make(amqp.Table)
	}

	if spec.DeadLetterExchange != nil {
		if err := spec.DeadLetterExchange.Apply(channel); err != nil {
			return fmt.Errorf("spec.DeadLetterExchange.Apply > %w", err)
		}

		spec.Args["x-dead-letter-exchange"] = spec.DeadLetterExchange.Name
	}

	if spec.DeadLetterStrategy != "" {
		spec.Args["dead-letter-strategy"] = spec.DeadLetterStrategy
	}
	if spec.Overflow != "" {
		spec.Args["x-overflow"] = spec.Overflow
	}
	if spec.DeadLetterRoutingKey != "" {
		spec.Args["x-dead-letter-routing-key"] = spec.DeadLetterRoutingKey
	}
	if spec.Mode != "" {
		spec.Args["x-queue-mode"] = spec.Mode
	}
	if spec.Lazy {
		spec.Args["x-queue-mode"] = "lazy"
	}
	if spec.MessageTTL > 0 {
		spec.Args["x-message-ttl"] = spec.MessageTTL.Milliseconds()
	}

	if _, err := channel.QueueDeclare(spec.Name, spec.Durable, spec.AutoDelete, spec.Exclusive, spec.NoWait, spec.Args); err != nil {
		return fmt.Errorf("QueueDeclare(%+v) > %w", spec, err)
	}

	return nil
}

func (spec QueueBind) Apply(channel *amqp.Channel) error {
	if spec.QueueName == "" && (spec.Queue == nil || spec.Queue.Name == "") {
		return fmt.Errorf("%w: either Queue or QueueName is required", ErrMissingRequiredArg)
	}

	if spec.ExchangeName == "" && (spec.Exchange == nil || spec.Exchange.Name == "") {
		return fmt.Errorf("%w: either Exchange or ExchangeName is required", ErrMissingRequiredArg)
	}

	queueName := spec.QueueName
	if spec.Queue != nil {
		queueName = spec.Queue.Name
		if err := spec.Queue.Apply(channel); err != nil {
			return fmt.Errorf("spec.Queue.Apply > %w", err)
		}
	}

	exchangeName := spec.ExchangeName
	if spec.Exchange != nil {
		exchangeName = spec.Exchange.Name
		if err := spec.Exchange.Apply(channel); err != nil {
			return fmt.Errorf("spec.Exchange.Apply > %w", err)
		}
	}

	if err := channel.QueueBind(queueName, spec.RoutingKey, exchangeName, spec.NoWait, spec.Args); err != nil {
		return fmt.Errorf("channel.QueueBind(%+v) > %w", spec, err)
	}

	return nil
}
