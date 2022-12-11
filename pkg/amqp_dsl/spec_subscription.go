package amqp_dsl

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type (
	Subscription struct {
		// either one is required
		Queue     *Queue
		QueueName string

		ConsumerTag string
		NoLocal     bool
		AutoAck     bool
		Exclusive   bool
		NoWait      bool
	}
)

func (spec Subscription) Consume(channel *amqp.Channel) (<-chan amqp.Delivery, error) {
	if channel == nil {
		return nil, ErrChannelCanNotBeNil
	}

	if spec.QueueName == "" && (spec.Queue == nil || spec.Queue.Name == "") {
		return nil, fmt.Errorf("%w: either Queue or QueueName is required", ErrMissingRequiredArg)
	}

	queueName := spec.QueueName
	if spec.Queue != nil {
		if err := spec.Queue.Apply(channel); err != nil {
			return nil, fmt.Errorf("spec.Queue.Apply > %w", err)
		}
		queueName = spec.Queue.Name
	}

	msgs, err := channel.Consume(queueName, spec.ConsumerTag, spec.AutoAck, spec.Exclusive, spec.NoLocal, spec.NoWait, nil)
	if err != nil {
		return nil, fmt.Errorf("channel.Consume(%s) > %w", spec.Queue.Name, err)
	}

	return msgs, nil
}
