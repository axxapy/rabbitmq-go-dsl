package amqp_dsl

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type (
	ChannelQos struct {
		PrefetchCount int
		PrefetchSize  int
		Global        bool
	}
)

func (spec ChannelQos) Apply(channel *amqp.Channel) error {
	if err := channel.Qos(spec.PrefetchCount, spec.PrefetchSize, spec.Global); err != nil {
		return fmt.Errorf("channel.ChannelQos(%+v) > %w", spec, err)
	}
	return nil
}
