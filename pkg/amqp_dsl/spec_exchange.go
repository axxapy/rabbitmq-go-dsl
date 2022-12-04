package amqp_dsl

import (
	"fmt"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
)

type (
	Exchange struct {
		Name       string
		Kind       string // direct / fanout / topic / headers
		Durable    bool   // exchanges survive broker restart
		AutoDelete bool   // exchange is deleted when last queue is unbound from it
		NoWait     bool
		Args       amqp.Table
		// Internal   bool // deprecated since amqp 0-9-1
	}

	ExchangeBind struct {
		//either one is required
		Destination     *Exchange
		DestinationName string

		//either one is required
		Source     *Exchange
		SourceName string

		RoutingKey string
		NoWait     bool

		Args amqp.Table
	}
)

func (spec Exchange) Apply(channel *amqp.Channel) error {
	if spec.Name == "" || strings.HasPrefix(spec.Name, "amq.") {
		return nil // do not redeclare default exchanges
	}
	if spec.Kind == "" {
		spec.Kind = "direct"
	}
	if err := channel.ExchangeDeclare(spec.Name, spec.Kind, spec.Durable, spec.AutoDelete, false /*spec.Internal*/, spec.NoWait, spec.Args); err != nil {
		return fmt.Errorf("ExchangeDeclare(%+v) > %w", spec, err)
	}
	return nil
}

func (spec ExchangeBind) Apply(channel *amqp.Channel) error {
	if spec.SourceName == "" && (spec.Source == nil || spec.Source.Name == "") {
		return fmt.Errorf("%w: either Source or SourceName is required", ErrMissingRequiredArg)
	}

	if spec.DestinationName == "" && (spec.Destination == nil || spec.Destination.Name == "") {
		return fmt.Errorf("%w: either Destination or DestinationName is required", ErrMissingRequiredArg)
	}

	sourceName := spec.SourceName
	if spec.Source != nil {
		sourceName = spec.Source.Name
		if err := spec.Source.Apply(channel); err != nil {
			return fmt.Errorf("spec.Source.Apply > %w", err)
		}
	}

	destinationName := spec.DestinationName
	if spec.Destination != nil {
		destinationName = spec.Destination.Name
		if err := spec.Destination.Apply(channel); err != nil {
			return fmt.Errorf("spec.Destination.Apply > %w", err)
		}
	}

	if err := channel.ExchangeBind(destinationName, spec.RoutingKey, sourceName, spec.NoWait, spec.Args); err != nil {
		return fmt.Errorf("channel.QueueBind(%+v) > %w", spec, err)
	}

	return nil
}
