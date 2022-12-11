package amqp_dsl

import (
	"errors"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type (
	Spec interface {
		Apply(*amqp.Channel) error
	}
)

var (
	ErrMissingRequiredArg = errors.New("missing required arg")
	ErrChannelCanNotBeNil = errors.New("channel can not be nil")
	ErrEmptySpecsList     = errors.New("at least single Spec is required")
)

func Declare(channel *amqp.Channel, specs ...Spec) error {
	if channel == nil {
		return ErrChannelCanNotBeNil
	}

	if len(specs) < 1 {
		return ErrEmptySpecsList
	}

	if err := channel.Tx(); err != nil {
		return fmt.Errorf("channel.Tx > %w", err)
	}
	for _, spec := range specs {
		if err := spec.Apply(channel); err != nil {
			return err
		}
	}
	if err := channel.TxCommit(); err != nil {
		return fmt.Errorf("channel.TxCommit > %w", err)
	}
	return nil
}
