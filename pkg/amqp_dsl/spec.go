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
)

func Declare(channel *amqp.Channel, specs ...Spec) error {
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
