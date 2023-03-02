package kafkaconsumer

import "context"

type MessageReader interface {
	ReadMessage(context.Context) ([]byte, error)
}
