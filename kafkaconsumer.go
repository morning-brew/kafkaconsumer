package kafkaconsumer

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/segmentio/kafka-go"
)

const DefaultBufSize = 100

var (
	ErrChannelClosed = errors.New("CHANNEL CLOSED")
	ErrChannelFull   = errors.New("CHANNEL_FULL")
)

type ErrorFunc func(error)

type Consumer[T any] struct {
	ctx            context.Context
	reader         *kafka.Reader
	MessageChannel chan T
	strict         bool
	wg             sync.WaitGroup
	once           sync.Once
	stopped        bool
	stoppedLock    sync.RWMutex
	onError        ErrorFunc
	parseMessage   func([]byte) T
}

func NewConsumer[T any](ctx context.Context, config Config, bufSize int64, strict bool, parseMessageFunc func([]byte) T, onError ErrorFunc) *Consumer[T] {
	if bufSize < 0 {
		bufSize = DefaultBufSize
	}
	if onError == nil {
		onError = func(error) {}
	}
	if parseMessageFunc == nil {
		parseMessageFunc = func(data []byte) T {
			var parsed T
			_ = json.Unmarshal(data, &parsed)
			return parsed
		}
	}
	return &Consumer[T]{
		reader:         config.Build(),
		MessageChannel: make(chan T, bufSize),
		strict:         strict,
		wg:             sync.WaitGroup{},
		once:           sync.Once{},
		stoppedLock:    sync.RWMutex{},
		onError:        onError,
		parseMessage:   parseMessageFunc,
	}
}

func (c *Consumer[T]) Start() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.consume(c.ctx)
	}()
}

func (c *Consumer[T]) consume(ctx context.Context) error {
	for {
		c.stoppedLock.RLock()
		if c.stopped {
			return ErrChannelClosed
		}
		c.stoppedLock.RUnlock()
		m, err := c.reader.ReadMessage(ctx)
		if err != nil {
			c.onError(err)
			continue
		}
		msg := c.parseMessage(m.Value)
		if c.strict {
			c.MessageChannel <- msg
			continue
		}
		select {
		case c.MessageChannel <- msg:
			continue
		default:
			c.onError(ErrChannelFull)
		}
	}

}

func (c *Consumer[T]) Close() {
	c.once.Do(func() {
		c.stoppedLock.Lock()
		c.stopped = true
		close(c.MessageChannel)
		c.stoppedLock.Unlock()
	})

	c.wg.Wait()
}
