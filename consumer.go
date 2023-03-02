package kafkaconsumer

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
)

const DefaultBufSize = 100

var (
	ErrChannelClosed = errors.New("CHANNEL CLOSED")
	ErrChannelFull   = errors.New("CHANNEL_FULL")
)

type ErrorFunc func(error)

type Consumer[T any] struct {
	ctx            context.Context
	reader         MessageReader
	MessageChannel chan T
	strict         bool
	wg             sync.WaitGroup
	once           sync.Once
	stopped        bool
	stoppedLock    sync.RWMutex
	onError        ErrorFunc
	parseMessage   func([]byte) T
	handleMessage  func(T) error
}

func NewConsumer[T any](ctx context.Context, reader MessageReader, bufSize int64, strict bool, parseMessageFunc func([]byte) T, onError ErrorFunc, handleMessage func(T) error) *Consumer[T] {
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
		reader:         reader,
		MessageChannel: make(chan T, bufSize),
		strict:         strict,
		wg:             sync.WaitGroup{},
		once:           sync.Once{},
		stoppedLock:    sync.RWMutex{},
		onError:        onError,
		parseMessage:   parseMessageFunc,
		handleMessage:  handleMessage,
	}
}

func (c *Consumer[T]) Start() {
	// start the consumer in a go func
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.consume(c.ctx)
	}()
	// if we are handling the messages, start this in a go func as well
	if c.handleMessage != nil {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			c.pollMessages()
		}()
	}
}

func (c *Consumer[T]) consume(ctx context.Context) error {
	for {
		// check to see if the channel is closed
		c.stoppedLock.RLock()
		if c.stopped {
			return ErrChannelClosed
		}
		c.stoppedLock.RUnlock()

		dataBytes, err := c.reader.ReadMessage(ctx)
		if err != nil {
			c.onError(err)
			continue
		}
		msg := c.parseMessage(dataBytes)

		// if in strict mode, block until the channel is open
		if c.strict {
			c.MessageChannel <- msg
			continue
		}

		// otherwise drop messages that we can't consume fast enough
		select {
		case c.MessageChannel <- msg:
			continue
		default:
			c.onError(ErrChannelFull)
		}
	}
}

func (c *Consumer[T]) pollMessages() {
	for msg := range c.MessageChannel {
		err := c.handleMessage(msg)
		if err != nil {
			c.onError(err)
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
	// wait for the channel to drain before finishing
	c.wg.Wait()
}
