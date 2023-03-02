package kafkaconsumer_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/morning-brew/kafkaconsumer"
	"github.com/stretchr/testify/assert"
)

type TestMessageReader struct {
	numMessages int
	maxMessages int
	message     []byte
	err         error
}

func (t *TestMessageReader) ReadMessage(ctx context.Context) ([]byte, error) {
	if t.numMessages < t.maxMessages {
		t.numMessages++
		return t.message, nil
	}
	return nil, t.err
}

type TestMessage struct {
	TestValue string `json:"test_value"`
}

func TestConsumerBaisc(t *testing.T) {
	type testCase struct {
		name            string
		testReader      kafkaconsumer.MessageReader
		expectedMessage TestMessage
		numMessages     int
	}

	testMessageBytes, _ := json.Marshal(TestMessage{TestValue: "testing"})

	testCases := []testCase{
		{
			name: "should be able to read messages put on the channel",
			testReader: &TestMessageReader{
				maxMessages: 1,
				message:     testMessageBytes,
				err:         fmt.Errorf("test error"),
			},
			expectedMessage: TestMessage{TestValue: "testing"},
			numMessages:     1,
		},
		{
			name: "should be able to accept multiple messages from the channel",
			testReader: &TestMessageReader{
				maxMessages: 2,
				message:     testMessageBytes,
				err:         fmt.Errorf("test error"),
			},
			expectedMessage: TestMessage{TestValue: "testing"},
			numMessages:     2,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			consumer := kafkaconsumer.NewConsumer[TestMessage](
				ctx,
				tc.testReader,
				-1,
				false,
				nil,
				nil,
				nil,
			)
			consumer.Start()
			time.Sleep(1 * time.Millisecond)
			consumer.Close()
			var actualMessages int
			for msg := range consumer.MessageChannel {
				assert.EqualValues(t, TestMessage{TestValue: "testing"}, msg)
				actualMessages++
			}
			assert.Equal(t, tc.numMessages, actualMessages)
		})
	}
}

type TestSafeReader struct {
	message []byte
}

func (t *TestSafeReader) ReadMessage(ctx context.Context) ([]byte, error) {
	return t.message, nil
}

func TestSafelyClosingChannel(t *testing.T) {
	testReader := TestSafeReader{
		message: []byte{},
	}

	t.Run("should not panic when trying to put messages on a closed channel", func(t *testing.T) {
		consumer := kafkaconsumer.NewConsumer[TestMessage](
			context.Background(),
			&testReader,
			-1,
			false,
			nil,
			nil,
			nil,
		)
		// close the channel before starting the consumer
		consumer.Close()
		// this would panic if this wasn't safe
		consumer.Start()
	})
}

func TestDroppingMessages(t *testing.T) {
	testMessageBytes, _ := json.Marshal(TestMessage{TestValue: "testing"})
	type testCase struct {
		name        string
		description string
		reader      kafkaconsumer.MessageReader
		strict      bool
		bufferSize  int64
		numMessages int
	}
	var emptyMessage = TestMessage{}

	testCases := []testCase{
		{
			name:        "should drop messages when strict mode is false and buffer is full",
			description: "there was not a good way to only test with it being able to pull one message and to be able to test with the strict mode so unfortunately 2 messages will be able to go on the channel",
			reader: &TestMessageReader{
				maxMessages: 3,
				message:     testMessageBytes,
			},
			strict:      false,
			bufferSize:  1,
			numMessages: 2, // the channel will put one on the
		},
		{
			name: "should not drop messages when strict mode is true and buffer is full",
			reader: &TestMessageReader{
				maxMessages: 3,
				message:     testMessageBytes,
			},
			strict:      true,
			bufferSize:  1,
			numMessages: 3,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			consumer := kafkaconsumer.NewConsumer[TestMessage](
				context.Background(),
				tc.reader,
				tc.bufferSize,
				tc.strict,
				nil,
				nil,
				nil,
			)
			consumer.Start()
			go func() {
				time.Sleep(1 * time.Millisecond)
				consumer.Close()
			}()
			var actualMessages int
			for msg := range consumer.MessageChannel {

				if msg != emptyMessage {
					actualMessages++
				}
			}
			assert.Equal(t, tc.numMessages, actualMessages)
		})
	}
}

type MessageHandler struct {
	messages []TestMessage
}

func (m *MessageHandler) handleMessage(msg TestMessage) error {
	m.messages = append(m.messages, msg)
	return nil
}
func TestHandleMessageFunc(t *testing.T) {
	type testCase struct {
		name            string
		testReader      kafkaconsumer.MessageReader
		expectedMessage TestMessage
		numMessages     int
	}

	testMessageBytes, _ := json.Marshal(TestMessage{TestValue: "testing"})
	testCases := []testCase{
		{
			name: "should be able to successfully call the provided handle message func",
			testReader: &TestMessageReader{
				maxMessages: 1,
				message:     testMessageBytes,
				err:         fmt.Errorf("test error"),
			},
			expectedMessage: TestMessage{TestValue: "testing"},
			numMessages:     1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testMessageHandler := &MessageHandler{
				messages: []TestMessage{},
			}
			consumer := kafkaconsumer.NewConsumer(
				context.Background(),
				tc.testReader,
				-1,
				false,
				nil,
				nil,
				testMessageHandler.handleMessage,
			)
			consumer.Start()
			time.Sleep(1 * time.Millisecond)
			consumer.Close()
			assert.Len(t, testMessageHandler.messages, tc.numMessages)
			for _, msg := range testMessageHandler.messages {
				assert.EqualValues(t, tc.expectedMessage, msg)
			}
		})
	}
}

type ErrorHandler struct {
	ErrMessage string
}

func (e *ErrorHandler) handleError(err error) {
	e.ErrMessage = err.Error()
}

func (e *ErrorHandler) handleMessage(msg TestMessage) error {
	return fmt.Errorf(msg.TestValue)
}

func TestOnErrorFunc(t *testing.T) {
	type testCase struct {
		name            string
		testReader      kafkaconsumer.MessageReader
		expectedMessage string
	}
	testMessageBytes, _ := json.Marshal(TestMessage{TestValue: "testing"})
	testCases := []testCase{
		{
			name: "should be able to successfully call the provided handle message func",
			testReader: &TestMessageReader{
				maxMessages: 1,
				message:     testMessageBytes,
				err:         fmt.Errorf("test error"),
			},
			expectedMessage: "test error",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testErrorHandler := &ErrorHandler{}
			consumer := kafkaconsumer.NewConsumer(
				context.Background(),
				tc.testReader,
				-1,
				false,
				nil,
				testErrorHandler.handleError,
				testErrorHandler.handleMessage,
			)
			consumer.Start()
			time.Sleep(1 * time.Millisecond)
			consumer.Close()
			assert.Equal(t, tc.expectedMessage, testErrorHandler.ErrMessage)
		})
	}
}
