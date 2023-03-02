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
			name: "should be able to read all messages put on the channel",
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
