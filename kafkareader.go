package kafkaconsumer

import (
	"context"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

type KafkaDefaultconfig struct {
	BrokerURLs     []string
	ReaderTopic    string
	ReaderGroupID  string
	DialerClientID string
	DialerUsername string
	DialerPassword string
}

func (c *KafkaDefaultconfig) NewDefaultKafkaReader() *KafkaReader {
	readConfig := kafka.ReaderConfig{
		Brokers:         c.BrokerURLs,
		Topic:           c.ReaderTopic,
		GroupID:         c.ReaderGroupID,
		ReadLagInterval: -1,
		Dialer: &kafka.Dialer{
			ClientID:  c.DialerClientID,
			DualStack: true,
			SASLMechanism: plain.Mechanism{
				Username: c.DialerUsername,
				Password: c.DialerPassword,
			},
		},
	}
	return &KafkaReader{
		reader: kafka.NewReader(readConfig),
	}
}

func NewKafkaReader(reader *kafka.Reader) *KafkaReader {
	return &KafkaReader{
		reader: reader,
	}
}

type KafkaReader struct {
	reader *kafka.Reader
}

func (kr *KafkaReader) ReadMessage(ctx context.Context) ([]byte, error) {
	m, err := kr.reader.ReadMessage(ctx)
	return m.Value, err
}
