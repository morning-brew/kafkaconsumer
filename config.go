package kafkaconsumer

import (
	"crypto/tls"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

type ReaderConfig struct {
	BrokerURL        string        `env:"KAFKA_READER_BROKER_URL"`
	ConsumerTopic    string        `env:"KAFKA_READER_CONSUMER_TOPIC"`
	GroupID          string        `env:"KAFKA_READER_GROUP_ID"`
	SessionTimeout   time.Duration `env:"KAFKA_READER_SESSION_TIMEOUT"`
	ReadLagIntervals time.Duration `env:"KAFKA_READ_LAG_INTERVALS"`
	Logger           kafka.Logger
	ErrorLogger      kafka.Logger
}

type DialerConfig struct {
	Username       string        `env:"KAFKA_DIALER_USERNAME"`
	Password       string        `env:"KAFKA_DIALER_PASSWORD"`
	ClientID       string        `env:"KAFKA_DIALER_CLIENT_ID"`
	DialerTimeout  time.Duration `env:"KAFKA_DIALER_TIMEOUT"`
	DialerDeadline time.Duration `env:"KAFKA_DIALER_DEADLINE"`
	DualStack      bool          `env:"DUAL_STACK"`
	TLSConfig      *tls.Config
}

type Config struct {
	Reader ReaderConfig
	Dialer DialerConfig
}

func (c *Config) Build() *kafka.Reader {
	readConfig := kafka.ReaderConfig{
		Brokers:         []string{c.Reader.BrokerURL},
		Topic:           c.Reader.ConsumerTopic,
		GroupID:         c.Reader.GroupID,
		SessionTimeout:  c.Reader.SessionTimeout,
		ReadLagInterval: c.Reader.ReadLagIntervals,
		Dialer: &kafka.Dialer{
			ClientID:  c.Dialer.ClientID,
			DualStack: c.Dialer.DualStack,
			Timeout:   c.Dialer.DialerTimeout,
			SASLMechanism: plain.Mechanism{
				Username: c.Dialer.Username,
				Password: c.Dialer.Password,
			},
		},
	}
	return kafka.NewReader(readConfig)
}

func NewConfig() *Config {
	return &Config{}
}

func (c *Config) WithLogger(logger kafka.Logger) {
	c.Reader.Logger = logger
}

func (c *Config) WithErrorLogger(logger kafka.Logger) {
	c.Reader.ErrorLogger = logger
}

func (c *Config) DisableLagInterval() {
	c.Reader.ReadLagIntervals = -1
}

func (c *Config) WithSessionTimeout(timeout time.Duration) {
	c.Reader.SessionTimeout = timeout
}

func (c *Config) WithTLSConfig(config *tls.Config) {
	c.Dialer.TLSConfig = config
}
