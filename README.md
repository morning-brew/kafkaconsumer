# kafkaconsumer
kafkaconsumer is a message consumption library that handles concurrent message polling with custom error handling, message parsing, and message handling provided. 

## Installation
```
go get github.com/morning-brew/kafkaconsumer
```
Note that this will only work with go versions 1.18 and later.

## Usage
Note that even though the name of the repo starts with kafka, this library can utilize message consumption from almost any service that satisfies the following interface 
```
type MessageReader interface {
	ReadMessage(context.Context) ([]byte, error)
}
```
This allows you to be able to wrap your message service in a function and pass it in to handle all of the polling, concurrency and safety. 

## Quick Start
Start with building your `MessageReader` which can be done locally or by using the `KafkaReader` functions as seen below

```
readerConfig := &kafkaconsumer.KafkaDefaultconfig {
	BrokerURLs:     []string{list your urls...},
	ReaderTopic:    "your topic",
	ReaderGroupID:  "your group id",
	DialerClientID: "client id",
	DialerUsername: "username",
	DialerPassword: "password",
}

reader := readerConfig.NewDefaultKafkareader()
```

Or alternatively, create your own MessageReader or pass your own kafka reader to the `NewKafkaReader` function

Next create and start your consumer
```
    consumer := kafkaconsumer.NewConsumer[<Your Message Type>](
				context.Background(),
				reader,
				-1,
				false,
				handleParse,
				handleError,
				handleMessage,
			)
	consumer.Start()
```

The consumer takes the following arguments
| paramter | Description | default |
| -------- | ----------- | ------- |
| ctx | context to be passed to the reader | no default, required to be passed |
| MessageReader | your message reader | no default, required to be passed |
| buffer size | the size of your channel buffer | passing -1 will set it to the library default of 100 |
| strict mode | setting false will drop messages when channel is full     | default is false | 
| parse func | function to handle parsing messages `func([]byte) <your type>` | leaving nil will do a default parser that will just marshal the incomming message to your message |
| error func | function to handle any errors that occur in parsing messages or reading messages `func(error)` | leaving nil will default to a no-op function |
| handleMessage | function to pass handling incomming messages to the library which will then asynchronously poll from the message channel and handle incomming messages with the passed function `func(<your message type>) error` | default will ignore starting a message polling func and will leave the message handling to you.
