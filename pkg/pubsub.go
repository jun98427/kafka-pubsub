package pkg

import (
	"github.com/Shopify/sarama"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
)

func KafkaSubscriber(group ...string) message.Subscriber {
	config := kafka.DefaultSaramaSubscriberConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	g := ""
	if len(group) > 0 {
		g = group[0]
	}

	subscriber, err := kafka.NewSubscriber(
		kafka.SubscriberConfig{
			Brokers:               []string{"localhost:9092"},
			Unmarshaler:           kafka.DefaultMarshaler{},
			OverwriteSaramaConfig: config,
			ConsumerGroup:         g,
		},
		watermill.NewStdLogger(false, false),
	)
	if err != nil {
		panic(err)
	}

	return subscriber
}

func KafkaPublisher() message.Publisher {
	config := kafka.DefaultSaramaSyncPublisherConfig()

	publisher, err := kafka.NewPublisher(
		kafka.PublisherConfig{
			Brokers:               []string{"localhost:9092"},
			Marshaler:             kafka.DefaultMarshaler{},
			OTELEnabled:           false,
			OverwriteSaramaConfig: config,
		},
		watermill.NewStdLogger(false, false),
	)
	if err != nil {
		panic(err)
	}

	return publisher
}
