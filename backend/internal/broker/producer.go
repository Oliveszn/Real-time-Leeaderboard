package broker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/segmentio/kafka-go"
)

const ScoreUpdatesTopic = "leaderboard.score_updates"

// create a kafka writer for the score update topic
func NewProducer(broker string) *kafka.Writer {
	return &kafka.Writer{
		Addr:                   kafka.TCP(broker),
		Topic:                  ScoreUpdatesTopic,
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
	}
}

// PublishScoreUpdate sends a score update event to kafka, the event payload is json encoded
func PublishScoreUpdate(ctx context.Context, w *kafka.Writer, event interface{}) error {
	payload, err := json.Marshal(event)

	if err != nil {
		return fmt.Errorf("marshal score update event failed: %w", err)
	}

	err = w.WriteMessages(ctx, kafka.Message{
		Value: payload,
	})
	if err != nil {
		return fmt.Errorf("kafka write failed: %w", err)
	}
	return nil
}
