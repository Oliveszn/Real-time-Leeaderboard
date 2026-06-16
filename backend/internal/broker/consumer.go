package broker

import (
	"context"
	"encoding/json"
	"log"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"

	"leaderboard/internal/models"
	"leaderboard/internal/store"
	"leaderboard/internal/ws"
)

// NewConsumer creates a Kafka reader for the score updates topic
func NewConsumer(broker string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{broker},
		Topic:    ScoreUpdatesTopic,
		GroupID:  "leaderboard-push-service",
		MinBytes: 1,
		MaxBytes: 10e6, // 10 MB
	})
}

// ConsumeScoreUpdates reads score_update events from Kafka and pushes them to connected WebSocket clients via the hub
//
// Fan-out logic:
// If the updated user is now in the top 10 → broadcast the full top-10
// to every connected client (leaderboard_update)
// Otherwise → send a targeted rank_update only to that user's connection
func ConsumeScoreUpdates(ctx context.Context, reader *kafka.Reader, hub *ws.Hub, rdb *redis.Client) {
	log.Println("kafka consumer: listening for score updates")

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Println("kafka consumer: context cancelled, shutting down")
				return
			}
			log.Printf("kafka consumer: read error: %v", err)
			continue
		}

		var event models.ScoreUpdateEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("kafka consumer: failed to unmarshal event: %v", err)
			continue
		}

		go handleScoreEvent(ctx, event, hub, rdb)
	}
}

func handleScoreEvent(ctx context.Context, event models.ScoreUpdateEvent, hub *ws.Hub, rdb *redis.Client) {
	// Get the user's current rank after the score update
	result, err := store.GetUserRank(ctx, rdb, event.UserID)
	if err != nil {
		log.Printf("kafka consumer: failed to get rank for user %s: %v", event.UserID, err)
		return
	}

	if result.Rank <= 10 {
		// Score change affected the top 10 broadcast updated leaderboard to all clients
		top10, err := store.GetTopN(ctx, rdb, 10)
		if err != nil {
			log.Printf("kafka consumer: failed to fetch top 10: %v", err)
			return
		}
		hub.BroadcastLeaderboard(top10)
		log.Printf("kafka consumer: broadcasted leaderboard update (user %s now rank %d)", event.UserID, result.Rank)
	} else {
		// Score change only affects a non-top-10 user targeted push to that user only
		hub.SendToUser(event.UserID, result)
		log.Printf("kafka consumer: sent rank update to user %s (rank %d)", event.UserID, result.Rank)
	}
}
