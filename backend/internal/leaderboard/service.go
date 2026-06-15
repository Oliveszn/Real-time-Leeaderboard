package leaderboard

import (
	"context"
	"leaderboard/internal/broker"
	"leaderboard/internal/models"
	"leaderboard/internal/store"

	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

type Service struct {
	Redis    *redis.Client
	Postgres *pgxpool.Pool
	Producer *kafka.Writer
}

func NewService(rdb *redis.Client, pg *pgxpool.Pool, producer *kafka.Writer) *Service {
	return &Service{
		Redis:    rdb,
		Postgres: pg,
		Producer: producer,
	}
}

// UpdateScore applies a score update for user
// ZINCRYBY on redis (synchronous) leader board reads must reflect this immediately
// asynchronous write to postgres
// Async publish to kafka, this drives the real-time wbsocket push
// retuns the users new total score from redis
func (s *Service) UpdateScore(ctx context.Context, userID string, points int) (float64, error) {
	newScore, err := store.IncrScore(ctx, s.Redis, userID, points)
	if err != nil {
		return 0, err
	}

	// Postgres write and kafka publish happens in the background so the api response isnt blocked, each with their ctx
	go func() {
		bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := store.RecordScoreEvent(bgCtx, s.Postgres, userID, points); err != nil {
			log.Printf("warning: failed to record score event for user %s: %v", userID, err)
		}
	}()

	go func() {
		bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		event := models.ScoreUpdateEvent{
			UserID:    userID,
			Points:    points,
			NewScore:  newScore,
			Timestamp: time.Now(),
		}

		if err := broker.PublishScoreUpdate(bgCtx, s.Producer, event); err != nil {
			log.Printf("warning: failed to publish score update for user %s: %v", userID, err)
		}
	}()

	return newScore, nil
}
