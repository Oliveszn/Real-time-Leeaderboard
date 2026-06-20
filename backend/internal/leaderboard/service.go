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

	writeQueue chan store.ScoreWriteInput
}

func NewService(rdb *redis.Client, pg *pgxpool.Pool, producer *kafka.Writer) *Service {
	s := &Service{
		Redis:    rdb,
		Postgres: pg,
		Producer: producer,

		//buff channel absorbs write bursts w/o blocking the request path
		writeQueue: make(chan store.ScoreWriteInput, 5000),
	}

	go s.runBatchFlusher()

	return s
}

// runBatchFlusher drains the write queue into Postgres in batches rather than opening a transaction per score event
func (s *Service) runBatchFlusher() {
	const (
		batchSize     = 100
		flushInterval = 250 * time.Millisecond
	)

	batch := make([]store.ScoreWriteInput, 0, batchSize)
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		toWrite := make([]store.ScoreWriteInput, len(batch))
		copy(toWrite, batch)
		batch = batch[:0]

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := store.RecordScoreEventsBatch(ctx, s.Postgres, toWrite); err != nil {
			log.Printf("warning: batch postgres write failed (%d events): %v", len(toWrite), err)
		}
	}

	for {
		select {
		case w, ok := <-s.writeQueue:
			if !ok {
				flush()
				return
			}
			batch = append(batch, w)
			if len(batch) >= batchSize {
				flush()
			}

		case <-ticker.C:
			flush()
		}
	}
}

// GetTopTen fetches the top 10 players from the Redis sorted set
func (s *Service) GetTopTen(ctx context.Context) ([]store.RankedEntry, error) {
	entries, err := store.GetTopN(ctx, s.Redis, 10)
	if err != nil {
		return nil, err
	}
	return entries, nil
}

// GetUserRank fetches a user's rank, score, and their 4 nearest neighbours
func (s *Service) GetUserRank(ctx context.Context, userID string) (*store.UserRankResult, error) {
	result, err := store.GetUserRank(ctx, s.Redis, userID)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// UpdateScore applies a score update for user
// ZINCRYBY on redis (synchronous) leader board reads must reflect this immediately asynchronous write to postgres
// Async publish to kafka (drives the real-time wbsocket push)
func (s *Service) UpdateScore(ctx context.Context, userID string, points int) (float64, error) {
	newScore, err := store.IncrScore(ctx, s.Redis, userID, points)
	if err != nil {
		return 0, err
	}

	//non-blocking queue if the queue is full and pg falling being cos of sustained load, we drop the durabillity write
	//rather than blocking api response, redis remain correct
	select {
	case s.writeQueue <- store.ScoreWriteInput{UserID: userID, Points: points}:
	default:
		log.Printf("warning: postgres write queue full, dropping event for user %s", userID)
	}

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
