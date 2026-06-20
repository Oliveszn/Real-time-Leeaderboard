package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func NewPostgresPool(dsn string) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse postgres dsn failed: %w", err)
	}

	//tuned based on neon free plan
	poolCfg.MaxConns = 20
	poolCfg.MinConns = 2
	poolCfg.MaxConnLifetime = 30 * time.Minute
	poolCfg.MaxConnIdleTime = 5 * time.Minute
	poolCfg.HealthCheckPeriod = 1 * time.Minute

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("postgres pool creation failed: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres ping failed: %w", err)
	}

	return pool, nil
}

// RecordScoreEvent writes an immutable audit log and upserts (create or update)
// the user's cumulative score. If the user doesn't exist yet, a row is created
// for them so the foreign keys resolve cleanly.
func RecordScoreEvent(ctx context.Context, pool *pgxpool.Pool, userID string, points int) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx failed: %w", err)
	}
	defer tx.Rollback(ctx) // no-op if committed

	_, err = tx.Exec(ctx, `
		INSERT INTO users (user_id, user_name)
		VALUES ($1, $2)
		ON CONFLICT (user_id) DO NOTHING
	`, userID, userID)
	if err != nil {
		return fmt.Errorf("ensure user failed: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO score_events (user_id, points)
		VALUES ($1, $2)
	`, userID, points)
	if err != nil {
		return fmt.Errorf("insert score_event failed: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO user_scores (user_id, score, updated_at)
		VALUES ($1, $2, NOW())
		ON CONFLICT (user_id) DO UPDATE
		SET score = user_scores.score + EXCLUDED.score,
		    updated_at = NOW()
	`, userID, points)
	if err != nil {
		return fmt.Errorf("upsert user_scores failed: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx failed: %w", err)
	}

	return nil
}

// ScoreWriteInput is a single pending score write passed into the batch writer.
type ScoreWriteInput struct {
	UserID string
	Points int
}

func RecordScoreEventsBatch(ctx context.Context, pool *pgxpool.Pool, writes []ScoreWriteInput) error {
	if len(writes) == 0 {
		return nil
	}

	userIDs := make([]string, len(writes))
	points := make([]int, len(writes))
	for i, w := range writes {
		userIDs[i] = w.UserID
		points[i] = w.Points
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin batch tx failed: %w", err)
	}
	defer tx.Rollback(ctx) // no-op if committed

	_, err = tx.Exec(ctx, `
		INSERT INTO users (user_id, user_name)
		SELECT DISTINCT u, u
		FROM UNNEST($1::text[]) AS u
		ON CONFLICT (user_id) DO NOTHING
	`, userIDs)
	if err != nil {
		return fmt.Errorf("batch ensure users failed: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO score_events (user_id, points)
		SELECT * FROM UNNEST($1::text[], $2::int[])
	`, userIDs, points)
	if err != nil {
		return fmt.Errorf("batch insert score_events failed: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO user_scores (user_id, score, updated_at)
		SELECT user_id, SUM(points), NOW()
		FROM UNNEST($1::text[], $2::int[]) AS t(user_id, points)
		GROUP BY user_id
		ON CONFLICT (user_id) DO UPDATE
		SET score = user_scores.score + EXCLUDED.score,
		    updated_at = NOW()
	`, userIDs, points)
	if err != nil {
		return fmt.Errorf("batch upsert user_scores failed: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit batch tx failed: %w", err)
	}

	return nil
}
