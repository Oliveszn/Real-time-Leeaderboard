package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// NewPostgresPool creates a pgx connection pool to Neon and verifies it with a Ping.
func NewPostgresPool(dsn string) (*pgxpool.Pool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres pool creation failed: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres ping failed: %w", err)
	}

	return pool, nil
}

// RecordScoreEvent writes an immutable audit log and upserts(create or update) users score
// if user no exist then create a row for them
func RecordScoreEvent(ctx context.Context, pool *pgxpool.Pool, userID string, points int) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx failed: %w", err)
	}
	defer tx.Rollback(ctx) // no-op if committed

	// Ensure the user exists, user_name defaults to user_id
	_, err = tx.Exec(ctx, `
		INSERT INTO users (user_id, user_name)
		VALUES ($1, $1)
		ON CONFLICT (user_id) DO NOTHING
	`, userID)
	if err != nil {
		return fmt.Errorf("ensure user failed: %w", err)
	}

	// Log the event
	_, err = tx.Exec(ctx, `
		INSERT INTO score_events (user_id, points)
		VALUES ($1, $2)
	`, userID, points)
	if err != nil {
		return fmt.Errorf("insert score_event failed: %w", err)
	}

	// Upsert cumulative score
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
