package store

import (
	"context"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// RebuildLeaderboard checks if the Redis leaderboard is empty and if so,repopulates it from user_scores in Postgres
// called once on startup to recover from data loss or redid restart
func RebuildLeaderboard(ctx context.Context, pool *pgxpool.Pool, rdb *redis.Client) error {
	// Check if the leaderboard already has data
	count, err := rdb.ZCard(ctx, LeaderboardKey).Result()
	if err != nil {
		return fmt.Errorf("zcard failed: %w", err)
	}

	if count > 0 {
		log.Printf("leaderboard rebuild: Redis already has %d entries, skipping", count)
		return nil
	}

	log.Println("leaderboard rebuild: Redis is empty, rebuilding from Postgres...")

	rows, err := pool.Query(ctx, `
		SELECT user_id::text, score
		FROM user_scores
		ORDER BY score DESC
	`)
	if err != nil {
		return fmt.Errorf("query user_scores failed: %w", err)
	}
	defer rows.Close()

	const batchSize = 500
	batch := make([]redis.Z, 0, batchSize)
	total := 0

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := rdb.ZAdd(ctx, LeaderboardKey, batch...).Err(); err != nil {
			return fmt.Errorf("zadd batch failed: %w", err)
		}
		total += len(batch)
		batch = batch[:0]
		return nil
	}

	for rows.Next() {
		var userID string
		var score float64

		if err := rows.Scan(&userID, &score); err != nil {
			return fmt.Errorf("scan row failed: %w", err)
		}

		batch = append(batch, redis.Z{
			Score:  score,
			Member: userID,
		})

		if len(batch) >= batchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}

	// Flush any remaining rows
	if err := flush(); err != nil {
		return err
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("row iteration error: %w", err)
	}

	log.Printf("leaderboard rebuild: restored %d users from Postgres into Redis", total)
	return nil
}
