package store

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const LeaderboardKey = "leaderboard"

// NewRedisClient creates a Redis client and verifies the connection with a PING.
func NewRedisClient(redisURL string) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr: redisURL,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping failed: %w", err)
	}

	return client, nil
}

// IncrScore adds the point to a users's given score in the sorted set, if user not exist, redis sets score to 0 then adds it
func IncrScore(ctx context.Context, rdb *redis.Client, userID string, points int) (float64, error) {
	newScore, err := rdb.ZIncrBy(ctx, LeaderboardKey, float64(points), userID).Result()
	if err != nil {
		return 0, fmt.Errorf("zincrby failed: %w", err)
	}
	return newScore, nil
}
