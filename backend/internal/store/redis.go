package store

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const LeaderboardKey = "leaderboard"

// NewRedisClient creates a Redis client and verifies the connection with a PING
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

// GetTopN returns the top n players from the leaderboard sorted by score descending
func GetTopN(ctx context.Context, rdb *redis.Client, n int) ([]RankedEntry, error) {
	results, err := rdb.ZRevRangeWithScores(ctx, LeaderboardKey, 0, int64(n-1)).Result()
	if err != nil {
		return nil, fmt.Errorf("zrevrange failed: %w", err)
	}

	entries := make([]RankedEntry, len(results))
	for i, z := range results {
		entries[i] = RankedEntry{
			UserID: z.Member.(string),
			Score:  z.Score,
			Rank:   i + 1, // 1-based rank
		}
	}
	return entries, nil
}

// GetUserRank returns the rank, score, and 4 nearest neighbours

// Two Redis commands
// ZREVRANK  to get the user's position and ZREVRANGE to get neighbour
func GetUserRank(ctx context.Context, rdb *redis.Client, userID string) (*UserRankResult, error) {
	// Pipeline both commands to Redis in a single round-trip
	pipe := rdb.Pipeline()
	rankCmd := pipe.ZRevRank(ctx, LeaderboardKey, userID)
	scoreCmd := pipe.ZScore(ctx, LeaderboardKey, userID)

	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("rank pipeline failed: %w", err)
	}

	rank0, err := rankCmd.Result() // 0-based rank from Redis
	if err == redis.Nil {
		return nil, fmt.Errorf("user %s not found in leaderboard", userID)
	}
	if err != nil {
		return nil, fmt.Errorf("zrevrank failed: %w", err)
	}

	score, err := scoreCmd.Result()
	if err != nil {
		return nil, fmt.Errorf("zscore failed: %w", err)
	}

	start, end := neighbourWindow(rank0)

	neighbours, err := rdb.ZRevRangeWithScores(ctx, LeaderboardKey, start, end).Result()
	if err != nil {
		return nil, fmt.Errorf("zrevrange neighbours failed: %w", err)
	}

	result := &UserRankResult{
		UserID: userID,
		Score:  score,
		Rank:   toOneBasedRank(rank0),
	}

	result.Neighbours = buildNeighbours(neighbours, start, userID)

	return result, nil
}

// neighbourWindow computes the Redis ZREVRANGE [start, end] bounds for the 4-above/4-below neighbourhood around a given 0-based rank. Clamps start at 0 so users near the top of the leaderboard don't request a negative index
func neighbourWindow(rank0 int64) (start, end int64) {
	start = rank0 - 4
	if start < 0 {
		start = 0
	}
	end = rank0 + 4
	return start, end
}

// toOneBasedRank converts Redis's 0-based ZREVRANK result to the 1-based rank shown to clients (rank 0 in Redis == rank 1 on the leaderboard)
func toOneBasedRank(rank0 int64) int {
	return int(rank0) + 1
}

// buildNeighbours converts a raw ZREVRANGE result (starting at absolute index `start`) into RankedEntry rows with correct absolute 1-based ranks, excluding the watched user themselves from the list
func buildNeighbours(raw []redis.Z, start int64, excludeUserID string) []RankedEntry {
	var neighbours []RankedEntry
	for i, z := range raw {
		memberID := z.Member.(string)
		absoluteRank := int(start) + i + 1 // 1-based absolute rank
		if memberID == excludeUserID {
			continue
		}
		neighbours = append(neighbours, RankedEntry{
			UserID: memberID,
			Score:  z.Score,
			Rank:   absoluteRank,
		})
	}
	return neighbours
}

// RankedEntry is a single leaderboard row returned to the client.
type RankedEntry struct {
	UserID string  `json:"user_id"`
	Score  float64 `json:"score"`
	Rank   int     `json:"rank"`
}

// UserRankResult is the response shape for GET /v1/scores/:userId
type UserRankResult struct {
	UserID     string        `json:"user_id"`
	Score      float64       `json:"score"`
	Rank       int           `json:"rank"`
	Neighbours []RankedEntry `json:"neighbours"`
}
