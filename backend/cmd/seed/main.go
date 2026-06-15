package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"

	"leaderboard/internal/config"
	"leaderboard/internal/store"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("no .env file, using environment variables")
	}

	cfg := config.Load()

	rdb, err := store.NewRedisClient(cfg.RedisURL)
	if err != nil {
		log.Fatalf("redis connection failed: %v", err)
	}
	defer rdb.Close()

	ctx := context.Background()

	// Wipe existing leaderboard so seeds are predictable
	rdb.Del(ctx, store.LeaderboardKey)

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	const (
		numUsers  = 10000
		batchSize = 500 // ZADD in batches to avoid one huge command
	)

	log.Printf("seeding %d users into Redis...", numUsers)
	seeded := 0

	for seeded < numUsers {
		end := seeded + batchSize
		if end > numUsers {
			end = numUsers
		}

		members := make([]redis.Z, end-seeded)
		for i := range members {
			members[i] = redis.Z{
				// Scores spread across a realistic range: 0–500k
				Score:  float64(rng.Intn(500000)),
				Member: fmt.Sprintf("sim-user-%05d", seeded+i+1),
			}
		}

		if err := rdb.ZAdd(ctx, store.LeaderboardKey, members...).Err(); err != nil {
			log.Fatalf("zadd batch failed: %v", err)
		}

		seeded = end
		log.Printf("  seeded %d / %d", seeded, numUsers)
	}

	log.Printf("seed complete — %d users in leaderboard", numUsers)

	// Print the top 10 for a sanity check
	top10, err := rdb.ZRevRangeWithScores(ctx, store.LeaderboardKey, 0, 9).Result()
	if err != nil {
		log.Fatalf("failed to read top 10: %v", err)
	}

	fmt.Println("\n--- Top 10 after seed ---")
	for i, z := range top10 {
		fmt.Printf("Rank %2d: %-18s score=%.0f\n", i+1, z.Member, z.Score)
	}
}
