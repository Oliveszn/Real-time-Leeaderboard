package api

import (
	"context"

	"leaderboard/internal/store"
)

// mockLeaderboardService is a test double for LeaderboardService
// Each method's behaviour is controlled by a function field, so individual tests can configure exactly the response they need without touching Redis, Postgres, or Kafka
type mockLeaderboardService struct {
	updateScoreFunc func(ctx context.Context, userID string, points int) (float64, error)
	getTopTenFunc   func(ctx context.Context) ([]store.RankedEntry, error)
	getUserRankFunc func(ctx context.Context, userID string) (*store.UserRankResult, error)
}

func (m *mockLeaderboardService) UpdateScore(ctx context.Context, userID string, points int) (float64, error) {
	if m.updateScoreFunc != nil {
		return m.updateScoreFunc(ctx, userID, points)
	}
	return 0, nil
}

func (m *mockLeaderboardService) GetTopTen(ctx context.Context) ([]store.RankedEntry, error) {
	if m.getTopTenFunc != nil {
		return m.getTopTenFunc(ctx)
	}
	return nil, nil
}

func (m *mockLeaderboardService) GetUserRank(ctx context.Context, userID string) (*store.UserRankResult, error) {
	if m.getUserRankFunc != nil {
		return m.getUserRankFunc(ctx, userID)
	}
	return nil, nil
}
