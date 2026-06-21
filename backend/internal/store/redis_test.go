package store

import (
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestNeighbourWindow(t *testing.T) {
	tests := []struct {
		name      string
		rank0     int64
		wantStart int64
		wantEnd   int64
	}{
		{
			name:      "middle of leaderboard",
			rank0:     50,
			wantStart: 46,
			wantEnd:   54,
		},
		{
			name:      "rank 0 (top of leaderboard) clamps start to 0",
			rank0:     0,
			wantStart: 0,
			wantEnd:   4,
		},
		{
			name:      "rank 2 clamps start to 0, not negative",
			rank0:     2,
			wantStart: 0,
			wantEnd:   6,
		},
		{
			name:      "rank 4 is the exact clamp boundary",
			rank0:     4,
			wantStart: 0,
			wantEnd:   8,
		},
		{
			name:      "rank 5 is just past the clamp boundary",
			rank0:     5,
			wantStart: 1,
			wantEnd:   9,
		},
		{
			name:      "large rank near bottom of a big leaderboard",
			rank0:     9999,
			wantStart: 9995,
			wantEnd:   10003,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotStart, gotEnd := neighbourWindow(tt.rank0)
			if gotStart != tt.wantStart {
				t.Errorf("start = %d, want %d", gotStart, tt.wantStart)
			}
			if gotEnd != tt.wantEnd {
				t.Errorf("end = %d, want %d", gotEnd, tt.wantEnd)
			}
		})
	}
}

func TestToOneBasedRank(t *testing.T) {
	tests := []struct {
		name  string
		rank0 int64
		want  int
	}{
		{"first place", 0, 1},
		{"second place", 1, 2},
		{"tenth place", 9, 10},
		{"far down the board", 9999, 10000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := toOneBasedRank(tt.rank0)
			if got != tt.want {
				t.Errorf("toOneBasedRank(%d) = %d, want %d", tt.rank0, got, tt.want)
			}
		})
	}
}

func TestBuildNeighbours(t *testing.T) {
	t.Run("excludes the watched user from their own neighbour list", func(t *testing.T) {
		raw := []redis.Z{
			{Member: "user-a", Score: 500},
			{Member: "user-b", Score: 400}, // this is the watched user
			{Member: "user-c", Score: 300},
		}

		got := buildNeighbours(raw, 0, "user-b")

		if len(got) != 2 {
			t.Fatalf("expected 2 neighbours (self excluded), got %d", len(got))
		}
		for _, n := range got {
			if n.UserID == "user-b" {
				t.Errorf("watched user should be excluded from neighbours, found %s", n.UserID)
			}
		}
	})

	t.Run("assigns correct absolute 1-based ranks from a non-zero start offset", func(t *testing.T) {
		// Simulates a user in the middle of the board: ZREVRANGE started
		// at absolute index 46 (0-based), so the first row returned is
		// rank 47, not rank 1.
		raw := []redis.Z{
			{Member: "user-a", Score: 900}, // absolute index 46 -> rank 47
			{Member: "user-b", Score: 850}, // absolute index 47 -> rank 48
			{Member: "user-c", Score: 800}, // absolute index 48 -> rank 49
		}

		got := buildNeighbours(raw, 46, "nonexistent-user")

		want := []struct {
			userID string
			rank   int
		}{
			{"user-a", 47},
			{"user-b", 48},
			{"user-c", 49},
		}

		if len(got) != len(want) {
			t.Fatalf("expected %d neighbours, got %d", len(want), len(got))
		}

		for i, w := range want {
			if got[i].UserID != w.userID {
				t.Errorf("neighbour[%d].UserID = %s, want %s", i, got[i].UserID, w.userID)
			}
			if got[i].Rank != w.rank {
				t.Errorf("neighbour[%d].Rank = %d, want %d", i, got[i].Rank, w.rank)
			}
		}
	})

	t.Run("empty input returns nil, not an empty slice with garbage", func(t *testing.T) {
		got := buildNeighbours([]redis.Z{}, 0, "user-a")
		if len(got) != 0 {
			t.Errorf("expected 0 neighbours for empty input, got %d", len(got))
		}
	})

	t.Run("preserves score values exactly", func(t *testing.T) {
		raw := []redis.Z{
			{Member: "user-a", Score: 123456.0},
		}

		got := buildNeighbours(raw, 0, "someone-else")

		if len(got) != 1 {
			t.Fatalf("expected 1 neighbour, got %d", len(got))
		}
		if got[0].Score != 123456.0 {
			t.Errorf("Score = %v, want 123456.0", got[0].Score)
		}
	})
}
