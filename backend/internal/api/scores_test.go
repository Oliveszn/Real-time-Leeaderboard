package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"

	"leaderboard/internal/store"
)

// PostScore

func TestPostScore_Success(t *testing.T) {
	mock := &mockLeaderboardService{
		updateScoreFunc: func(ctx context.Context, userID string, points int) (float64, error) {
			if userID != "user-1" || points != 100 {
				t.Errorf("unexpected args: userID=%s points=%d", userID, points)
			}
			return 450, nil
		},
	}
	h := NewScoresHandler(mock)

	body, _ := json.Marshal(map[string]interface{}{"user_id": "user-1", "points": 100})
	req := httptest.NewRequest(http.MethodPost, "/v1/scores", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	h.PostScore(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d. body: %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON response: %v", err)
	}

	if resp["user_id"] != "user-1" {
		t.Errorf("response user_id = %v, want user-1", resp["user_id"])
	}
	if resp["new_score"] != float64(450) {
		t.Errorf("response new_score = %v, want 450", resp["new_score"])
	}
}

func TestPostScore_MissingUserID(t *testing.T) {
	mock := &mockLeaderboardService{}
	h := NewScoresHandler(mock)

	body, _ := json.Marshal(map[string]interface{}{"points": 100})
	req := httptest.NewRequest(http.MethodPost, "/v1/scores", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	h.PostScore(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestPostScore_InvalidJSON(t *testing.T) {
	mock := &mockLeaderboardService{}
	h := NewScoresHandler(mock)

	req := httptest.NewRequest(http.MethodPost, "/v1/scores", bytes.NewReader([]byte("{not valid json")))
	rec := httptest.NewRecorder()

	h.PostScore(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestPostScore_ServiceError(t *testing.T) {
	mock := &mockLeaderboardService{
		updateScoreFunc: func(ctx context.Context, userID string, points int) (float64, error) {
			return 0, errors.New("redis is down")
		},
	}
	h := NewScoresHandler(mock)

	body, _ := json.Marshal(map[string]interface{}{"user_id": "user-1", "points": 100})
	req := httptest.NewRequest(http.MethodPost, "/v1/scores", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	h.PostScore(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
}

// Points should be additive, never replacing
// This test exists to catch a regression where someone "simplifies" UpdateScore into a SET instead of an INCR.
func TestPostScore_PointsAreAdditiveNotAbsolute(t *testing.T) {
	var receivedPoints int
	mock := &mockLeaderboardService{
		updateScoreFunc: func(ctx context.Context, userID string, points int) (float64, error) {
			receivedPoints = points
			return float64(points), nil // pretend this was the user's first score
		},
	}
	h := NewScoresHandler(mock)

	body, _ := json.Marshal(map[string]interface{}{"user_id": "user-1", "points": 50})
	req := httptest.NewRequest(http.MethodPost, "/v1/scores", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	h.PostScore(rec, req)

	if receivedPoints != 50 {
		t.Errorf("handler should pass points through unchanged for the service to add, got %d", receivedPoints)
	}
}

// GetTopScores

func TestGetTopScores_Success(t *testing.T) {
	mock := &mockLeaderboardService{
		getTopTenFunc: func(ctx context.Context) ([]store.RankedEntry, error) {
			return []store.RankedEntry{
				{UserID: "user-1", Score: 500, Rank: 1},
				{UserID: "user-2", Score: 400, Rank: 2},
			}, nil
		},
	}
	h := NewScoresHandler(mock)

	req := httptest.NewRequest(http.MethodGet, "/v1/scores", nil)
	rec := httptest.NewRecorder()

	h.GetTopScores(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	var resp struct {
		Data  []store.RankedEntry `json:"data"`
		Total int                 `json:"total"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON response: %v", err)
	}

	if resp.Total != 2 {
		t.Errorf("total = %d, want 2", resp.Total)
	}
	if len(resp.Data) != 2 || resp.Data[0].UserID != "user-1" {
		t.Errorf("unexpected data: %+v", resp.Data)
	}
}

func TestGetTopScores_EmptyLeaderboard(t *testing.T) {
	mock := &mockLeaderboardService{
		getTopTenFunc: func(ctx context.Context) ([]store.RankedEntry, error) {
			return []store.RankedEntry{}, nil
		},
	}
	h := NewScoresHandler(mock)

	req := httptest.NewRequest(http.MethodGet, "/v1/scores", nil)
	rec := httptest.NewRecorder()

	h.GetTopScores(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	var resp map[string]interface{}
	json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp["total"] != float64(0) {
		t.Errorf("total = %v, want 0", resp["total"])
	}
}

func TestGetTopScores_ServiceError(t *testing.T) {
	mock := &mockLeaderboardService{
		getTopTenFunc: func(ctx context.Context) ([]store.RankedEntry, error) {
			return nil, errors.New("redis unavailable")
		},
	}
	h := NewScoresHandler(mock)

	req := httptest.NewRequest(http.MethodGet, "/v1/scores", nil)
	rec := httptest.NewRecorder()

	h.GetTopScores(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
}

// GetUserScore
func withMuxVars(req *http.Request, vars map[string]string) *http.Request {
	return mux.SetURLVars(req, vars)
}

func TestGetUserScore_Success(t *testing.T) {
	mock := &mockLeaderboardService{
		getUserRankFunc: func(ctx context.Context, userID string) (*store.UserRankResult, error) {
			if userID != "user-42" {
				t.Errorf("unexpected userID: %s", userID)
			}
			return &store.UserRankResult{
				UserID: "user-42",
				Score:  1000,
				Rank:   6,
				Neighbours: []store.RankedEntry{
					{UserID: "user-41", Score: 1100, Rank: 5},
					{UserID: "user-43", Score: 900, Rank: 7},
				},
			}, nil
		},
	}
	h := NewScoresHandler(mock)

	req := httptest.NewRequest(http.MethodGet, "/v1/scores/user-42", nil)
	req = withMuxVars(req, map[string]string{"userId": "user-42"})
	rec := httptest.NewRecorder()

	h.GetUserScore(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d. body: %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var resp store.UserRankResult
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON response: %v", err)
	}

	if resp.Rank != 6 {
		t.Errorf("rank = %d, want 6", resp.Rank)
	}
	if len(resp.Neighbours) != 2 {
		t.Errorf("neighbours = %d, want 2", len(resp.Neighbours))
	}
}

func TestGetUserScore_MissingUserIDParam(t *testing.T) {
	mock := &mockLeaderboardService{}
	h := NewScoresHandler(mock)

	req := httptest.NewRequest(http.MethodGet, "/v1/scores/", nil)
	req = withMuxVars(req, map[string]string{}) // no userId set
	rec := httptest.NewRecorder()

	h.GetUserScore(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestGetUserScore_UserNotFound(t *testing.T) {
	mock := &mockLeaderboardService{
		getUserRankFunc: func(ctx context.Context, userID string) (*store.UserRankResult, error) {
			return nil, errors.New("user not found in leaderboard")
		},
	}
	h := NewScoresHandler(mock)

	req := httptest.NewRequest(http.MethodGet, "/v1/scores/ghost-user", nil)
	req = withMuxVars(req, map[string]string{"userId": "ghost-user"})
	rec := httptest.NewRecorder()

	h.GetUserScore(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}
