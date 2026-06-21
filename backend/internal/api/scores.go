package api

import (
	"context"
	"encoding/json"
	"net/http"

	"leaderboard/internal/store"
	// "leaderboard/internal/leaderboard"
	"leaderboard/internal/models"

	"github.com/gorilla/mux"
)

type LeaderboardService interface {
	UpdateScore(ctx context.Context, userID string, points int) (float64, error)
	GetTopTen(ctx context.Context) ([]store.RankedEntry, error)
	GetUserRank(ctx context.Context, userID string) (*store.UserRankResult, error)
}

type ScoresHandler struct {
	Service LeaderboardService
}

func NewScoresHandler(svc LeaderboardService) *ScoresHandler {
	return &ScoresHandler{Service: svc}
}

// PostScore handles POST /v1/scores
// Adds `points` to the user's existing score
// (enforced via APIKeyMiddleware)
func (h *ScoresHandler) PostScore(w http.ResponseWriter, r *http.Request) {
	var req models.ScoreUpdateRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request body"}`, http.StatusBadRequest)
		return
	}

	if req.UserID == "" {
		http.Error(w, `{"error":"user_id is required"}`, http.StatusBadRequest)
		return
	}

	newScore, err := h.Service.UpdateScore(r.Context(), req.UserID, req.Points)
	if err != nil {
		http.Error(w, `{"error":"failed to update score"}`, http.StatusInternalServerError)
		return
	}

	resp := models.ScoreUpdateResponse{
		UserID:   req.UserID,
		NewScore: newScore,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

// GetTopScores handles GET /v1/scores
// Returns the top 10 players from the leaderboard.
func (h *ScoresHandler) GetTopScores(w http.ResponseWriter, r *http.Request) {
	entries, err := h.Service.GetTopTen(r.Context())
	if err != nil {
		http.Error(w, `{"error":"failed to fetch leaderboard"}`, http.StatusInternalServerError)
		return
	}

	resp := map[string]interface{}{
		"data":  entries,
		"total": len(entries),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// GetUserScore handles GET /v1/scores/:userId
// Returns the user's rank, score, and 4 nearest neighbours above and below them.
func (h *ScoresHandler) GetUserScore(w http.ResponseWriter, r *http.Request) {
	userID := mux.Vars(r)["userId"]
	if userID == "" {
		http.Error(w, `{"error":"userId is required"}`, http.StatusBadRequest)
		return
	}

	result, err := h.Service.GetUserRank(r.Context(), userID)
	if err != nil {
		http.Error(w, `{"error":"user not found or leaderboard unavailable"}`, http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}
