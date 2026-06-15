package api

import (
	"encoding/json"
	"net/http"

	"leaderboard/internal/leaderboard"
	"leaderboard/internal/models"
)

type ScoresHandler struct {
	Service *leaderboard.Service
}

func NewScoresHandler(svc *leaderboard.Service) *ScoresHandler {
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
