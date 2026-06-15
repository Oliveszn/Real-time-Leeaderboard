package models

import "time"

// ScoreUpdateRequest is the payload for POST /v1/scores
type ScoreUpdateRequest struct {
	UserID string `json:"user_id"`
	Points int    `json:"points"`
}

// ScoreUpdateResponse is returned after a successful score update
type ScoreUpdateResponse struct {
	UserID   string  `json:"user_id"`
	NewScore float64 `json:"new_score"`
}

// ScoreUpdateEvent is published to Kafka after a score change
type ScoreUpdateEvent struct {
	UserID    string    `json:"user_id"`
	Points    int       `json:"points"`
	NewScore  float64   `json:"new_score"`
	Timestamp time.Time `json:"timestamp"`
}
