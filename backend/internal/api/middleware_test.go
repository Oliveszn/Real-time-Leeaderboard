package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAPIKeyMiddleware(t *testing.T) {
	const validKey = "secret-key-123"

	passthrough := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	handler := APIKeyMiddleware(validKey)(passthrough)

	tests := []struct {
		name       string
		headerKey  string
		wantStatus int
	}{
		{
			name:       "valid key passes through",
			headerKey:  validKey,
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing key is rejected",
			headerKey:  "",
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "wrong key is rejected",
			headerKey:  "wrong-key",
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "key with different casing is rejected (exact match required)",
			headerKey:  "SECRET-KEY-123",
			wantStatus: http.StatusUnauthorized,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/v1/scores", nil)
			if tt.headerKey != "" {
				req.Header.Set("X-API-Key", tt.headerKey)
			}
			rec := httptest.NewRecorder()

			handler.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Errorf("status = %d, want %d", rec.Code, tt.wantStatus)
			}
		})
	}
}

func TestAPIKeyMiddleware_DoesNotCallNextHandlerWhenUnauthorized(t *testing.T) {
	nextCalled := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		nextCalled = true
	})

	handler := APIKeyMiddleware("correct-key")(next)

	req := httptest.NewRequest(http.MethodPost, "/v1/scores", nil)
	req.Header.Set("X-API-Key", "wrong-key")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if nextCalled {
		t.Error("next handler should not be called when API key is invalid")
	}
}
