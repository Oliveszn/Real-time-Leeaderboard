package ws

import (
	"encoding/json"
	"testing"
	"time"
)

// newTestClient creates a Client wired to a hub but without a real websocket.Conn
// for testing Hub's register/broadcast/target logic,
func newTestClient(hub *Hub, userID string) *Client {
	return &Client{
		hub:    hub,
		conn:   nil,
		send:   make(chan []byte, 16),
		userID: userID,
	}
}

func TestHub_BroadcastReachesAllClients(t *testing.T) {
	hub := NewHub()
	go hub.Run()

	c1 := newTestClient(hub, "user-1")
	c2 := newTestClient(hub, "user-2")

	hub.register <- c1
	hub.register <- c2
	time.Sleep(20 * time.Millisecond) // let the Run loop process registration

	hub.BroadcastLeaderboard([]string{"top10-payload"})

	for _, c := range []*Client{c1, c2} {
		select {
		case msg := <-c.send:
			var parsed PushMessage
			if err := json.Unmarshal(msg, &parsed); err != nil {
				t.Fatalf("invalid JSON: %v", err)
			}
			if parsed.Type != "leaderboard_update" {
				t.Errorf("type = %s, want leaderboard_update", parsed.Type)
			}
		case <-time.After(time.Second):
			t.Fatal("client did not receive broadcast message")
		}
	}
}

func TestHub_TargetedMessageOnlyReachesIntendedUser(t *testing.T) {
	hub := NewHub()
	go hub.Run()

	watched := newTestClient(hub, "watched-user")
	other := newTestClient(hub, "other-user")

	hub.register <- watched
	hub.register <- other
	time.Sleep(20 * time.Millisecond)

	hub.SendToUser("watched-user", map[string]int{"rank": 5})

	select {
	case msg := <-watched.send:
		var parsed PushMessage
		json.Unmarshal(msg, &parsed)
		if parsed.Type != "rank_update" {
			t.Errorf("type = %s, want rank_update", parsed.Type)
		}
	case <-time.After(time.Second):
		t.Fatal("watched user did not receive targeted message")
	}

	select {
	case msg := <-other.send:
		t.Fatalf("other user should not receive targeted message, got: %s", msg)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestHub_UnregisterRemovesClientFromBroadcast(t *testing.T) {
	hub := NewHub()
	go hub.Run()

	c1 := newTestClient(hub, "user-1")
	hub.register <- c1
	time.Sleep(20 * time.Millisecond)

	hub.unregister <- c1
	time.Sleep(20 * time.Millisecond)

	if hub.ConnectedCount() != 0 {
		t.Errorf("connected count = %d, want 0 after unregister", hub.ConnectedCount())
	}
}

func TestHub_ConnectedCountTracksActiveClients(t *testing.T) {
	hub := NewHub()
	go hub.Run()

	if hub.ConnectedCount() != 0 {
		t.Fatalf("expected 0 connected clients initially, got %d", hub.ConnectedCount())
	}

	c1 := newTestClient(hub, "user-1")
	c2 := newTestClient(hub, "user-2")
	hub.register <- c1
	hub.register <- c2
	time.Sleep(20 * time.Millisecond)

	if hub.ConnectedCount() != 2 {
		t.Errorf("connected count = %d, want 2", hub.ConnectedCount())
	}
}
