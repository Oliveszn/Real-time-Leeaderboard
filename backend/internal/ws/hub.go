package ws

import (
	"encoding/json"
	"log"
	"sync"
)

// PushMessage is the envelope sent to clients over WebSocket.
type PushMessage struct {
	Type    string      `json:"type"`    // "leaderboard_update" | "rank_update"
	Payload interface{} `json:"payload"` // top-10 slice or user rank result
}

// Hub maintains the set of active WebSocket clients and broadcasts messages.
// all mutations go through the register/unregister channels which are processed serially in the Run loop
type Hub struct {
	// All connected clients, keyed by pointer
	clients map[*Client]bool

	// Index of userID client for targeted pushes
	userIndex map[string]*Client

	// Inbound channels from ServeWs
	register   chan *Client
	unregister chan *Client

	// Broadcast sends a message to every connected client
	broadcast chan []byte

	// Target sends a message to a specific user only
	target chan targetedMessage

	mu sync.RWMutex
}

type targetedMessage struct {
	userID  string
	payload []byte
}

func NewHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]bool),
		userIndex:  make(map[string]*Client),
		register:   make(chan *Client, 256),
		unregister: make(chan *Client, 256),
		broadcast:  make(chan []byte, 256),
		target:     make(chan targetedMessage, 256),
	}
}

// Run starts the hub's event loop
func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			if client.userID != "" {
				h.userIndex[client.userID] = client
			}
			h.mu.Unlock()
			log.Printf("ws: client connected (user=%s) total=%d", client.userID, len(h.clients))

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				delete(h.userIndex, client.userID)
				close(client.send)
			}
			h.mu.Unlock()
			log.Printf("ws: client disconnected (user=%s) total=%d", client.userID, len(h.clients))

		case message := <-h.broadcast:
			h.mu.RLock()
			for client := range h.clients {
				select {
				case client.send <- message:
				default:
					// Client's buffer is full — drop and disconnect
					close(client.send)
					delete(h.clients, client)
					delete(h.userIndex, client.userID)
				}
			}
			h.mu.RUnlock()

		case msg := <-h.target:
			h.mu.RLock()
			if client, ok := h.userIndex[msg.userID]; ok {
				select {
				case client.send <- msg.payload:
				default:
					close(client.send)
					delete(h.clients, client)
					delete(h.userIndex, client.userID)
				}
			}
			h.mu.RUnlock()
		}
	}
}

// BroadcastLeaderboard sends a leaderboard_update to every connected client
// Called when a score change affects the top 10.
func (h *Hub) BroadcastLeaderboard(payload interface{}) {
	msg := PushMessage{Type: "leaderboard_update", Payload: payload}
	data, err := json.Marshal(msg)
	if err != nil {
		log.Printf("ws: failed to marshal leaderboard broadcast: %v", err)
		return
	}
	h.broadcast <- data
}

// SendToUser sends a rank_update to a specific user's connection only
// Called when a score change affects a user not in the top 10
func (h *Hub) SendToUser(userID string, payload interface{}) {
	msg := PushMessage{Type: "rank_update", Payload: payload}
	data, err := json.Marshal(msg)
	if err != nil {
		log.Printf("ws: failed to marshal rank update for user %s: %v", userID, err)
		return
	}
	h.target <- targetedMessage{userID: userID, payload: data}
}

// ConnectedCount returns the number of currently connected clients
func (h *Hub) ConnectedCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}
