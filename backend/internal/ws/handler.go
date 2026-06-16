package ws

import (
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,

	// all origin allowed, to-do tighten in prod
	CheckOrigin: func(r *http.Request) bool { return true },
}

// ServeWs upgrades the HTTP connection to WebSocket and registers the client
// with the hub. The optional `userId` query param links the connection to a
// specific user so targeted rank pushes can reach them
func ServeWs(hub *Hub, w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("ws upgrade failed: %v", err)
		return
	}

	userID := r.URL.Query().Get("userId")

	// The http.Server's WriteTimeout in main would kill long-lived WebSocket
	// connections. Clearing the deadline after upgrade hands timeout
	// responsibility to the per-client ping/pong logic in writePump
	conn.NetConn().SetDeadline(time.Time{})

	client := &Client{
		hub:    hub,
		conn:   conn,
		send:   make(chan []byte, 256),
		userID: userID,
	}

	hub.register <- client

	// Run read and write pumps in separate goroutines
	go client.writePump()
	go client.readPump()
}
