package websocket

import (
	"log"
	"net/http"

	gorillaws "github.com/gorilla/websocket"

	"nxt-msa-notifications/internal/adapter/middleware"
)

var upgrader = gorillaws.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	// CheckOrigin validates the request origin.
	// In production this should whitelist the Next360 frontend domain.
	// The API Gateway's sticky-session routing ensures the same pod handles
	// subsequent frames from the same connection.
	CheckOrigin: func(r *http.Request) bool {
		// TODO: restrict to known origins in production
		// origin := r.Header.Get("Origin")
		// return origin == "https://app.next360.com"
		return true
	},
}

// ServeWS handles the WebSocket upgrade handshake.
// It decodes the JWT from the ?token= query parameter (passed by the frontend
// before the connection is upgraded, since WebSocket API cannot set custom headers),
// registers the client in the Hub, and starts the read/write goroutines.
//
// The catch-up push (offline notifications) is triggered inside hub.RegisterClient().
func ServeWS(hub *Hub, w http.ResponseWriter, r *http.Request) {
	// Extract token from query param — standard pattern for WebSocket auth
	// since browsers cannot set Authorization headers during WS handshake
	tokenStr := r.URL.Query().Get("token")
	if tokenStr == "" {
		http.Error(w, "missing token", http.StatusUnauthorized)
		return
	}

	claims, err := middleware.DecodeJWT(tokenStr)
	if err != nil {
		log.Printf("[WS] Auth failed: %v", err)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[WS] Upgrade failed user=%s: %v", claims.UserID, err)
		return
	}

	client := NewClient(claims.UserID, claims.JTI, hub, conn)

	// RegisterClient also triggers the offline catch-up push asynchronously
	hub.RegisterClient(client)

	// Each client requires exactly two goroutines for its lifecycle
	go client.WritePump()
	go client.ReadPump()
}
