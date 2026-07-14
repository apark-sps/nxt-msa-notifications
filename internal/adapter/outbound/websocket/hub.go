package websocket

import (
	"context"
	"encoding/json"
	"log"
	"sync"

	"nxt-msa-notifications/internal/domain"
	"nxt-msa-notifications/internal/port/outbound"
)

// Hub manages all active WebSocket connections on this pod.
// It implements the outbound.Notifier port so the DispatchUseCase
// can call Send() without knowing anything about WebSocket internals.
//
// The connection map is nested: map[userID]map[*Client]struct{} to support
// multiple simultaneous connections per user (multi-device / multi-tab).
type Hub struct {
	mu          sync.RWMutex
	connections map[string]map[*Client]struct{}
	repo        outbound.NotificationRepository // Used for catch-up on WS connect
}

func NewHub(repo outbound.NotificationRepository) *Hub {
	return &Hub{
		connections: make(map[string]map[*Client]struct{}),
		repo:        repo,
	}
}

// Channel implements outbound.Notifier — identifies this adapter's channel type.
func (h *Hub) Channel() domain.Channel {
	return domain.ChannelWebSocket
}

// Send implements outbound.Notifier.
// Looks up the user in the local session map. If the user is not connected to
// this pod, it returns nil cleanly — the stream consumer on every other pod will
// also attempt Send() and one of them will find the correct connection.
func (h *Hub) Send(ctx context.Context, notification *domain.Notification) error {
	h.mu.RLock()
	clients, exists := h.connections[notification.UserID]
	h.mu.RUnlock()

	if !exists || len(clients) == 0 {
		return nil // User not connected to this pod — silent discard
	}

	payload, err := json.Marshal(map[string]any{
		"type":         "notification",
		"notification": notification,
	})
	if err != nil {
		return err
	}

	for client := range clients {
		select {
		case <-client.done:
			// Skip closed clients
		case client.send <- payload:
		default:
			// Client's send buffer is full — treat as a dead connection
			h.CloseClient(client)
		}
	}
	return nil
}

// RegisterClient adds the client to the session map and immediately triggers
// a catch-up push of all pending notifications the user missed while offline.
func (h *Hub) RegisterClient(c *Client) {
	h.mu.Lock()
	if _, exists := h.connections[c.UserID]; !exists {
		h.connections[c.UserID] = make(map[*Client]struct{})
	}
	h.connections[c.UserID][c] = struct{}{}
	h.mu.Unlock()

	log.Printf("[Hub] Client registered user=%s session=%s total_connections=%d",
		c.UserID, c.SessionID, h.connectionCount())

	// Push unread notifications from DB asynchronously — non-blocking
	go h.sendCatchUp(c)
}

// CloseClient removes the client from the session map and closes its connection.
func (h *Hub) CloseClient(c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()

	clients, exists := h.connections[c.UserID]
	if !exists {
		return
	}

	if _, ok := clients[c]; ok {
		delete(clients, c)
		c.Close()
		log.Printf("[Hub] Client closed user=%s session=%s", c.UserID, c.SessionID)
	}

	if len(clients) == 0 {
		delete(h.connections, c.UserID)
	}
}

// sendCatchUp queries the DB for unread notifications and sends them as the
// first message on the WebSocket connection — the hybrid approach catch-up flow.
// This eliminates the need for a separate HTTP GET call to populate the notification bell.
func (h *Hub) sendCatchUp(c *Client) {
	unread, err := h.repo.FindByUser(context.Background(), c.UserID, true, 50, 0)
	if err != nil {
		log.Printf("[Hub] Catch-up query failed user=%s: %v", c.UserID, err)
		return
	}

	if len(unread) == 0 {
		return
	}

	payload, err := json.Marshal(map[string]any{
		"type":          "catch_up",
		"notifications": unread,
		"unread_count":  len(unread),
	})
	if err != nil {
		return
	}

	select {
	case <-c.done:
		// Client closed
	case c.send <- payload:
		log.Printf("[Hub] Catch-up pushed user=%s count=%d", c.UserID, len(unread))
	default:
		h.CloseClient(c)
	}
}

func (h *Hub) connectionCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	count := 0
	for _, clients := range h.connections {
		count += len(clients)
	}
	return count
}
