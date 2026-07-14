package websocket

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 4096
)

// Client represents a single active WebSocket connection belonging to one user.
// A user may have multiple active Clients (multi-device/multi-tab support).
type Client struct {
	UserID    string
	SessionID string // JWT jti claim — for logging and future session revocation
	hub       *Hub
	conn      *websocket.Conn
	send      chan []byte
	done      chan struct{}
	closeOnce sync.Once
}

func NewClient(userID, sessionID string, hub *Hub, conn *websocket.Conn) *Client {
	return &Client{
		UserID:    userID,
		SessionID: sessionID,
		hub:       hub,
		conn:      conn,
		send:      make(chan []byte, 256),
		done:      make(chan struct{}),
	}
}

// Close closes the client connection and signals the write pump to stop.
// It is safe to call multiple times concurrently.
func (c *Client) Close() {
	c.closeOnce.Do(func() {
		close(c.done)
		c.conn.Close()
	})
}

// ReadPump reads incoming frames from the client connection.
// Its primary function is to keep the connection alive by handling pong responses
// and detecting disconnections. Clients do not send notification payloads to the server
// via this channel; they use the HTTP REST endpoints for actions (mark read, etc.).
func (c *Client) ReadPump() {
	defer func() {
		c.hub.CloseClient(c)
	}()

	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		_, _, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("[Client] ReadPump error user=%s session=%s: %v", c.UserID, c.SessionID, err)
			}
			break
		}
	}
}

// WritePump drains the send channel and writes frames to the WebSocket connection.
// A periodic ping is sent to maintain the connection health.
func (c *Client) WritePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.Close()
	}()

	for {
		select {
		case <-c.done:
			return

		case message, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, message); err != nil {
				log.Printf("[Client] WritePump error user=%s: %v", c.UserID, err)
				return
			}

		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// SendJSON marshals a value and enqueues it on the send channel.
// Returns false if the channel buffer is full or client is closed.
func (c *Client) SendJSON(v any) bool {
	data, err := json.Marshal(v)
	if err != nil {
		return false
	}
	select {
	case <-c.done:
		return false
	case c.send <- data:
		return true
	default:
		return false
	}
}
