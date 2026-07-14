package domain

import "time"

// NotificationEvent is the incoming contract published by Java microservices via RabbitMQ.
// It is consumed from both the Quorum Queue (for DB persistence) and the
// Stream (for real-time WebSocket fan-out).
type NotificationEvent struct {
	EventID     string            `json:"event_id"`     // Idempotency key — used to prevent duplicate DB writes
	Source      string            `json:"source"`       // Originating microservice (e.g. "nxt-msa-users")
	Type        string            `json:"type"`         // Event trigger type (e.g. "user.created")
	UserIDs     []string          `json:"user_ids"`     // Target recipient user IDs
	HierarchyID *int              `json:"hierarchy_id"` // Organizational node scope
	Title       string            `json:"title"`
	Body        string            `json:"body"`
	Metadata    map[string]string `json:"metadata"`
	Channels    []Channel         `json:"channels"` // Requested delivery channels
	OccurredAt  time.Time         `json:"occurred_at"`
}
