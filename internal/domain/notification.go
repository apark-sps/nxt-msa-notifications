package domain

import (
	"time"

	"github.com/google/uuid"
)

// NotificationNamespace is the domain-wide UUID namespace used for RFC 4122 v5
// deterministic notification ID generation (EventID + UserID).
var NotificationNamespace = uuid.MustParse("d5696d5e-85e6-4279-8db2-df66b72a6b28")

// GenerateID computes a deterministic UUID v5 for a given event and recipient user.
func GenerateID(eventID, userID string) string {
	return uuid.NewSHA1(NotificationNamespace, []byte(eventID+":"+userID)).String()
}

// DeliveryStatus represents the lifecycle state of a notification.
type DeliveryStatus string

const (
	StatusPending   DeliveryStatus = "pending"
	StatusDelivered DeliveryStatus = "delivered"
	StatusRead      DeliveryStatus = "read"
	StatusFailed    DeliveryStatus = "failed"
)

// Notification is the core domain entity. It represents a single notification
// persisted in the database and delivered to a user.
type Notification struct {
	ID          string            `json:"id"                  db:"id"`
	UserID      string            `json:"user_id"             db:"user_id"`      // Primary route identifier ("USERS0001")
	HierarchyID *int              `json:"hierarchy_id"        db:"hierarchy_id"` // Organizational scope
	Type        string            `json:"type"                db:"type"`         // e.g. "user.created"
	Title       string            `json:"title"               db:"title"`
	Body        string            `json:"body"                db:"body"`
	Metadata    map[string]string `json:"metadata"            db:"-"` // Deserialized from JSONB
	Channels    []Channel         `json:"channels"            db:"-"` // Deserialized from TEXT[]
	Status      DeliveryStatus    `json:"status"              db:"status"`
	CreatedAt   time.Time         `json:"created_at"          db:"created_at"`
	ReadAt      *time.Time        `json:"read_at,omitempty"   db:"read_at"`
}
