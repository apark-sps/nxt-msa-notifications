package outbound

import (
	"context"
	"nxt-msa-notifications/internal/domain"
)

// NotificationRepository is the port for all persistence operations.
// The PostgreSQL adapter implements this interface.
type NotificationRepository interface {
	// Save persists a new notification. Implementations must be idempotent
	// on event_id to guard against quorum queue redelivery.
	Save(ctx context.Context, n *domain.Notification) error

	// MarkAsRead marks a specific notification as read for a given user.
	MarkAsRead(ctx context.Context, notificationID, userID string) error

	// MarkAllAsRead marks all pending/delivered notifications as read for a user.
	MarkAllAsRead(ctx context.Context, userID string) error

	// FindByUser retrieves paginated notifications for a user.
	// When unreadOnly is true, only non-read notifications are returned.
	FindByUser(ctx context.Context, userID string, unreadOnly bool, limit, offset int) ([]domain.Notification, error)

	// CountUnread returns the count of unread notifications for a user.
	// Used by the HTTP GET /count route and the WebSocket catch-up push.
	CountUnread(ctx context.Context, userID string) (int, error)

	// CountAll returns total notifications for a user.
	// When unreadOnly is true, only unread notifications are counted.
	CountAll(ctx context.Context, userID string, unreadOnly bool) (int, error)
}
