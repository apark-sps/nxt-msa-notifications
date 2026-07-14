package outbound

import (
	"context"

	"nxt-msa-notifications/internal/domain"
)

// Notifier is the port implemented by each outbound delivery channel adapter.
// New channels (SMS, Email, Push) are added by implementing this interface —
// no use-case code changes required.
type Notifier interface {
	Channel() domain.Channel
	Send(ctx context.Context, notification *domain.Notification) error
}
