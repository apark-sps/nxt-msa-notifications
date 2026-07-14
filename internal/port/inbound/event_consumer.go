package inbound

import (
	"context"

	"nxt-msa-notifications/internal/domain"
)

// EventConsumer is the inbound port contract for AMQP message consumers.
// Both the quorum queue consumer and the stream consumer implement this interface.
type EventConsumer interface {
	Start(ctx context.Context) error
}

// DBWriteHandler is the function signature for the quorum queue consumer callback.
type DBWriteHandler func(ctx context.Context, event domain.NotificationEvent) error

// RealtimeHandler is the function signature for the stream consumer callback.
type RealtimeHandler func(ctx context.Context, event domain.NotificationEvent)
