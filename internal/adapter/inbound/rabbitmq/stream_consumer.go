package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	stream "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"

	"nxt-msa-notifications/internal/domain"
	"nxt-msa-notifications/internal/port/inbound"
)

// StreamConsumer reads from the durable stream (notifications.broadcast).
// Unlike the QuorumConsumer, every pod reads every message independently using
// a unique consumer name (pod hostname/name) for server-side offset tracking.
// This provides O(1) broker-side fan-out: one stream entity, N independent readers.
type StreamConsumer struct {
	uri        string
	streamName string
	podName    string
	maxAgeSecs int
	handler    inbound.RealtimeHandler
}

func NewStreamConsumer(
	uri, streamName, podName string,
	maxAgeSecs int,
	handler inbound.RealtimeHandler,
) *StreamConsumer {
	return &StreamConsumer{
		uri:        uri,
		streamName: streamName,
		podName:    podName,
		maxAgeSecs: maxAgeSecs,
		handler:    handler,
	}
}

// Start connects to the stream and begins consuming from the last committed offset
// for this pod. Reconnects with exponential backoff on connection loss.
func (c *StreamConsumer) Start(ctx context.Context) error {
	backoff := time.Second

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		if err := c.consume(ctx); err != nil {
			slog.Error("Stream consumer connection error, retrying", "error", err, "backoff", backoff)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(backoff):
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = time.Second
	}
}

func (c *StreamConsumer) consume(ctx context.Context) error {
	env, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().SetUri(c.uri),
	)
	if err != nil {
		return fmt.Errorf("stream env: %w", err)
	}
	defer env.Close()

	// Declare stream with retention policy (idempotent)
	streamOpts := stream.NewStreamOptions().
		SetMaxAge(time.Duration(c.maxAgeSecs) * time.Second).
		SetMaxSegmentSizeBytes(stream.ByteCapacity{}.MB(500))

	err = env.DeclareStream(c.streamName, streamOpts)
	if err != nil && !isStreamAlreadyExists(err) {
		return fmt.Errorf("declare stream: %w", err)
	}

	// Resolve the starting offset for this pod.
	// QueryOffset returns the last offset committed by this named consumer.
	// If no offset has been stored yet (first-ever run of this pod name),
	// it returns an error — we fall back to First() to consume from the
	// beginning of the current retention window.
	// Consumer name is "podName:streamName" — unique per pod, self-describing in the RabbitMQ UI,
	// and consistent with the offset key stored server-side for resume-on-restart.
	consumerName := c.podName + ":" + c.streamName
	offsetSpec := resolveStartOffset(env, c.streamName, consumerName)

	consumer, err := env.NewConsumer(c.streamName,
		func(consumerCtx stream.ConsumerContext, message *amqp.Message) {
			var event domain.NotificationEvent
			if err := json.Unmarshal(message.Data[0], &event); err != nil {
				slog.Error("Failed to unmarshal stream event", "error", err)
				return
			}

			// Every pod calls this. The Hub.Send() performs an O(1) local map lookup
			// and silently returns nil if the user isn't connected to this pod.
			c.handler(ctx, event)
		},
		stream.NewConsumerOptions().
			SetConsumerName(consumerName). // Unique per pod — enables server-side offset tracking
			SetOffset(offsetSpec),         // Resume from last committed offset, or First() on new pod
	)
	if err != nil {
		return fmt.Errorf("new consumer: %w", err)
	}
	defer consumer.Close()

	slog.Info("Started consuming from stream", "stream", c.streamName, "consumer", consumerName)

	// Obtain the close notification channel from the consumer to detect connection drop
	consumerClose := consumer.NotifyClose()

	// Block until context is cancelled or consumer is closed externally
	select {
	case <-ctx.Done():
		return nil
	case ev := <-consumerClose:
		return fmt.Errorf("consumer connection closed: stream=%s name=%s reason=%s", ev.StreamName, ev.Name, ev.Reason)
	}
}

// isStreamAlreadyExists checks if the stream declaration error is a benign "already exists" case.
func isStreamAlreadyExists(err error) bool {
	return err != nil && err.Error() == "stream already exists"
}

// resolveStartOffset queries the broker for the last committed offset of this
// named consumer. This is the correct replacement for the deprecated LastConsumed().
//
// Behaviour:
//   - First run (no stored offset): QueryOffset returns an error → fall back to
//     First(), consuming all messages in the current retention window.
//   - Subsequent runs: returns the exact offset where this pod left off,
//     so no messages are missed or replayed after a pod restart.
func resolveStartOffset(env *stream.Environment, streamName, consumerName string) stream.OffsetSpecification {
	offset, err := env.QueryOffset(consumerName, streamName)
	if err != nil {
		// No stored offset for this consumer name — start from the beginning
		// of whatever the stream currently retains (within the 24h window).
		slog.Info("No stored offset for consumer (first run) — starting from First()", "consumer", consumerName)
		return stream.OffsetSpecification{}.First()
	}

	// Advance by 1 past the last committed offset to avoid reprocessing
	// the final message that was already handled before the pod stopped.
	slog.Info("Resuming consumer from offset", "consumer", consumerName, "offset", offset+1)
	return stream.OffsetSpecification{}.Offset(offset + 1)
}
