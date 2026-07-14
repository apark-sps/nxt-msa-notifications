package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"nxt-msa-notifications/internal/domain"
	"nxt-msa-notifications/internal/port/inbound"
)

// QuorumConsumer reads from the durable quorum queue (notifications.persist).
// All pods compete for messages — exactly one pod processes each event and writes to the DB.
// Uses automatic reconnection with exponential backoff on connection loss.
type QuorumConsumer struct {
	uri         string
	queue       string
	exchange    string
	routingKey  string
	streamQueue string
	podName     string
	handler     inbound.DBWriteHandler
}

func NewQuorumConsumer(
	uri, queue, exchange, routingKey, streamQueue, podName string,
	handler inbound.DBWriteHandler,
) *QuorumConsumer {
	return &QuorumConsumer{
		uri:         uri,
		queue:       queue,
		exchange:    exchange,
		routingKey:  routingKey,
		streamQueue: streamQueue,
		podName:     podName,
		handler:     handler,
	}
}

// Start begins consuming from the quorum queue and blocks until ctx is cancelled.
// On connection or channel loss, it reconnects with exponential backoff.
func (c *QuorumConsumer) Start(ctx context.Context) error {
	backoff := time.Second

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		if err := c.consume(ctx); err != nil {
			log.Printf("[QuorumConsumer] Connection error: %v — retrying in %s", err, backoff)
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
		backoff = time.Second // Reset backoff on clean exit
	}
}

func (c *QuorumConsumer) consume(ctx context.Context) error {
	conn, err := amqp.Dial(c.uri)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("channel: %w", err)
	}
	defer ch.Close()

	// Declare the topic exchange (idempotent — matches NxtMsaNotificationConfig.java)
	if err := ch.ExchangeDeclare(c.exchange, "topic", true, false, false, false, nil); err != nil {
		return fmt.Errorf("exchange declare: %w", err)
	}

	// Declare the quorum queue (idempotent)
	if _, err := ch.QueueDeclare(c.queue, true, false, false, false,
		amqp.Table{"x-queue-type": "quorum"},
	); err != nil {
		return fmt.Errorf("queue declare: %w", err)
	}

	// Bind queue to exchange
	if err := ch.QueueBind(c.queue, c.routingKey, c.exchange, false, nil); err != nil {
		return fmt.Errorf("queue bind: %w", err)
	}

	// Declare and bind the stream queue (idempotent — routes events to RabbitMQ Stream)
	if c.streamQueue != "" {
		_, err = ch.QueueDeclare(
			c.streamQueue,
			true,  // durable
			false, // auto-delete
			false, // exclusive
			false, // no-wait
			amqp.Table{
				"x-queue-type":                    "stream",
				"x-max-age":                       "86400s",
				"x-stream-max-segment-size-bytes": int64(500000000),
				"x-queue-leader-locator":          "least-leaders",
			},
		)
		if err != nil {
			return fmt.Errorf("stream queue declare: %w", err)
		}

		err = ch.QueueBind(c.streamQueue, "#", c.exchange, false, nil)
		if err != nil {
			return fmt.Errorf("stream queue bind: %w", err)
		}
	}

	// QoS: process one message at a time — ensures clean competing consumer semantics
	if err := ch.Qos(1, 0, false); err != nil {
		return fmt.Errorf("qos: %w", err)
	}

	// Consumer tag is "podName:queue" — unique per pod, visible in the RabbitMQ UI
	consumerTag := c.podName + ":" + c.queue
	msgs, err := ch.Consume(c.queue, consumerTag, false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	log.Printf("[QuorumConsumer] Started consuming from %s (tag=%s)", c.queue, consumerTag)

	connClose := conn.NotifyClose(make(chan *amqp.Error, 1))

	for {
		select {
		case <-ctx.Done():
			return nil

		case err := <-connClose:
			return fmt.Errorf("connection closed: %v", err)

		case msg, ok := <-msgs:
			if !ok {
				return fmt.Errorf("message channel closed")
			}

			var event domain.NotificationEvent
			if err := json.Unmarshal(msg.Body, &event); err != nil {
				log.Printf("[QuorumConsumer] Failed to unmarshal event: %v — nacking", err)
				msg.Nack(false, false) // Dead-letter malformed messages
				continue
			}

			if err := c.handler(ctx, event); err != nil {
				log.Printf("[QuorumConsumer] Handler error event_id=%s: %v — nacking for requeue", event.EventID, err)
				msg.Nack(false, true) // Requeue on handler errors (transient DB failures)
				continue
			}

			msg.Ack(false)
		}
	}
}
