package usecase

import (
	"context"
	"fmt"
	"log"

	"nxt-msa-notifications/internal/domain"
	"nxt-msa-notifications/internal/port/outbound"
)

// DispatchUseCase orchestrates notification persistence and real-time delivery.
// It is the only component aware of both the repository and the notifier adapters.
type DispatchUseCase struct {
	repo      outbound.NotificationRepository
	notifiers map[domain.Channel]outbound.Notifier
}

func NewDispatchUseCase(repo outbound.NotificationRepository, notifiers []outbound.Notifier) *DispatchUseCase {
	notifMap := make(map[domain.Channel]outbound.Notifier, len(notifiers))
	for _, n := range notifiers {
		notifMap[n.Channel()] = n
	}
	return &DispatchUseCase{repo: repo, notifiers: notifMap}
}

// HandleDBWrite persists every recipient notification to PostgreSQL.
// Called exclusively by the Quorum Queue consumer — exactly one pod executes this per event,
// preventing duplicate writes by design (competing consumers).
func (uc *DispatchUseCase) HandleDBWrite(ctx context.Context, event domain.NotificationEvent) error {
	for _, userID := range event.UserIDs {
		notif := &domain.Notification{
			ID:          domain.GenerateID(event.EventID, userID),
			UserID:      userID,
			HierarchyID: event.HierarchyID,
			Type:        event.Type,
			Title:       event.Title,
			Body:        event.Body,
			Metadata:    event.Metadata,
			Channels:    event.Channels,
			Status:      domain.StatusPending,
			CreatedAt:   event.OccurredAt,
		}

		if err := uc.repo.Save(ctx, notif); err != nil {
			return fmt.Errorf("HandleDBWrite: failed to persist for user %s: %w", userID, err)
		}
	}
	return nil
}

// HandleRealTimeDispatch fans the event out to all registered outbound channel notifiers.
// Called by the Stream consumer — every pod executes this. Each Notifier.Send() implementation
// performs its own local check (e.g., WebSocket hub checks its session map) and silently
// discards if the user is not connected to this pod.
//
// If event.Channels is empty, the event is dispatched to ALL registered notifiers — this
// prevents silent drops when the field is omitted from the published payload.
func (uc *DispatchUseCase) HandleRealTimeDispatch(ctx context.Context, event domain.NotificationEvent) {
	for _, userID := range event.UserIDs {
		notif := &domain.Notification{
			ID:          domain.GenerateID(event.EventID, userID),
			UserID:      userID,
			HierarchyID: event.HierarchyID,
			Type:        event.Type,
			Title:       event.Title,
			Body:        event.Body,
			Metadata:    event.Metadata,
			Channels:    event.Channels,
			Status:      domain.StatusPending,
			CreatedAt:   event.OccurredAt,
		}

		// When no channels are specified, broadcast to every registered notifier.
		// When channels are specified, deliver only to the matching ones.
		targets := make([]outbound.Notifier, 0, len(uc.notifiers))
		if len(event.Channels) == 0 {
			for _, n := range uc.notifiers {
				targets = append(targets, n)
			}
		} else {
			for _, ch := range event.Channels {
				if n, active := uc.notifiers[ch]; active {
					targets = append(targets, n)
				}
			}
		}

		for _, n := range targets {
			go func(notifier outbound.Notifier, nt *domain.Notification) {
				if err := notifier.Send(ctx, nt); err != nil {
					log.Printf("[DispatchUseCase] channel=%s user=%s error=%v", notifier.Channel(), nt.UserID, err)
				}
			}(n, notif)
		}
	}
}

