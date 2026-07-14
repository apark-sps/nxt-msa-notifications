package usecase

import (
	"context"
	"fmt"

	"nxt-msa-notifications/internal/port/outbound"
)

// AcknowledgeUseCase handles marking notifications as read.
type AcknowledgeUseCase struct {
	repo outbound.NotificationRepository
}

func NewAcknowledgeUseCase(repo outbound.NotificationRepository) *AcknowledgeUseCase {
	return &AcknowledgeUseCase{repo: repo}
}

// MarkAsRead marks a single notification as read for a given user.
// The userID guard ensures a user cannot mark another user's notification as read.
func (uc *AcknowledgeUseCase) MarkAsRead(ctx context.Context, notificationID, userID string) error {
	if err := uc.repo.MarkAsRead(ctx, notificationID, userID); err != nil {
		return fmt.Errorf("AcknowledgeUseCase.MarkAsRead: %w", err)
	}
	return nil
}

// MarkAllAsRead marks all unread notifications as read for a user.
func (uc *AcknowledgeUseCase) MarkAllAsRead(ctx context.Context, userID string) error {
	if err := uc.repo.MarkAllAsRead(ctx, userID); err != nil {
		return fmt.Errorf("AcknowledgeUseCase.MarkAllAsRead: %w", err)
	}
	return nil
}
