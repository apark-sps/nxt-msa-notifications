package usecase

import (
	"context"
	"fmt"

	"nxt-msa-notifications/internal/domain"
	"nxt-msa-notifications/internal/port/outbound"
)

// HistoryUseCase handles notification history queries.
type HistoryUseCase struct {
	repo outbound.NotificationRepository
}

func NewHistoryUseCase(repo outbound.NotificationRepository) *HistoryUseCase {
	return &HistoryUseCase{repo: repo}
}

// GetHistory returns a paginated list of notifications for a user.
func (uc *HistoryUseCase) GetHistory(ctx context.Context, userID string, unreadOnly bool, limit, offset int) ([]domain.Notification, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}

	notifications, err := uc.repo.FindByUser(ctx, userID, unreadOnly, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("HistoryUseCase.GetHistory: %w", err)
	}
	return notifications, nil
}

// GetUnreadCount returns the total count of unread notifications for a user.
// Used by the HTTP GET /count route and the WebSocket catch-up message.
func (uc *HistoryUseCase) GetUnreadCount(ctx context.Context, userID string) (int, error) {
	count, err := uc.repo.CountUnread(ctx, userID)
	if err != nil {
		return 0, fmt.Errorf("HistoryUseCase.GetUnreadCount: %w", err)
	}
	return count, nil
}

// GetTotalCount returns the total count of notifications for a user.
// When unreadOnly is true, only unread notifications are counted.
func (uc *HistoryUseCase) GetTotalCount(ctx context.Context, userID string, unreadOnly bool) (int, error) {
	count, err := uc.repo.CountAll(ctx, userID, unreadOnly)
	if err != nil {
		return 0, fmt.Errorf("HistoryUseCase.GetTotalCount: %w", err)
	}
	return count, nil
}
