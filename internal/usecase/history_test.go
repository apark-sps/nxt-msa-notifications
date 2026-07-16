package usecase_test

import (
	"context"
	"errors"
	"nxt-msa-notifications/internal/domain"
	"nxt-msa-notifications/internal/usecase"
	"testing"
)

// ─────────────────────────────────────────────
// Hand-crafted mocks
// ─────────────────────────────────────────────

type mockHistoryRepo struct {
	notifications []domain.Notification
	unreadCount   int
	totalCount    int
	findErr       error
	countUnreadErr error
	countAllErr   error
}

func (m *mockHistoryRepo) Save(_ context.Context, _ *domain.Notification) error { return nil }
func (m *mockHistoryRepo) MarkAsRead(_ context.Context, _, _ string) error      { return nil }
func (m *mockHistoryRepo) MarkAllAsRead(_ context.Context, _ string) error      { return nil }

func (m *mockHistoryRepo) FindByUser(_ context.Context, _ string, _ bool, _, _ int) ([]domain.Notification, error) {
	return m.notifications, m.findErr
}

func (m *mockHistoryRepo) CountUnread(_ context.Context, _ string) (int, error) {
	return m.unreadCount, m.countUnreadErr
}

func (m *mockHistoryRepo) CountAll(_ context.Context, _ string, _ bool) (int, error) {
	return m.totalCount, m.countAllErr
}

// ─────────────────────────────────────────────
// GetHistory Tests
// ─────────────────────────────────────────────

func TestGetHistory_ReturnsNotifications(t *testing.T) {
	expected := []domain.Notification{
		{ID: "notif-001", UserID: "USERS0001", Title: "Hello"},
	}
	repo := &mockHistoryRepo{notifications: expected}
	uc := usecase.NewHistoryUseCase(repo)

	result, err := uc.GetHistory(context.Background(), "USERS0001", false, 10, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 notification, got %d", len(result))
	}
	if result[0].ID != "notif-001" {
		t.Errorf("ID: got %q, want %q", result[0].ID, "notif-001")
	}
}

func TestGetHistory_DefaultLimitApplied(t *testing.T) {
	repo := &mockHistoryRepo{notifications: []domain.Notification{}}
	uc := usecase.NewHistoryUseCase(repo)

	result, err := uc.GetHistory(context.Background(), "USERS0001", false, 0, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Error("expected empty slice, got nil")
	}
}

func TestGetHistory_UpperBoundLimitApplied(t *testing.T) {
	repo := &mockHistoryRepo{notifications: []domain.Notification{}}
	uc := usecase.NewHistoryUseCase(repo)

	result, err := uc.GetHistory(context.Background(), "USERS0001", false, 200, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Error("expected empty slice, got nil")
	}
}

func TestGetHistory_RepositoryError_PropagatesError(t *testing.T) {
	repo := &mockHistoryRepo{findErr: errors.New("db error")}
	uc := usecase.NewHistoryUseCase(repo)

	_, err := uc.GetHistory(context.Background(), "USERS0001", false, 10, 0)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// ─────────────────────────────────────────────
// GetUnreadCount Tests
// ─────────────────────────────────────────────

func TestGetUnreadCount_ReturnsCount(t *testing.T) {
	repo := &mockHistoryRepo{unreadCount: 5}
	uc := usecase.NewHistoryUseCase(repo)

	count, err := uc.GetUnreadCount(context.Background(), "USERS0001")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 5 {
		t.Errorf("count: got %d, want %d", count, 5)
	}
}

func TestGetUnreadCount_ZeroWhenNoUnread(t *testing.T) {
	repo := &mockHistoryRepo{unreadCount: 0}
	uc := usecase.NewHistoryUseCase(repo)

	count, err := uc.GetUnreadCount(context.Background(), "USERS0001")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 0 {
		t.Errorf("count: got %d, want %d", count, 0)
	}
}

func TestGetUnreadCount_RepositoryError_PropagatesError(t *testing.T) {
	repo := &mockHistoryRepo{countUnreadErr: errors.New("db error")}
	uc := usecase.NewHistoryUseCase(repo)

	_, err := uc.GetUnreadCount(context.Background(), "USERS0001")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// ─────────────────────────────────────────────
// GetTotalCount Tests
// ─────────────────────────────────────────────

func TestGetTotalCount_ReturnsCount(t *testing.T) {
	repo := &mockHistoryRepo{totalCount: 42}
	uc := usecase.NewHistoryUseCase(repo)

	count, err := uc.GetTotalCount(context.Background(), "USERS0001", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 42 {
		t.Errorf("count: got %d, want %d", count, 42)
	}
}

func TestGetTotalCount_ZeroWhenNoNotifications(t *testing.T) {
	repo := &mockHistoryRepo{totalCount: 0}
	uc := usecase.NewHistoryUseCase(repo)

	count, err := uc.GetTotalCount(context.Background(), "USERS0001", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 0 {
		t.Errorf("count: got %d, want %d", count, 0)
	}
}

func TestGetTotalCount_RepositoryError_PropagatesError(t *testing.T) {
	repo := &mockHistoryRepo{countAllErr: errors.New("db error")}
	uc := usecase.NewHistoryUseCase(repo)

	_, err := uc.GetTotalCount(context.Background(), "USERS0001", false)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}
