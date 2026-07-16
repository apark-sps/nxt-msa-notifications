package usecase_test

import (
	"context"
	"errors"
	"nxt-msa-notifications/internal/domain"
	"nxt-msa-notifications/internal/port/outbound"
	"nxt-msa-notifications/internal/usecase"
	"sync"
	"testing"
	"time"
)

// ─────────────────────────────────────────────
// Hand-crafted mocks — no external dependencies
// ─────────────────────────────────────────────

// mockRepo is a thread-safe in-memory mock for outbound.NotificationRepository.
type mockRepo struct {
	mu    sync.Mutex
	saved []*domain.Notification
	err   error // if set, Save returns this error
}

func (m *mockRepo) Save(_ context.Context, n *domain.Notification) error {
	if m.err != nil {
		return m.err
	}
	m.mu.Lock()
	m.saved = append(m.saved, n)
	m.mu.Unlock()
	return nil
}
func (m *mockRepo) MarkAsRead(_ context.Context, _, _ string) error { return nil }
func (m *mockRepo) MarkAllAsRead(_ context.Context, _ string) error { return nil }
func (m *mockRepo) FindByUser(_ context.Context, _ string, _ bool, _, _ int) ([]domain.Notification, error) {
	return nil, nil
}
func (m *mockRepo) CountUnread(_ context.Context, _ string) (int, error) { return 0, nil }
func (m *mockRepo) CountAll(_ context.Context, _ string, _ bool) (int, error) { return 0, nil }

// mockNotifier is a thread-safe in-memory mock for outbound.Notifier.
type mockNotifier struct {
	mu       sync.Mutex
	channel  domain.Channel
	received []*domain.Notification
	err      error // if set, Send returns this error
}

func (m *mockNotifier) Channel() domain.Channel { return m.channel }
func (m *mockNotifier) Send(_ context.Context, n *domain.Notification) error {
	if m.err != nil {
		return m.err
	}
	m.mu.Lock()
	m.received = append(m.received, n)
	m.mu.Unlock()
	return nil
}

// ─────────────────────────────────────────────
// Test helpers
// ─────────────────────────────────────────────

func newTestEvent(userIDs []string, channels []domain.Channel) domain.NotificationEvent {
	return domain.NotificationEvent{
		EventID:    "evt-test-001",
		Source:     "nxt-msa-test",
		Type:       "test.event",
		UserIDs:    userIDs,
		Title:      "Test Title",
		Body:       "Test Body",
		Metadata:   map[string]string{"key": "value"},
		Channels:   channels,
		OccurredAt: time.Now(),
	}
}

// ─────────────────────────────────────────────
// HandleDBWrite Tests
// ─────────────────────────────────────────────

func TestHandleDBWrite_SingleUser_PersistsOneRecord(t *testing.T) {
	repo := &mockRepo{}
	uc := usecase.NewDispatchUseCase(repo, nil)

	event := newTestEvent([]string{"USERS0001"}, []domain.Channel{domain.ChannelWebSocket})

	if err := uc.HandleDBWrite(context.Background(), event); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(repo.saved) != 1 {
		t.Fatalf("expected 1 saved notification, got %d", len(repo.saved))
	}
}

func TestHandleDBWrite_MultipleUsers_PersistsOneRecordPerUser(t *testing.T) {
	repo := &mockRepo{}
	uc := usecase.NewDispatchUseCase(repo, nil)

	event := newTestEvent([]string{"USERS0001", "USERS0002", "USERS0003"}, []domain.Channel{domain.ChannelWebSocket})

	if err := uc.HandleDBWrite(context.Background(), event); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(repo.saved) != 3 {
		t.Fatalf("expected 3 saved notifications, got %d", len(repo.saved))
	}
}

func TestHandleDBWrite_GeneratesDeterministicID(t *testing.T) {
	repo := &mockRepo{}
	uc := usecase.NewDispatchUseCase(repo, nil)

	event := newTestEvent([]string{"USERS0001"}, []domain.Channel{domain.ChannelWebSocket})

	_ = uc.HandleDBWrite(context.Background(), event)

	saved := repo.saved[0]
	expected := domain.GenerateID(event.EventID, "USERS0001")

	if saved.ID != expected {
		t.Errorf("ID mismatch: got %q, want %q", saved.ID, expected)
	}
}

func TestHandleDBWrite_SetsStatusPending(t *testing.T) {
	repo := &mockRepo{}
	uc := usecase.NewDispatchUseCase(repo, nil)

	event := newTestEvent([]string{"USERS0001"}, []domain.Channel{domain.ChannelWebSocket})
	_ = uc.HandleDBWrite(context.Background(), event)

	if repo.saved[0].Status != domain.StatusPending {
		t.Errorf("Status: got %q, want %q", repo.saved[0].Status, domain.StatusPending)
	}
}

func TestHandleDBWrite_RepositoryError_PropagatesError(t *testing.T) {
	repo := &mockRepo{err: errors.New("db down")}
	uc := usecase.NewDispatchUseCase(repo, nil)

	event := newTestEvent([]string{"USERS0001"}, []domain.Channel{domain.ChannelWebSocket})

	if err := uc.HandleDBWrite(context.Background(), event); err == nil {
		t.Error("expected error from failing repository, got nil")
	}
}

// ─────────────────────────────────────────────
// HandleRealTimeDispatch Tests
// ─────────────────────────────────────────────

func TestHandleRealTimeDispatch_DeliversToRegisteredChannel(t *testing.T) {
	repo := &mockRepo{}
	notifier := &mockNotifier{channel: domain.ChannelWebSocket}
	uc := usecase.NewDispatchUseCase(repo, []outbound.Notifier{notifier})

	event := newTestEvent([]string{"USERS0001"}, []domain.Channel{domain.ChannelWebSocket})
	uc.HandleRealTimeDispatch(context.Background(), event)

	// Allow goroutines to complete
	time.Sleep(50 * time.Millisecond)

	notifier.mu.Lock()
	count := len(notifier.received)
	notifier.mu.Unlock()

	if count != 1 {
		t.Errorf("expected 1 delivery, got %d", count)
	}
}

func TestHandleRealTimeDispatch_MultipleUsers_DeliversToAll(t *testing.T) {
	repo := &mockRepo{}
	notifier := &mockNotifier{channel: domain.ChannelWebSocket}
	uc := usecase.NewDispatchUseCase(repo, []outbound.Notifier{notifier})

	event := newTestEvent([]string{"USERS0001", "USERS0002"}, []domain.Channel{domain.ChannelWebSocket})
	uc.HandleRealTimeDispatch(context.Background(), event)

	time.Sleep(50 * time.Millisecond)

	notifier.mu.Lock()
	count := len(notifier.received)
	notifier.mu.Unlock()

	if count != 2 {
		t.Errorf("expected 2 deliveries, got %d", count)
	}
}

func TestHandleRealTimeDispatch_UnknownChannel_IsDiscardedSilently(t *testing.T) {
	repo := &mockRepo{}
	notifier := &mockNotifier{channel: domain.ChannelWebSocket}
	uc := usecase.NewDispatchUseCase(repo, []outbound.Notifier{notifier})

	// Event requests SMS but only WebSocket is registered
	event := newTestEvent([]string{"USERS0001"}, []domain.Channel{"sms"})
	uc.HandleRealTimeDispatch(context.Background(), event)

	time.Sleep(50 * time.Millisecond)

	notifier.mu.Lock()
	count := len(notifier.received)
	notifier.mu.Unlock()

	if count != 0 {
		t.Errorf("expected 0 deliveries for unregistered channel, got %d", count)
	}
}

func TestHandleRealTimeDispatch_SetsCorrectIDAndStatus(t *testing.T) {
	repo := &mockRepo{}
	notifier := &mockNotifier{channel: domain.ChannelWebSocket}
	uc := usecase.NewDispatchUseCase(repo, []outbound.Notifier{notifier})

	event := newTestEvent([]string{"USERS0001"}, []domain.Channel{domain.ChannelWebSocket})
	uc.HandleRealTimeDispatch(context.Background(), event)

	time.Sleep(50 * time.Millisecond)

	notifier.mu.Lock()
	received := notifier.received[0]
	notifier.mu.Unlock()

	expectedID := domain.GenerateID(event.EventID, "USERS0001")
	if received.ID != expectedID {
		t.Errorf("ID mismatch: got %q, want %q", received.ID, expectedID)
	}
	if received.Status != domain.StatusPending {
		t.Errorf("Status: got %q, want %q", received.Status, domain.StatusPending)
	}
}

func TestHandleRealTimeDispatch_IDMatchesDBWriteID(t *testing.T) {
	repo := &mockRepo{}
	notifier := &mockNotifier{channel: domain.ChannelWebSocket}
	uc := usecase.NewDispatchUseCase(repo, []outbound.Notifier{notifier})

	event := newTestEvent([]string{"USERS0001"}, []domain.Channel{domain.ChannelWebSocket})

	_ = uc.HandleDBWrite(context.Background(), event)
	uc.HandleRealTimeDispatch(context.Background(), event)

	time.Sleep(50 * time.Millisecond)

	notifier.mu.Lock()
	rtID := notifier.received[0].ID
	notifier.mu.Unlock()

	dbID := repo.saved[0].ID

	if rtID != dbID {
		t.Errorf("real-time ID %q must match DB-persisted ID %q", rtID, dbID)
	}
}
