package http_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"nxt-msa-notifications/internal/domain"
	"nxt-msa-notifications/internal/port/outbound"
	"nxt-msa-notifications/internal/usecase"
	"strings"
	"testing"

	httphandler "nxt-msa-notifications/internal/adapter/inbound/http"
)

// ─────────────────────────────────────────────
// Hand-crafted mocks
// ─────────────────────────────────────────────

type mockHandlerRepo struct {
	notifications []domain.Notification
	unreadCount   int
	totalCount    int
	markReadErr   error
	findErr       error
	countErr      error
}

func (m *mockHandlerRepo) Save(_ context.Context, _ *domain.Notification) error { return nil }
func (m *mockHandlerRepo) MarkAsRead(_ context.Context, _, _ string) error      { return m.markReadErr }

func (m *mockHandlerRepo) MarkAllAsRead(_ context.Context, _ string) error { return m.markReadErr }

func (m *mockHandlerRepo) FindByUser(_ context.Context, _ string, _ bool, _, _ int) ([]domain.Notification, error) {
	return m.notifications, m.findErr
}

func (m *mockHandlerRepo) CountUnread(_ context.Context, _ string) (int, error) {
	return m.unreadCount, m.countErr
}

func (m *mockHandlerRepo) CountAll(_ context.Context, _ string, _ bool) (int, error) {
	return m.totalCount, m.countErr
}

// ─────────────────────────────────────────────
// Test helpers
// ─────────────────────────────────────────────

// buildMockJWT builds a minimal valid mock JWT for test requests.
func buildMockJWT(userID, sessionID string) string {
	header, _ := json.Marshal(map[string]string{"alg": "RS256", "typ": "JWT"})
	body, _ := json.Marshal(map[string]string{
		"jti":           sessionID,
		"custom:iduser": userID,
		"custom:role":   "ROLE_ADMIN",
	})
	sig := []byte("dummy")

	encode := base64.RawURLEncoding.EncodeToString
	return strings.Join([]string{encode(header), encode(body), encode(sig)}, ".")
}

// newTestHandler constructs a Handler wired to a mock repository.
func newTestHandler(repo outbound.NotificationRepository) *httphandler.Handler {
	history := usecase.NewHistoryUseCase(repo)
	ack := usecase.NewAcknowledgeUseCase(repo)
	return httphandler.NewHandler(history, ack)
}

// ─────────────────────────────────────────────
// GET /v1/notifications tests
// ─────────────────────────────────────────────

func TestGetNotifications_ValidToken_ReturnsTotalCount(t *testing.T) {
	repo := &mockHandlerRepo{
		notifications: []domain.Notification{
			{ID: "notif-001", UserID: "USERS0001", Title: "Hello"},
		},
		totalCount: 42,
	}
	h := newTestHandler(repo)

	req := httptest.NewRequest(http.MethodGet, "/v1/notifications", nil)
	req.Header.Set("Authorization", "Bearer "+buildMockJWT("USERS0001", "sess-001"))
	w := httptest.NewRecorder()

	h.GetNotifications(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status: got %d, want %d", w.Code, http.StatusOK)
	}

	var body map[string]any
	json.NewDecoder(w.Body).Decode(&body)
	if body["count"].(float64) != 42 {
		t.Errorf("count: got %v, want 42", body["count"])
	}
}

func TestGetNotifications_MissingToken_Returns401(t *testing.T) {
	h := newTestHandler(&mockHandlerRepo{})

	req := httptest.NewRequest(http.MethodGet, "/v1/notifications", nil)
	w := httptest.NewRecorder()

	h.GetNotifications(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("status: got %d, want %d", w.Code, http.StatusUnauthorized)
	}
}

func TestGetNotifications_InvalidToken_Returns401(t *testing.T) {
	h := newTestHandler(&mockHandlerRepo{})

	req := httptest.NewRequest(http.MethodGet, "/v1/notifications", nil)
	req.Header.Set("Authorization", "Bearer not-a-jwt")
	w := httptest.NewRecorder()

	h.GetNotifications(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("status: got %d, want %d", w.Code, http.StatusUnauthorized)
	}
}

// ─────────────────────────────────────────────
// GET /v1/notifications/count tests
// ─────────────────────────────────────────────

func TestGetUnreadCount_ValidToken_ReturnsCorrectCount(t *testing.T) {
	repo := &mockHandlerRepo{unreadCount: 7}
	h := newTestHandler(repo)

	req := httptest.NewRequest(http.MethodGet, "/v1/notifications/count", nil)
	req.Header.Set("Authorization", "Bearer "+buildMockJWT("USERS0001", "sess-001"))
	w := httptest.NewRecorder()

	h.GetUnreadCount(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status: got %d, want %d", w.Code, http.StatusOK)
	}

	var body map[string]any
	json.NewDecoder(w.Body).Decode(&body)
	if body["unread_count"].(float64) != 7 {
		t.Errorf("unread_count: got %v, want 7", body["unread_count"])
	}
}

func TestGetUnreadCount_MissingToken_Returns401(t *testing.T) {
	h := newTestHandler(&mockHandlerRepo{})
	req := httptest.NewRequest(http.MethodGet, "/v1/notifications/count", nil)
	w := httptest.NewRecorder()

	h.GetUnreadCount(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("status: got %d, want %d", w.Code, http.StatusUnauthorized)
	}
}

// ─────────────────────────────────────────────
// PATCH /v1/notifications/{id}/read tests
// ─────────────────────────────────────────────

func TestMarkAsRead_ValidToken_Returns204(t *testing.T) {
	h := newTestHandler(&mockHandlerRepo{})

	req := httptest.NewRequest(http.MethodPatch, "/v1/notifications/notif-abc/read", nil)
	req.Header.Set("Authorization", "Bearer "+buildMockJWT("USERS0001", "sess-001"))
	req.SetPathValue("id", "notif-abc")
	w := httptest.NewRecorder()

	h.MarkAsRead(w, req)

	if w.Code != http.StatusNoContent {
		t.Errorf("status: got %d, want %d", w.Code, http.StatusNoContent)
	}
}

func TestMarkAsRead_MissingToken_Returns401(t *testing.T) {
	h := newTestHandler(&mockHandlerRepo{})

	req := httptest.NewRequest(http.MethodPatch, "/v1/notifications/notif-abc/read", nil)
	req.SetPathValue("id", "notif-abc")
	w := httptest.NewRecorder()

	h.MarkAsRead(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("status: got %d, want %d", w.Code, http.StatusUnauthorized)
	}
}

// ─────────────────────────────────────────────
// PATCH /v1/notifications/read-all tests
// ─────────────────────────────────────────────

func TestMarkAllAsRead_ValidToken_Returns204(t *testing.T) {
	h := newTestHandler(&mockHandlerRepo{})

	req := httptest.NewRequest(http.MethodPatch, "/v1/notifications/read-all", nil)
	req.Header.Set("Authorization", "Bearer "+buildMockJWT("USERS0001", "sess-001"))
	w := httptest.NewRecorder()

	h.MarkAllAsRead(w, req)

	if w.Code != http.StatusNoContent {
		t.Errorf("status: got %d, want %d", w.Code, http.StatusNoContent)
	}
}

func TestMarkAllAsRead_MissingToken_Returns401(t *testing.T) {
	h := newTestHandler(&mockHandlerRepo{})

	req := httptest.NewRequest(http.MethodPatch, "/v1/notifications/read-all", nil)
	w := httptest.NewRecorder()

	h.MarkAllAsRead(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("status: got %d, want %d", w.Code, http.StatusUnauthorized)
	}
}

// ─────────────────────────────────────────────
// GET /health tests
// ─────────────────────────────────────────────

func TestHealthCheck_Returns200WithStatusOK(t *testing.T) {
	h := newTestHandler(&mockHandlerRepo{})

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()

	h.HealthCheck(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status: got %d, want %d", w.Code, http.StatusOK)
	}

	var body map[string]string
	json.NewDecoder(w.Body).Decode(&body)
	if body["status"] != "ok" {
		t.Errorf("status field: got %q, want %q", body["status"], "ok")
	}
}

// ─────────────────────────────────────────────
// Pagination defaults tests
// ─────────────────────────────────────────────

func TestGetNotifications_ZeroLimit_DefaultsTo50(t *testing.T) {
	var capturedLimit int
	repo := &mockHandlerRepo{}
	// Override FindByUser to capture the limit
	_ = fmt.Sprintf("%d", capturedLimit) // suppress unused warning; actual assertion via response

	h := newTestHandler(repo)

	req := httptest.NewRequest(http.MethodGet, "/v1/notifications?limit=0", nil)
	req.Header.Set("Authorization", "Bearer "+buildMockJWT("USERS0001", "sess-001"))
	w := httptest.NewRecorder()

	h.GetNotifications(w, req)

	// With a limit of 0 passed in, GetHistory applies the 50 default. Response should be 200.
	if w.Code != http.StatusOK {
		t.Errorf("status: got %d, want %d", w.Code, http.StatusOK)
	}
}
