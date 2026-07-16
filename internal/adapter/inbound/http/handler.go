package http

import (
	"encoding/json"
	"net/http"
	"nxt-msa-notifications/internal/adapter/middleware"
	"nxt-msa-notifications/internal/domain"
	"nxt-msa-notifications/internal/usecase"
	"strconv"
)

// ─────────────────────────────────────────────────────────────
// Response types — used by handlers and referenced in swag annotations
// ─────────────────────────────────────────────────────────────

// NotificationListResponse is the payload returned by GET /v1/notifications.
// total_count is the total number of notifications matching the filter (for pagination UI).
// returned_count is the number of items in the current page (convenience for clients).
type NotificationListResponse struct {
	Notifications []domain.Notification `json:"notifications"`
	TotalCount    int                   `json:"total_count"`
	ReturnedCount int                   `json:"returned_count"`
}

// UnreadCountResponse is the payload returned by GET /v1/notifications/count.
type UnreadCountResponse struct {
	UnreadCount int `json:"unread_count"`
}

// ErrorResponse is the standard error envelope returned on 4xx/5xx responses.
type ErrorResponse struct {
	Error string `json:"error"`
}

// HealthResponse is the payload returned by GET /health.
type HealthResponse struct {
	Status string `json:"status"`
}

// ─────────────────────────────────────────────────────────────
// Handler
// ─────────────────────────────────────────────────────────────

// Handler holds the use case dependencies for all HTTP endpoints.
type Handler struct {
	history     *usecase.HistoryUseCase
	acknowledge *usecase.AcknowledgeUseCase
}

func NewHandler(history *usecase.HistoryUseCase, acknowledge *usecase.AcknowledgeUseCase) *Handler {
	return &Handler{history: history, acknowledge: acknowledge}
}

// GetNotifications handles GET /v1/notifications
//
//	@Summary		List notifications for the authenticated user
//	@Description	Returns a paginated list of notifications ordered by created_at DESC.
//	@Description	Pass unread=true to filter only unread items.
//	@Tags			notifications
//	@Produce		json
//	@Security		BearerAuth
//	@Param			unread	query		boolean					false	"Return only unread notifications"
//	@Param			limit	query		integer					false	"Maximum results to return (default 50)"
//	@Param			offset	query		integer					false	"Pagination offset (default 0)"
//	@Success		200		{object}	NotificationListResponse	"Notification list with total_count and returned_count"
//	@Failure		401		{object}	ErrorResponse				"Unauthorized — missing or invalid token"
//	@Failure		500		{object}	ErrorResponse				"Internal server error"
//	@Router			/notifications [get]
func (h *Handler) GetNotifications(w http.ResponseWriter, r *http.Request) {
	claims, err := extractClaims(r)
	if err != nil {
		writeError(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	unreadOnly := r.URL.Query().Get("unread") == "true"
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))

	notifications, err := h.history.GetHistory(r.Context(), claims.UserID, unreadOnly, limit, offset)
	if err != nil {
		writeError(w, "internal server error", http.StatusInternalServerError)
		return
	}

	returnedCount := len(notifications)

	totalCount, err := h.history.GetTotalCount(r.Context(), claims.UserID, unreadOnly)
	if err != nil {
		writeError(w, "internal server error", http.StatusInternalServerError)
		return
	}

	writeJSON(w, NotificationListResponse{
		Notifications: notifications,
		TotalCount:    totalCount,
		ReturnedCount: returnedCount,
	})
}

// GetUnreadCount handles GET /v1/notifications/count
//
//	@Summary		Get unread notification count
//	@Description	Returns the total number of unread notifications for the authenticated user.
//	@Description	Intended for the notification bell badge. Queries the read-replica for low latency.
//	@Tags			notifications
//	@Produce		json
//	@Security		BearerAuth
//	@Success		200	{object}	UnreadCountResponse	"Unread notification count"
//	@Failure		401	{object}	ErrorResponse		"Unauthorized — missing or invalid token"
//	@Failure		500	{object}	ErrorResponse		"Internal server error"
//	@Router			/notifications/count [get]
func (h *Handler) GetUnreadCount(w http.ResponseWriter, r *http.Request) {
	claims, err := extractClaims(r)
	if err != nil {
		writeError(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	count, err := h.history.GetUnreadCount(r.Context(), claims.UserID)
	if err != nil {
		writeError(w, "internal server error", http.StatusInternalServerError)
		return
	}

	writeJSON(w, UnreadCountResponse{UnreadCount: count})
}

// MarkAsRead handles PATCH /v1/notifications/{id}/read
//
//	@Summary		Mark a single notification as read
//	@Description	Sets the delivery_status of the given notification to "read".
//	@Description	The notification must belong to the authenticated user.
//	@Tags			notifications
//	@Produce		json
//	@Security		BearerAuth
//	@Param			id	path	string	true	"Notification UUID"
//	@Success		204	"No Content — notification marked as read"
//	@Failure		400	{object}	ErrorResponse	"Bad request — invalid ID or ownership mismatch"
//	@Failure		401	{object}	ErrorResponse	"Unauthorized — missing or invalid token"
//	@Router			/notifications/{id}/read [patch]
func (h *Handler) MarkAsRead(w http.ResponseWriter, r *http.Request) {
	claims, err := extractClaims(r)
	if err != nil {
		writeError(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	notificationID := r.PathValue("id")

	if err := h.acknowledge.MarkAsRead(r.Context(), notificationID, claims.UserID); err != nil {
		writeError(w, "internal server error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// MarkAllAsRead handles PATCH /v1/notifications/read-all
//
//	@Summary		Mark all notifications as read
//	@Description	Sets delivery_status to "read" for every unread notification belonging
//	@Description	to the authenticated user in a single batch operation.
//	@Tags			notifications
//	@Produce		json
//	@Security		BearerAuth
//	@Success		204	"No Content — all notifications marked as read"
//	@Failure		401	{object}	ErrorResponse	"Unauthorized — missing or invalid token"
//	@Failure		500	{object}	ErrorResponse	"Internal server error"
//	@Router			/notifications/read-all [patch]
func (h *Handler) MarkAllAsRead(w http.ResponseWriter, r *http.Request) {
	claims, err := extractClaims(r)
	if err != nil {
		writeError(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	if err := h.acknowledge.MarkAllAsRead(r.Context(), claims.UserID); err != nil {
		writeError(w, "internal server error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// HealthCheck handles GET /health — used by load balancers and Kubernetes liveness probes.
//
//	@Summary		Service liveness probe
//	@Description	Returns HTTP 200 with {"status":"ok"} when the service is healthy.
//	@Description	Used by load balancers, Kubernetes liveness probes, and Docker healthchecks.
//	@Tags			ops
//	@Produce		json
//	@Success		200	{object}	HealthResponse	"Service is healthy"
//	@Router			/health [get]
func (h *Handler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, HealthResponse{Status: "ok"})
}

// extractClaims decodes the JWT from the Authorization header.
func extractClaims(r *http.Request) (*middleware.CognitoClaims, error) {
	auth := r.Header.Get("Authorization")
	return middleware.DecodeJWT(auth)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		// Header already sent — log only; cannot change status code at this point.
		// This path is only reachable if the response writer itself is broken.
		_ = err
	}
}

func writeError(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	// RFC 9110 §15.5.2: a 401 response MUST include a WWW-Authenticate challenge.
	if code == http.StatusUnauthorized {
		w.Header().Set("WWW-Authenticate", `Bearer realm="nxt-msa-notifications"`)
	}
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(ErrorResponse{Error: msg})
}
