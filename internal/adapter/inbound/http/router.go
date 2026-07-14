package http

import (
	"net/http"

	httpSwagger "github.com/swaggo/http-swagger/v2"

	ws "nxt-msa-notifications/internal/adapter/outbound/websocket"
)

// NewRouter wires all HTTP and WebSocket routes onto a standard ServeMux using Go 1.22+ patterns.
func NewRouter(handler *Handler, hub *ws.Hub) *http.ServeMux {
	mux := http.NewServeMux()

	// Swagger UI — available at /api/index.html
	mux.Handle("/api/", httpSwagger.WrapHandler)

	// Liveness probe
	mux.HandleFunc("GET /health", handler.HealthCheck)

	// WebSocket endpoint — JWT passed as ?token= query param
	mux.HandleFunc("GET /v1/notifications/ws", func(w http.ResponseWriter, r *http.Request) {
		ws.ServeWS(hub, w, r)
	})

	// Unread count (bell badge fallback)
	mux.HandleFunc("GET /v1/notifications/count", handler.GetUnreadCount)

	// Mark all notifications read
	mux.HandleFunc("PATCH /v1/notifications/read-all", handler.MarkAllAsRead)

	// Paginated history
	mux.HandleFunc("GET /v1/notifications", handler.GetNotifications)

	// Mark single notification read
	mux.HandleFunc("PATCH /v1/notifications/{id}/read", handler.MarkAsRead)

	return mux
}
