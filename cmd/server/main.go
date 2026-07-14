package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"nxt-msa-notifications/config"
	_ "nxt-msa-notifications/docs"
	inboundhttp "nxt-msa-notifications/internal/adapter/inbound/http"
	"nxt-msa-notifications/internal/adapter/inbound/rabbitmq"
	pgRepo "nxt-msa-notifications/internal/adapter/outbound/postgres"
	wsAdapter "nxt-msa-notifications/internal/adapter/outbound/websocket"
	"nxt-msa-notifications/internal/domain"
	"nxt-msa-notifications/internal/infra/secrets"
	"nxt-msa-notifications/internal/port/outbound"
	"nxt-msa-notifications/internal/usecase"
)

// @title				NXT Notifications API
// @version			1.0
// @description		Real-time and persistent notification service for the NXT platform.
// @description		Messages are delivered over WebSocket (fan-out via RabbitMQ Stream) and
// @description		persisted to PostgreSQL (Quorum Queue, exactly-once semantics).
// @contact.name		NXT Platform Team
// @contact.email		platform@nxt.com
// @license.name		Proprietary
// @host				localhost:8085
// @BasePath			/v1
// @securityDefinitions.apikey	BearerAuth
// @in					header
// @name				Authorization
// @description		JWT token issued by AWS Cognito. Format: "Bearer <token>"
func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("[main] Failed to load configuration: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// ─────────────────────────────────────────────────────────────
	// 1. Resolve DB credentials
	// In production: fetch from AWS Secrets Manager (matching GenericDataSourceConfig pattern)
	// In local/dev: use environment variables directly from config.Load()
	// ─────────────────────────────────────────────────────────────
	if secretName := cfg.Database.SecretName; secretName != "" {
		log.Printf("[main] Fetching DB credentials from Secrets Manager: %s", secretName)
		fetcher, err := secrets.NewFetcher(ctx, secretName)
		if err != nil {
			log.Fatalf("[main] Secrets Manager init failed: %v", err)
		}
		secret, err := fetcher.Fetch(ctx)
		if err != nil {
			log.Fatalf("[main] Secret fetch failed: %v", err)
		}
		cfg.Database.Host = secret.Host
		cfg.Database.HostRO = secret.HostRO
		cfg.Database.User = secret.Username
		cfg.Database.Password = secret.Password
		cfg.Database.Name = secret.DBName
		cfg.Database.Port = secret.Port
		cfg.Database.SSLMode = "require" // Enforce SSL in AWS/production
	}

	// ─────────────────────────────────────────────────────────────
	// 2. PostgreSQL — primary (writes) + read-replica (reads)
	// ─────────────────────────────────────────────────────────────
	repo, err := pgRepo.NewRepository(cfg.Database.DSN(), cfg.Database.DSRRO())
	if err != nil {
		log.Fatalf("[main] PostgreSQL connection failed: %v", err)
	}
	defer repo.Close()
	log.Printf("[main] PostgreSQL connected (primary=%s, ro=%s)", cfg.Database.Host, cfg.Database.HostRO)

	// ─────────────────────────────────────────────────────────────
	// 3. WebSocket Hub — thread-safe session manager
	// ─────────────────────────────────────────────────────────────
	hub := wsAdapter.NewHub(repo)

	// ─────────────────────────────────────────────────────────────
	// 4. Use Cases — wire repo + notifiers
	// ─────────────────────────────────────────────────────────────
	notifiers := []outbound.Notifier{hub} // Hub implements outbound.Notifier
	dispatchUC := usecase.NewDispatchUseCase(repo, notifiers)
	acknowledgeUC := usecase.NewAcknowledgeUseCase(repo)
	historyUC := usecase.NewHistoryUseCase(repo)

	// ─────────────────────────────────────────────────────────────
	// 5. AMQP Consumers
	// ─────────────────────────────────────────────────────────────

	// Quorum Queue consumer — DB persistence (competing consumers, exactly-once write)
	quorumConsumer := rabbitmq.NewQuorumConsumer(
		cfg.AMQP.URI,
		cfg.AMQP.PersistQueue,
		cfg.AMQP.Exchange,
		cfg.AMQP.RoutingKey,
		cfg.Stream.StreamName,
		cfg.Server.PodName,
		func(ctx context.Context, event domain.NotificationEvent) error {
			return dispatchUC.HandleDBWrite(ctx, event)
		},
	)

	// Stream consumer — real-time fan-out (all pods read independently via named offset)
	streamConsumer := rabbitmq.NewStreamConsumer(
		cfg.Stream.URI,
		cfg.Stream.StreamName,
		cfg.Server.PodName,
		cfg.Stream.MaxAgeSecs,
		func(ctx context.Context, event domain.NotificationEvent) {
			dispatchUC.HandleRealTimeDispatch(ctx, event)
		},
	)

	// Start both consumers in background goroutines
	go func() {
		if err := quorumConsumer.Start(ctx); err != nil {
			log.Printf("[main] Quorum consumer exited: %v", err)
		}
	}()

	go func() {
		if err := streamConsumer.Start(ctx); err != nil {
			log.Printf("[main] Stream consumer exited: %v", err)
		}
	}()

	// ─────────────────────────────────────────────────────────────
	// 6. HTTP + WebSocket Server
	// ─────────────────────────────────────────────────────────────
	httpHandler := inboundhttp.NewHandler(historyUC, acknowledgeUC)
	router := inboundhttp.NewRouter(httpHandler, hub)

	server := &http.Server{
		Addr:         fmt.Sprintf(":%s", cfg.Server.Port),
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Printf("[main] nxt-msa-notifications listening on :%s (pod=%s)", cfg.Server.Port, cfg.Server.PodName)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[main] HTTP server error: %v", err)
		}
	}()

	// ─────────────────────────────────────────────────────────────
	// 7. Graceful Shutdown
	// ─────────────────────────────────────────────────────────────
	<-ctx.Done()
	log.Println("[main] Shutdown signal received — draining connections...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("[main] HTTP server shutdown error: %v", err)
	}

	log.Println("[main] nxt-msa-notifications stopped cleanly")
}
