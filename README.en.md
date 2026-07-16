# nxt-msa-notifications

> **Real-Time & Scalable Notification Microservice** — Core service of the **NXT Platform** responsible for real-time WebSocket push notifications, automatic offline catch-up, and persistent notification history across multi-channel delivery (WebSocket, Email, SMS).

[![Go](https://img.shields.io/badge/Go-1.26+-00ADD8?logo=go&logoColor=white)](https://go.dev/)
[![Architecture](https://img.shields.io/badge/Architecture-Hexagonal-brightgreen.svg)](https://alistair.cockburn.us/hexagonal-architecture/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16%2B%20Partitioned-336791?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![RabbitMQ](https://img.shields.io/badge/RabbitMQ-3.13%2B%20Streams%20%2F%20Quorum-FF6600?logo=rabbitmq&logoColor=white)](https://www.rabbitmq.com/)
[![WebSockets](https://img.shields.io/badge/WebSockets-Real--Time-blue)](https://developer.mozilla.org/en-US/docs/Web/API/WebSockets_API)
[![Docker](https://img.shields.io/badge/Docker-Enabled-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)

🌐 **Language / Idioma**: 🇺🇸 English · [🇪🇸 Español (predeterminado)](README.md)

---

## Table of Contents

- [Overview](#overview)
- [Technology Stack](#technology-stack)
- [Key Features](#key-features)
- [Architecture & Design Patterns](#architecture--design-patterns)
  - [Hexagonal Architecture (Ports & Adapters)](#hexagonal-architecture-ports--adapters)
  - [Hybrid Messaging Topology: Quorum Queues vs. RabbitMQ Streams](#hybrid-messaging-topology-quorum-queues-vs-rabbitmq-streams)
  - [Real-Time WebSocket Hub & Automatic Catch-Up](#real-time-websocket-hub--automatic-catch-up)
  - [Database Partitioning & Read/Write Replication](#database-partitioning--readwrite-replication)
  - [Multi-Channel Delivery Abstraction](#multi-channel-delivery-abstraction)
- [Domain Model & Notification Lifecycle](#domain-model--notification-lifecycle)
- [Project Structure](#project-structure)
- [Requirements & Prerequisites](#requirements--prerequisites)
- [Configuration & Environment Variables](#configuration--environment-variables)
- [Local Development & Running](#local-development--running)
  - [Running with Docker Compose](#running-with-docker-compose)
  - [Running Standalone](#running-standalone)
- [API & WebSocket Reference](#api--websocket-reference)
  - [REST Endpoints](#rest-endpoints)
  - [WebSocket Protocol](#websocket-protocol)
- [Security & Authentication](#security--authentication)
- [Integration with Java Ecosystem (nxt-msa-commons)](#integration-with-java-ecosystem-nxt-msa-commons)
- [Testing Strategy](#testing-strategy)
- [Deployment & Production Considerations](#deployment--production-considerations)
- [License](#license)

---

## Overview

`nxt-msa-notifications` is a high-concurrency, low-latency microservice built in **Go 1.26+** designed to serve as the notification distribution engine for the NXT platform. It provides reliable push delivery across multiple channels (WebSocket, Email, SMS) with automatic offline catch-up and persistent notification history.

This service solves two critical challenges in modern microservice architectures:
1. **Guaranteed At-Least-Once Persistence**: Ensuring that every event emitted by core backend services is reliably persisted without overwhelming the primary database.
2. **Instantaneous Real-Time Delivery & Offline Catch-Up**: Delivering notifications to active browser/mobile sessions via WebSockets in milliseconds, while automatically pushing missed notifications to users the exact moment they reconnect — **eliminating frontend polling entirely**.

---

## Technology Stack

- **Language Runtime:** Go 1.26+ (utilizing `net/http` with Go 1.22+ route patterns, `log/slog` structured logging).
- **Real-Time WebSockets:** `github.com/gorilla/websocket` (v1.5.3) for connection upgrading, heartbeat frame support, and buffered writing.
- **AMQP Broker Integration:** `github.com/rabbitmq/amqp091-go` (v1.12.0) for connection pooling and durable consensus operations (Quorum Queues).
- **Stream Broker Integration:** `github.com/rabbitmq/rabbitmq-stream-go-client` (v1.8.1) for high-performance Stream protocol consumer attachment and offset recovery (`QueryOffset`).
- **Database Driver & Mapping:**
  - `github.com/lib/pq` (v1.12.3) as the pure Go PostgreSQL driver.
  - `github.com/jmoiron/sqlx` (v1.4.0) for lightweight structural binding and dual-pool (primary/replica) management.
- **Cloud Infrastructure:** `github.com/aws/aws-sdk-go-v2` + `secretsmanager` for secure database credential queries.
- **API Documentation:** Swagger/OpenAPI via `github.com/swaggo/http-swagger/v2`.
- **JWT Decode-Only:** Base64 decode of Cognito JWT payload — no signature verification (delegated to API Gateway).

---

## Key Features

- ⚡ **Real-Time WebSocket Push**: Bi-directional WebSocket communication hub with automated session management, heartbeat monitoring (ping/pong), and dead-connection pruning.
- 🔄 **Automatic Offline Catch-Up**: When a client establishes a WebSocket connection, the hub automatically queries PostgreSQL for unread notifications during their offline window and pushes them immediately to populate UI alert badges.
- 🐇 **Hybrid RabbitMQ Messaging Topology**:
  - **Quorum Queues**: Highly available, Raft-consensus FIFO queues dedicated to asynchronous database persistence (competing consumers).
  - **RabbitMQ Streams**: Append-only log structures providing O(1) non-destructive broadcast fan-out across multiple Kubernetes pods.
- 📬 **Multi-Channel Delivery Abstraction**: Plug-in `Notifier` interface supporting WebSocket delivery today, with empty adapter slots for Email and SMS — new channels added without use-case changes.
- 🗄️ **Partitioned PostgreSQL & Read/Write Pooling**:
  - Monthly declarative table partitioning (`notifications.notification_y2026m07`) for infinite horizontal scaling.
  - Partial B-Tree index on unread items (`status != 'read'`) for sub-millisecond badge count queries.
  - Split connection pools isolating write traffic (primary DB) from high-volume read queries (read replicas).
- 🔐 **Zero-Trust JWT Authentication**:
  - Decode-only JWT parsing matching Java Spring Security (`JwtAuthenticationFilter`) standards.
  - AWS Cognito claims (`custom:iduser`, `custom:role`, `custom:hierarchyId`) extracted from token payload.
- ☁️ **Cloud Native & AWS Ready**:
  - Integrated AWS Secrets Manager client for automated database credential rotation.
  - Configurable SSL/TLS database modes (`require` for AWS Secrets Manager / RDS, `disable` for local development).

---

## Architecture & Design Patterns

### Hexagonal Architecture (Ports & Adapters)

The codebase strictly adheres to **Hexagonal Architecture (Ports & Adapters)**, ensuring total isolation between business logic and infrastructure concerns.

### Hybrid Messaging Topology: Quorum Queues vs. RabbitMQ Streams

A defining architectural feature is its **hybrid messaging topology**. Rather than relying on a single queue type, the system leverages Quorum Queues for database persistence and RabbitMQ Streams for real-time broadcast.

#### Why Quorum Queues for Database Persistence?
- **Transactional Safety & Consensus**: Quorum Queues use the Raft consensus algorithm across RabbitMQ cluster nodes.
- **Competing Consumers (Work Queue Model)**: Each message is delivered to **exactly one worker instance**, ensuring SQL `INSERT` operations are executed without duplication.
- **Idempotency via `ON CONFLICT DO NOTHING`**: If a pod crashes after write but before ACK, the redelivered message is silently skipped.
- **Poison Message Handling**: Malformed messages are dead-lettered; transient DB failures trigger requeue with automatic redelivery.

#### Why RabbitMQ Streams for WebSocket Fan-Out?

> **Team note**: RabbitMQ Streams work in a fundamentally different way from classic or quorum queues. If you come from an AMQP 0-9-1 background, this model may surprise you.

##### Streams Are Immutable Logs (Append-Only)

Traditional queues are **destructive**: messages disappear once acknowledged. Streams, on the other hand, are persistent, immutable records where information is only appended at the end — exactly like Kafka.

**Messages are never removed by an ACK.** They only leave the Stream according to configured retention policies (maximum age `x-max-age` or maximum log size). This means **multiple consumers can independently read the same messages** — a fundamental requirement for broadcasting to all pods.

##### Consumer Progress: Offsets Instead of ACKs

Instead of per-message ACKs, Streams use **offsets** — 64-bit integers representing the exact position of a message in the log.

Each service pod registers with a unique consumer name (`{POD_NAME}:{STREAM_NAME}`) and the broker stores its progress independently. This enables:

- **O(1) Fan-Out**: A single Stream, N independent readers — no copy overhead at the broker.
- **Offset Recovery (`QueryOffset`)**: On restart, each pod queries its last committed offset and resumes from `offset + 1` — no message loss, no unnecessary re-broadcast.
- **Full Replay**: Unlike classic queues where an ACK is irreversible, Streams allow re-reading from any position by resetting the offset — invaluable for debugging and disaster recovery.

##### Model Comparison

| Feature | Classic / Quorum Queues | RabbitMQ Streams |
| :--- | :--- | :--- |
| **Consumer Progress** | Per-message ACKs (`basic.ack`) | Offsets (position pointers) |
| **Message Lifetime** | Deleted after ACK | Defined by retention policy (age/size) |
| **Competing Consumers** | Distributes messages *between* consumers | Each consumer can read *all* messages |
| **Replay** | Impossible once ACKed | Fully supported (reset offset) |
| **Protocol** | AMQP 0-9-1 | Native Stream Protocol (port 5552) |

##### Implementation: Auto-Commit of Offsets

Rather than confirming each message individually (which would add significant network overhead), the consumer uses **dual-threshold auto-commit**: it commits the offset to the broker every 500 processed messages *or* every 5 seconds — whichever fires first. This guarantees a predictable maximum replay window in the event of a pod failure.

---

### Real-Time WebSocket Hub & Automatic Catch-Up

The WebSocket Hub (`internal/adapter/outbound/websocket/hub.go`) acts as an in-memory session registry and event dispatcher. It maintains thread-safe mappings of active client connections keyed by `user_id`, supporting multiple simultaneous connections per user (multi-device/multi-tab).

---

### Database Partitioning & Read/Write Replication

1. **Declarative Monthly Partitioning**: The master table `notifications.notification` is partitioned by range on `created_at`. Child tables are created via migration, keeping index trees small.
2. **Partial Unread Indexes**: A partial B-Tree index on `status != 'read'` reduces index size by over 95%, enabling sub-millisecond badge counts.
3. **Dual Pool Architecture**: Primary pool for writes (Quorum consumer INSERTs), Read Replica pool for reads (pagination, catch-up).

### Multi-Channel Delivery Abstraction

The `outbound.Notifier` interface defines a single `Send()` method, allowing delivery to any channel without use-case changes. Currently: WebSocket Hub (live), Email (placeholder), SMS (placeholder).

---

## Domain Model & Notification Lifecycle

### Notification Status Lifecycle

```
Published → Persisted (Quorum → PostgreSQL)
Published → DeliveredLive (Stream → WebSocket)
Persisted → DeliveredCatchUp (Client reconnects)
DeliveredLive / DeliveredCatchUp → Unread → Read
```

Notification IDs are deterministic UUID v5 from `(EventID + ":" + UserID)`, ensuring identical IDs across both the DB persistence and real-time dispatch paths.

---

## Project Structure

```
nxt-msa-notifications/
├── cmd/server/main.go               # Entry point & dependency wiring
├── config/config.go                 # Environment variable parsing
├── internal/
│   ├── adapter/inbound/
│   │   ├── http/                    # REST handlers, router
│   │   └── rabbitmq/
│   │       ├── quorum_consumer.go   # Quorum consumer (DB persistence)
│   │       └── stream_consumer.go   # Stream consumer (real-time fan-out)
│   ├── adapter/outbound/
│   │   ├── postgres/repository.go   # Dual-pool SQL repository
│   │   └── websocket/               # Hub, client, upgrader
│   ├── domain/                      # Notification entity, event contract, ID gen
│   ├── port/                        # Inbound/outbound port interfaces
│   └── usecase/                     # Dispatch, acknowledge, history
├── migrations/001_create_notifications.sql
├── README.md                        # 🇪🇸 Spanish (default)
└── README.en.md                     # 🇺🇸 English (this file)
```

---

## Requirements & Prerequisites

| Component | Version | Notes |
| :--- | :--- | :--- |
| **Go** | `1.26+` | `http.ServeMux` patterns + `log/slog` |
| **PostgreSQL** | `16.0+` | Range partitioning + partial indexes |
| **RabbitMQ** | `3.13+` | Stream Protocol (port 5552) + Quorum Queues |
| **Docker & Compose** | `24.0+` | Local dev stack |
| **AWS CLI** | Cloud/Prod only | When `DB_SECRET_NAME` is set |

---

## Configuration & Environment Variables

Configured entirely via environment variables (12-Factor App). Resource names are derived from `APP_ENV`.

| Variable | Default | Description |
| :--- | :--- | :--- |
| `APP_ENV` | `dev` | Profile (`dev`, `qa`, `sbx`) — drives resource naming |
| `SERVER_PORT` | `8085` | HTTP/WebSocket port |
| `DB_HOST` | `localhost` | Primary DB host |
| `DB_HOST_RO` | `localhost` | Read-replica host |
| `DB_SSL_MODE` | `disable` | `disable` for Docker, `require` for AWS/RDS |
| `DB_SECRET_NAME` | *(empty)* | AWS Secrets Manager — overrides DB credentials when set |
| `AMQP_URI` | `amqp://guest:guest@localhost:5672/` | AMQP connection string |
| `STREAM_URI` | `rabbitmq-stream://guest:guest@localhost:5552/` | Stream protocol URI |
| `STREAM_NAME` | `sps-{env}-notifications-queue-broadcast` | RabbitMQ Stream name |
| `STREAM_MAX_AGE_SECS` | `86400` | Stream retention (24h) |
| `POD_NAME` | *(hostname)* | Unique consumer name for offset tracking |

---

## Local Development & Running

```bash
# Start infrastructure
docker compose up -d

# Run service
go run cmd/server/main.go

# Run tests (with race detection)
go test -v -race ./...

# Build
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o bin/nxt-notifications cmd/server/main.go
```

> RabbitMQ Management UI: `http://localhost:15672` (guest/guest) · Swagger UI: `http://localhost:8085/api/`

---

## API & WebSocket Reference

### REST Endpoints (all require `Authorization: Bearer <jwt>`)

| Method | Path | Description | Response |
| :--- | :--- | :--- | :--- |
| `GET` | `/health` | Health check | `200 {"status":"ok"}` |
| `GET` | `/v1/notifications` | Paginated history (`?limit=20&offset=0&unread=true`) | `200 {"notifications":[],"total_count":N,"returned_count":N}` |
| `GET` | `/v1/notifications/count` | Unread count | `200 {"unread_count":N}` |
| `PATCH` | `/v1/notifications/{id}/read` | Mark single as read | `204` |
| `PATCH` | `/v1/notifications/read-all` | Mark all as read | `204` |
| `GET` | `/api/` | Swagger UI | `200` |

### WebSocket

```http
GET /v1/notifications/ws?token=<jwt>
```

Server pushes two message types:
- `{"type":"notification", "notification":{...}}` — live event
- `{"type":"catch_up", "notifications":[...], "unread_count":N}` — on connect

Heartbeat: server PING every 54s, client must PONG within 60s.

---

## Security & Authentication

Decode-only JWT (no signature verification — delegated to API Gateway). Extracts `custom:iduser`, `custom:role`, `custom:hierarchyId`, `jti`. All queries scoped by `user_id`.

---

## Integration with Java Ecosystem (`nxt-msa-commons`)

Java microservices publish via `NotificationPublisher` (Spring Boot 4 / Java 21). The starter publishes to `sps-{env}-notifications-exchange-events`. RabbitMQ routes to both the Quorum Queue (DB) and Stream (WebSocket fan-out).

---

## Testing Strategy

Standard library only — no external mock frameworks. Tests are race-safe (`-race`), layer-isolated, and deterministic.

| File | Layer | Key Assertions |
|------|-------|----------------|
| `domain/notification_test.go` | Domain | UUID v5 determinism |
| `middleware/jwt_test.go` | Middleware | Claim extraction, Bearer stripping |
| `usecase/dispatch_test.go` | Use Case | ID contract, channel routing |
| `http/handler_test.go` | HTTP | All routes, 401s, 204s, pagination |

---

## Deployment & Production Considerations

1. **Sticky Sessions**: Required at Ingress layer for WebSocket reconnection reliability.
2. **Partition Maintenance**: Create monthly partitions; drop partitions older than 90 days.
3. **Stream Retention**: `STREAM_MAX_AGE_SECS=86400` (24h). Each pod resumes from its own `QueryOffset` on restart.
4. **Graceful Shutdown**: `SIGTERM` → drain HTTP → unsubscribe consumers → close DB pools.

---

## License

This project is proprietary software owned by **Smart Payment Services**. All rights reserved.
