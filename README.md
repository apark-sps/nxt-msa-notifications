# nxt-msa-notifications

> **Real-Time & Scalable Notification Microservice** — Core service of the **NXT Platform** responsible for real-time WebSocket push notifications, automatic offline catch-up, and persistent notification history across multi-channel delivery (WebSocket, Email, SMS).

[![Go](https://img.shields.io/badge/Go-1.26+-00ADD8?logo=go&logoColor=white)](https://go.dev/)
[![Architecture](https://img.shields.io/badge/Architecture-Hexagonal-brightgreen.svg)]()
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16%2B%20Partitioned-336791?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![RabbitMQ](https://img.shields.io/badge/RabbitMQ-3.13%2B%20Streams%20%2F%20Quorum-FF6600?logo=rabbitmq&logoColor=white)](https://www.rabbitmq.com/)
[![WebSockets](https://img.shields.io/badge/WebSockets-Real--Time-blue)](https://developer.mozilla.org/en-US/docs/Web/API/WebSockets_API)
[![Docker](https://img.shields.io/badge/Docker-Enabled-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)

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
2. **Instantaneous Real-Time Delivery & Offline Catch-Up**: Delivering notifications to active browser/mobile sessions via WebSockets in milliseconds, while automatically pushing missed notifications to users the exact moment they reconnect—**eliminating frontend polling entirely**.

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

```mermaid
graph TD
    subgraph "Inbound Adapters (Driving)"
        REST["HTTP Handler (net/http ServeMux)"]
        WS["WebSocket Hub / Upgrader"]
        RMQ_IN["RabbitMQ Consumers (Quorum & Stream)"]
    end

    subgraph "Application Core (Ports & Use Cases)"
        IN_PORT["Inbound Port (event_consumer.go)"]
        UC_DISPATCH["DispatchUseCase"]
        UC_ACK["AcknowledgeUseCase"]
        UC_HISTORY["HistoryUseCase"]
        OUT_PORT_REPO["Outbound Port: NotificationRepository"]
        OUT_PORT_NOTIFIER["Outbound Port: Notifier"]
    end

    subgraph "Outbound Adapters (Driven)"
        PG_ADAPTER["PostgreSQL Repository (sqlx + lib/pq)"]
        WS_OUT["WebSocket Hub (implements Notifier)"]
        EMAIL["Email Adapter (placeholder)"]
        SMS["SMS Adapter (placeholder)"]
        AWS_ADAPTER["AWS Secrets Manager Fetcher"]
    end

    REST --> IN_PORT
    WS --> IN_PORT
    RMQ_IN --> IN_PORT

    IN_PORT --> UC_DISPATCH
    IN_PORT --> UC_ACK
    IN_PORT --> UC_HISTORY

    UC_DISPATCH --> OUT_PORT_REPO
    UC_DISPATCH --> OUT_PORT_NOTIFIER
    UC_ACK --> OUT_PORT_REPO
    UC_HISTORY --> OUT_PORT_REPO

    OUT_PORT_REPO --> PG_ADAPTER
    OUT_PORT_NOTIFIER --> WS_OUT
    OUT_PORT_NOTIFIER --> EMAIL
    OUT_PORT_NOTIFIER --> SMS
    PG_ADAPTER -.->|"Credential Rotation"| AWS_ADAPTER
```

### Hybrid Messaging Topology: Quorum Queues vs. RabbitMQ Streams

A defining architectural feature is its **hybrid messaging topology**. Rather than relying on a single queue type, the system leverages Quorum Queues for database persistence and RabbitMQ Streams for real-time broadcast.

```mermaid
graph LR
    subgraph "Producers (Java Microservices)"
        PUB["NotificationPublisher (nxt-msa-commons)"]
    end

    subgraph "RabbitMQ Broker (3.13+)"
        EX["Topic Exchange: sps-{env}-notifications-exchange-events"]
        QQ["Quorum Queue: sps-{env}-notifications-queue-persist"]
        RS["RabbitMQ Stream: sps-{env}-notifications-queue-broadcast"]
    end

    subgraph "nxt-msa-notifications (Go Service)"
        subgraph "Worker Pod"
            QC["Quorum Consumer (competing consumers)"]
            SC["Stream Consumer (named offset)"]
            HUB["WebSocket Hub"]
            DB_POOL["Primary DB Pool"]
            DB_REPLICA["Read Replica Pool"]
        end
    end

    PUB -->|"Publish (Routing Key: #)"| EX
    EX -->|"Binding: #"| QQ
    EX -->|"Binding: #"| RS

    QQ -->|"Competing Consumers"| QC
    QC -->|"INSERT ON CONFLICT DO NOTHING"| DB_POOL

    RS -->|"Non-Destructive Fan-Out (Per-Pod Offset)"| SC
    SC -->|"Push to Local Active Sessions"| HUB
```

#### Why Quorum Queues for Database Persistence?
- **Transactional Safety & Consensus**: Quorum Queues use the Raft consensus algorithm across RabbitMQ cluster nodes.
- **Competing Consumers (Work Queue Model)**: Each message is delivered to **exactly one worker instance**, ensuring SQL `INSERT` operations are executed without duplication.
- **Idempotency via `ON CONFLICT DO NOTHING`**: If a pod crashes after write but before ACK, the redelivered message is silently skipped.
- **Poison Message Handling**: Malformed messages are dead-lettered; transient DB failures trigger requeue with automatic redelivery.

#### Why RabbitMQ Streams for WebSocket Fan-Out?
- **Non-Destructive Read & Fan-Out**: Multiple consumers read the same messages independently without removing them.
- **O(1) Broadcast Across Pods**: Every running pod receives every message and checks if the target user is connected locally.
- **Independent Pod Offsets (`QueryOffset`)**: Each pod registers with a unique consumer name (`podName:streamName`). On restart, `QueryOffset` returns the last committed offset, enabling seamless resume without message loss or re-broadcast.

---

### Real-Time WebSocket Hub & Automatic Catch-Up

The WebSocket Hub (`internal/adapter/outbound/websocket/hub.go`) acts as an in-memory session registry and event dispatcher. It maintains thread-safe mappings of active client connections keyed by `user_id`, supporting multiple simultaneous connections per user (multi-device/multi-tab).

#### Automatic Catch-Up Workflow

```mermaid
sequenceDiagram
    autonumber
    participant Client as "Frontend Client"
    participant WS as "WebSocket Upgrader"
    participant Hub as "WebSocket Hub"
    participant UC as "DispatchUseCase"
    participant DB as "PostgreSQL (Read Replica)"

    Client->>WS: Connect /v1/notifications/ws?token=<JWT>
    WS->>WS: Decode JWT (decode-only), extract user_id
    WS->>Hub: RegisterClient(client)
    Hub->>DB: Query Unread (status != 'read', limit 50)
    DB-->>Hub: Return List[Notification]
    Hub-->>Client: Push WebSocket Message (type: catch_up)
    Hub-->>Client: Push Unread Count
    Note over Client,DB: Client is synchronized — listening for live stream events
```

---

### Database Partitioning & Read/Write Replication

1. **Declarative Monthly Partitioning**: The master table `notifications.notification` is partitioned by range on `created_at`. Child tables (`notification_y2026m07`, `notification_y2026m08`, etc.) are created via migration. This ensures index trees remain small and allows instant archival without `VACUUM` locking.
2. **Partial Unread Indexes**:
   ```sql
   CREATE INDEX IF NOT EXISTS idx_notif_user_unread
       ON notifications.notification (user_id, created_at DESC)
       WHERE status != 'read';
   ```
   This reduces index size by over 95%, allowing unread badge counts to execute in sub-millisecond time.
3. **Dual Pool Architecture**: The repository configures two distinct `sqlx.DB` instances:
   - **Primary Pool**: Dedicated exclusively to Quorum consumer `INSERT` operations and UPDATE statements.
   - **Read Replica Pool**: Dedicated to pagination queries and WebSocket catch-up reads, preventing read spikes from impacting ingestion throughput.

### Multi-Channel Delivery Abstraction

The `outbound.Notifier` interface defines a single `Send()` method, allowing delivery to any channel without use-case changes:

```go
type Notifier interface {
    Channel() domain.Channel
    Send(ctx context.Context, notification *domain.Notification) error
}
```

Currently implemented adapters:
- **WebSocket Hub** — real-time push to connected browser/mobile sessions.
- **Email** — placeholder (directory ready for implementation).
- **SMS** — placeholder (directory ready for implementation).

The `DispatchUseCase.HandleRealTimeDispatch()` iterates over the event's requested channels and dispatches to each registered notifier in a separate goroutine, silently discarding unregistered channels.

---

## Domain Model & Notification Lifecycle

### Core Entity: `Notification`

```go
type Notification struct {
    ID          string            `json:"id"`            // UUID v5 (deterministic: EventID + UserID)
    UserID      string            `json:"user_id"`       // Primary route identifier ("USERS0001")
    HierarchyID *int              `json:"hierarchy_id"`  // Organizational scope (nullable)
    Type        string            `json:"type"`          // Event trigger type ("user.created")
    Title       string            `json:"title"`
    Body        string            `json:"body"`
    Metadata    map[string]string `json:"metadata"`      // Arbitrary key-value payload
    Channels    []Channel         `json:"channels"`      // Requested delivery channels
    Status      DeliveryStatus    `json:"status"`        // pending | delivered | read | failed
    CreatedAt   time.Time         `json:"created_at"`
    ReadAt      *time.Time        `json:"read_at,omitempty"`
}
```

### Incoming Event Contract: `NotificationEvent`

```go
type NotificationEvent struct {
    EventID     string            `json:"event_id"`     // Idempotency key
    Source      string            `json:"source"`       // Originating microservice
    Type        string            `json:"type"`         // Event trigger type
    UserIDs     []string          `json:"user_ids"`     // Target recipients
    HierarchyID *int              `json:"hierarchy_id"` // Organizational scope
    Title       string            `json:"title"`
    Body        string            `json:"body"`
    Metadata    map[string]string `json:"metadata"`
    Channels    []Channel         `json:"channels"`     // ["websocket", "email", "sms"]
    OccurredAt  time.Time         `json:"occurred_at"`
}
```

### Notification Status Lifecycle

```mermaid
stateDiagram-v2
    [*] --> Published: Event Emitted by Java Microservice
    Published --> Persisted: Quorum Consumer INSERT (PostgreSQL)
    Published --> DeliveredLive: Stream Consumer → Notifier.Send()
    Persisted --> DeliveredCatchUp: Client Reconnects → Hub.sendCatchUp()
    DeliveredLive --> Unread: Displayed in UI Badge
    DeliveredCatchUp --> Unread: Displayed in UI Badge
    Unread --> Read: User Clicks (PATCH /read or /read-all)
    Read --> [*]
```

### Deterministic ID Generation

Notification IDs are generated as UUID v5 from `(EventID + ":" + UserID)`, ensuring the same event produces identical IDs across the DB persistence path and the real-time dispatch path. This is verified by `TestHandleRealTimeDispatch_IDMatchesDBWriteID`.

---

## Project Structure

```
nxt-msa-notifications/
├── cmd/
│   └── server/
│       └── main.go                  # Application entry point & dependency wiring
├── config/
│   └── config.go                    # Environment variable parsing (env-aware naming)
├── docker-compose.yml               # Local dev stack (Postgres 16, RabbitMQ 3.13+)
├── Dockerfile                       # Multi-stage build (Alpine builder → distroless)
├── go.mod / go.sum                  # Go module dependencies
├── internal/
│   ├── adapter/
│   │   ├── inbound/
│   │   │   ├── http/
│   │   │   │   ├── handler.go       # REST endpoint handlers
│   │   │   │   ├── handler_test.go  # HTTP handler tests
│   │   │   │   └── router.go        # Route wiring (Go 1.22+ ServeMux patterns)
│   │   │   └── rabbitmq/
│   │   │       ├── quorum_consumer.go # Quorum queue consumer (DB persistence)
│   │   │       └── stream_consumer.go # Stream consumer (real-time fan-out)
│   │   ├── middleware/
│   │   │   ├── jwt.go               # Decode-only JWT parsing (Cognito claims)
│   │   │   └── jwt_test.go
│   │   └── outbound/
│   │       ├── email/               # Email notifier (placeholder)
│   │       ├── postgres/
│   │       │   └── repository.go    # Dual-pool SQL repository (primary/replica)
│   │       ├── sms/                 # SMS notifier (placeholder)
│   │       └── websocket/
│   │           ├── client.go        # Individual WS connection lifecycle & pumps
│   │           ├── hub.go           # Central session registry & broadcaster
│   │           └── upgrader.go      # WS upgrade handler with JWT auth
│   ├── domain/
│   │   ├── channel.go               # Channel type constants (websocket, email, sms)
│   │   ├── event.go                 # NotificationEvent incoming contract
│   │   ├── notification.go          # Notification entity & ID generation
│   │   └── notification_test.go     # Domain unit tests
│   ├── infra/
│   │   └── secrets/
│   │       └── aws.go               # AWS Secrets Manager credential fetcher
│   ├── port/
│   │   ├── inbound/
│   │   │   └── event_consumer.go    # Inbound port interfaces (EventConsumer, handlers)
│   │   └── outbound/
│   │       ├── notifier.go          # Outbound Notifier interface
│   │       └── repository.go        # Outbound NotificationRepository interface
│   └── usecase/
│       ├── acknowledge.go           # MarkAsRead / MarkAllAsRead use cases
│       ├── dispatch.go              # DB persistence + real-time dispatch use case
│       ├── dispatch_test.go         # Dispatch use case tests
│       └── history.go               # GetHistory / GetUnreadCount use cases
├── docs/
│   ├── docs.go                      # Swagger-generated documentation
│   ├── swagger.json
│   └── swagger.yaml
├── migrations/
│   └── 001_create_notifications.sql # Partitioned table + indexes
└── .env                             # Local development environment
```

---

## Requirements & Prerequisites

| Component | Version / Requirement | Notes |
| :--- | :--- | :--- |
| **Go (SDK)** | `1.26+` | Required for `http.ServeMux` pattern matching and `log/slog` |
| **PostgreSQL** | `16.0+` | Requires range partitioning and partial B-Tree index support |
| **RabbitMQ** | `3.13+` | **Critical**: Must support Stream Protocol (Port 5552) and Quorum Queues |
| **Docker & Compose** | `24.0+` | Recommended for spinning up local dependency stack |
| **AWS CLI / Credentials** | Required for Cloud / Prod | Needed only when `DB_SECRET_NAME` is configured |

---

## Configuration & Environment Variables

The application is configured entirely via environment variables, following the **12-Factor App** methodology. Exchange, queue, and stream names are automatically derived from `APP_ENV` (e.g., `sps-dev-notifications-exchange-events`, `sps-qa-notifications-queue-persist`).

| Variable Name | Default Value | Description |
| :--- | :--- | :--- |
| `APP_ENV` | `dev` | Environment profile (`dev`, `qa`, `sbx`) — drives resource naming |
| `SERVER_PORT` | `8085` | HTTP / WebSocket server listening port |
| `DB_HOST` | `localhost` | Primary (write) database host |
| `DB_HOST_RO` | `localhost` | Read-replica database host |
| `DB_PORT` | `5432` | Database port |
| `DB_NAME` | `notifications` | Database name |
| `DB_USER` | *(empty)* | Database user |
| `DB_PASSWORD` | *(empty)* | Database password |
| `DB_SSL_MODE` | `disable` | SSL mode (`disable` for local Docker, `require` for AWS/RDS) |
| `DB_SECRET_NAME` | *(empty)* | AWS Secrets Manager secret name — overrides DB_HOST/DB_USER/DB_PASSWORD when set |
| `AMQP_URI` | `amqp://guest:guest@localhost:5672/` | AMQP connection string |
| `AMQP_EXCHANGE` | `sps-{env}-notifications-exchange-events` | Topic exchange name |
| `AMQP_PERSIST_QUEUE` | `sps-{env}-notifications-queue-persist` | Quorum queue name |
| `AMQP_ROUTING_KEY` | `#` | Routing key binding for both consumers |
| `STREAM_URI` | `rabbitmq-stream://guest:guest@localhost:5552/` | Stream protocol connection string |
| `STREAM_NAME` | `sps-{env}-notifications-queue-broadcast` | RabbitMQ Stream name |
| `STREAM_MAX_AGE_SECS` | `86400` | Stream retention window (24 hours) |
| `POD_NAME` | *(hostname)* | Unique consumer name for stream offset tracking |

---

## Local Development & Running

### Running with Docker Compose

```bash
# 1. Start infrastructure dependencies in the background
docker compose up -d

# 2. Verify containers are healthy
docker compose ps

# 3. Run the Go application locally
go run cmd/server/main.go
```

> **Note**: Access the RabbitMQ Management UI at `http://localhost:15672` (Credentials: `guest` / `guest`). Swagger UI is available at `http://localhost:8085/api/`.

### Running Standalone

```bash
# 1. Download dependencies and verify modules
go mod download
go mod verify

# 2. Run unit and architecture tests
go test -v -race ./...

# 3. Build optimized binary
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o bin/nxt-notifications cmd/server/main.go

# 4. Execute binary
./bin/nxt-notifications
```

---

## API & WebSocket Reference

### REST Endpoints

All HTTP REST endpoints require a valid JWT in the `Authorization` header:
- `Authorization: Bearer <jwt_token>`

#### 1. Health Check
```http
GET /health
```
**Response (200 OK)**:
```json
{
  "status": "ok"
}
```

#### 2. Get Paginated Notifications
```http
GET /v1/notifications?limit=20&offset=0&unread=true
```
**Response (200 OK)**:
```json
{
  "notifications": [
    {
      "id": "c3a9f1b2-8d4e-4a1b-9f3c-2a1b9d4e8f3a",
      "user_id": "USERS0001",
      "type": "user.created",
      "title": "User Created",
      "body": "A new user has been created.",
      "metadata": {},
      "status": "pending",
      "created_at": "2026-07-06T20:40:12Z"
    }
  ],
  "count": 1
}
```

#### 3. Get Unread Notification Count
```http
GET /v1/notifications/count
```
**Response (200 OK)**:
```json
{
  "unread_count": 14
}
```

#### 4. Mark Notification as Read
```http
PATCH /v1/notifications/{id}/read
```
**Response (204 No Content)**

#### 5. Mark All Notifications as Read
```http
PATCH /v1/notifications/read-all
```
**Response (204 No Content)**

#### Swagger UI
```http
GET /api/
```
Serves the Swagger UI documentation for all endpoints.

### WebSocket Protocol

To establish a real-time WebSocket connection, connect to the following endpoint with the JWT passed as a query parameter (since browser WebSocket APIs cannot set custom headers during the handshake):

```http
GET /v1/notifications/ws?token=<jwt> HTTP/1.1
Host: api.nxt.com
Upgrade: websocket
Connection: Upgrade
```

#### Server Push Message Format

**New Notification (live)**:
```json
{
  "type": "notification",
  "notification": {
    "id": "f4b2e1c0-1a2b-3c4d-5e6f-7a8b9c0d1e2f",
    "user_id": "USERS0001",
    "type": "user.created",
    "title": "User Created",
    "body": "A new user has been created.",
    "status": "pending",
    "created_at": "2026-07-06T20:47:00Z"
  }
}
```

**Catch-Up (on connect)**:
```json
{
  "type": "catch_up",
  "notifications": [
    { "id": "...", "user_id": "USERS0001", "title": "...", ... }
  ],
  "unread_count": 5
}
```

#### Heartbeat (Ping/Pong)
- **Server Ping**: The server emits a WebSocket `PING` frame every **54 seconds**.
- **Client Pong**: The client must respond with a `PONG` frame within **60 seconds**, otherwise the hub closes the connection and releases session resources.

---

## Security & Authentication

### Stateless JWT (Decode-Only)

`nxt-msa-notifications` performs **decode-only** JWT parsing — no signature verification, no public key, no HMAC secret. Token validation is delegated to the edge API Gateway (AWS Cognito / API Gateway / Kong).

1. **Token Extraction**: For REST endpoints, the `Authorization: Bearer <token>` header is used. For WebSocket connections, the token is passed as `?token=<jwt>`.
2. **Claim Extraction**: The middleware extracts `custom:iduser` (user ID), `custom:role` (role), `custom:hierarchyId` (organizational scope), and `jti` (session ID).
3. **Validation**: Tokens with missing `jti` or `custom:iduser` claims are rejected, mirroring the Java `JwtAuthenticationFilter.isInvalidRequest()` logic.
4. **User Scoping**: All database queries are scoped by `user_id`, ensuring cross-tenant isolation.

---

## Integration with Java Ecosystem (`nxt-msa-commons`)

Backend microservices in the NXT ecosystem publish notifications using the `NotificationPublisher` component provided by `nxt-msa-commons` (Java 21 / Spring Boot 4).

### Java Producer Example

```java
package com.nxt.platform.service;

import com.nxt.platform.messaging.notification.NotificationPublisher;
import com.nxt.platform.messaging.notification.NotificationMessageDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class UserManagementService {

    @Autowired
    private NotificationPublisher notificationPublisher;

    public void notifyUserCreated(String userId) {
        NotificationMessageDTO notification = NotificationMessageDTO.builder()
            .source("nxt-msa-users")
            .type("user.created")
            .userIds(List.of(userId))
            .title("User Created")
            .body("A new user has been created in the system.")
            .metadata(Map.of("userId", userId))
            .channels(List.of("websocket", "email"))
            .build();

        notificationPublisher.publish(notification);
    }
}
```

When `notificationPublisher.publish()` is called:
1. The Spring Boot starter publishes JSON to the exchange `sps-{env}-notifications-exchange-events` with routing key `#`.
2. RabbitMQ routes a copy to `sps-{env}-notifications-queue-persist` (Quorum → DB) and `sps-{env}-notifications-queue-broadcast` (Stream → Go WebSockets).

---

## Testing Strategy

The project uses a clean, layered unit test suite built exclusively with the **Go standard library** — no third-party mock generators or test frameworks. Tests are fast, deterministic, and isolated per architectural layer.

### Running the Test Suite

```bash
# Run all unit tests with race detection
go test -v -race ./...

# Run a specific package
go test -v -race ./internal/domain/...
go test -v -race ./internal/usecase/...
go test -v -race ./internal/adapter/middleware/...
go test -v -race ./internal/adapter/inbound/http/...
```

### Test Files

| File | Package | Coverage |
|------|---------|----------|
| [`internal/domain/notification_test.go`](internal/domain/notification_test.go) | `domain` | Deterministic UUID v5 generation, delivery status constants |
| [`internal/adapter/middleware/jwt_test.go`](internal/adapter/middleware/jwt_test.go) | `middleware` | JWT decode-only parsing, claim extraction, `Bearer` prefix stripping, `DisplayName` assembly |
| [`internal/usecase/dispatch_test.go`](internal/usecase/dispatch_test.go) | `usecase` | DB persistence, real-time dispatch, deterministic ID alignment, channel routing and discard logic |
| [`internal/adapter/inbound/http/handler_test.go`](internal/adapter/inbound/http/handler_test.go) | `http` | All REST routes — `401` auth failures, successful responses, unread count accuracy, `204 No Content` mark-read, pagination defaults, health check |

### Design Philosophy

- **No external mock libraries** — thin, hand-crafted in-memory mocks implement the `outbound.NotificationRepository` and `outbound.Notifier` interfaces directly inside test files.
- **Layer isolation** — each test file targets a single architectural layer; no test crosses hexagonal boundaries.
- **Race-safe** — all tests pass cleanly with `-race` enabled, verifying thread-safety in concurrent goroutine dispatch flows.
- **Deterministic ID contract** — `TestHandleRealTimeDispatch_IDMatchesDBWriteID` explicitly asserts that the ID generated by the database persistence pipeline and the real-time WebSocket dispatch pipeline are identical for the same `(EventID, UserID)` pair.

---

## Deployment & Production Considerations

### 1. Kubernetes & Load Balancer Sticky Sessions

Because WebSocket connections are persistent, stateful TCP connections, deploying across multiple Kubernetes pods requires **Sticky Sessions (Session Affinity)** at the Ingress / API Gateway layer:

```yaml
metadata:
  annotations:
    nginx.ingress.kubernetes.io/affinity: "cookie"
    nginx.ingress.kubernetes.io/session-cookie-name: "nxt_ws_affinity"
    nginx.ingress.kubernetes.io/session-cookie-max-age: "86400"
    nginx.ingress.kubernetes.io/proxy-read-timeout: "3600"
    nginx.ingress.kubernetes.io/proxy-send-timeout: "3600"
```

While RabbitMQ Streams guarantee that *all* pods receive every notification broadcast, sticky sessions ensure that reconnection handshakes and ping/pong frames route reliably.

### 2. Database Partition Maintenance

Monthly partitions are created via SQL migration. Schedule a monthly job to generate future partitions:

```sql
SELECT create_monthly_partition('notifications.notification', CURRENT_DATE + INTERVAL '1 month');
```

Partitions older than 90 days should be dropped via scheduled job to free storage.

### 3. Stream Retention & Offset Recovery

Stream retention is configured via `STREAM_MAX_AGE_SECS` (default: 24 hours). Each pod tracks its offset independently using the consumer name `{POD_NAME}:{STREAM_NAME}`. On restart, `QueryOffset` resumes from the last committed position.

### 4. Graceful Shutdown

The server implements OS signal trapping (`SIGINT`, `SIGTERM`). Upon receiving a termination signal:
1. The HTTP/WebSocket server stops accepting new connections.
2. RabbitMQ Quorum and Stream consumers unsubscribe and commit their final offsets.
3. Database connection pools (`sqlx.DB`) are drained and closed cleanly.

---

## License

This project is proprietary software owned by **Smart Payment Services**. All rights reserved.
