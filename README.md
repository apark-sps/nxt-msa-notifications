# nxt-msa-notifications

> **Microservicio de Notificaciones en Tiempo Real y Escalable** — Servicio principal de la **Plataforma NXT** responsable de notificaciones push en tiempo real vía WebSocket, recuperación automática de mensajes sin conexión, e historial persistente de notificaciones en múltiples canales (WebSocket, Email, SMS).

[![Go](https://img.shields.io/badge/Go-1.26+-00ADD8?logo=go&logoColor=white)](https://go.dev/)
[![Architecture](https://img.shields.io/badge/Architecture-Hexagonal-brightgreen.svg)](https://alistair.cockburn.us/hexagonal-architecture/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16%2B%20Partitioned-336791?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![RabbitMQ](https://img.shields.io/badge/RabbitMQ-3.13%2B%20Streams%20%2F%20Quorum-FF6600?logo=rabbitmq&logoColor=white)](https://www.rabbitmq.com/)
[![WebSockets](https://img.shields.io/badge/WebSockets-Real--Time-blue)](https://developer.mozilla.org/en-US/docs/Web/API/WebSockets_API)
[![Docker](https://img.shields.io/badge/Docker-Enabled-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)

🌐 **Idioma / Language**: 🇪🇸 Español (predeterminado) · [🇺🇸 English](README.en.md)

---



## Tabla de Contenidos

- [Descripción General](#descripción-general)
- [Stack Tecnológico](#stack-tecnológico)
- [Características Principales](#características-principales)
- [Arquitectura y Patrones de Diseño](#arquitectura-y-patrones-de-diseño)
  - [Arquitectura Hexagonal (Puertos y Adaptadores)](#arquitectura-hexagonal-puertos-y-adaptadores)
  - [Topología de Mensajería Híbrida: Quorum Queues vs. RabbitMQ Streams](#topología-de-mensajería-híbrida-quorum-queues-vs-rabbitmq-streams)
  - [Hub WebSocket en Tiempo Real y Recuperación Automática](#hub-websocket-en-tiempo-real-y-recuperación-automática)
  - [Particionamiento de Base de Datos y Replicación Lectura/Escritura](#particionamiento-de-base-de-datos-y-replicación-lecturaescritura)
  - [Abstracción de Entrega Multicanal](#abstracción-de-entrega-multicanal)
- [Modelo de Dominio y Ciclo de Vida de Notificaciones](#modelo-de-dominio-y-ciclo-de-vida-de-notificaciones)
- [Estructura del Proyecto](#estructura-del-proyecto)
- [Requisitos y Prerrequisitos](#requisitos-y-prerrequisitos)
- [Configuración y Variables de Entorno](#configuración-y-variables-de-entorno)
- [Desarrollo Local y Ejecución](#desarrollo-local-y-ejecución)
  - [Ejecución con Docker Compose](#ejecución-con-docker-compose)
  - [Ejecución Independiente](#ejecución-independiente)
- [Referencia de API y WebSocket](#referencia-de-api-y-websocket)
  - [Endpoints REST](#endpoints-rest)
  - [Protocolo WebSocket](#protocolo-websocket)
- [Seguridad y Autenticación](#seguridad-y-autenticación)
- [Integración con el Ecosistema Java (nxt-msa-commons)](#integración-con-el-ecosistema-java-nxt-msa-commons)
- [Estrategia de Pruebas](#estrategia-de-pruebas)
- [Despliegue y Consideraciones de Producción](#despliegue-y-consideraciones-de-producción)
- [Licencia](#licencia)

---

## Descripción General

`nxt-msa-notifications` es un microservicio de alta concurrencia y baja latencia construido en **Go 1.26+**, diseñado para ser el motor de distribución de notificaciones de la plataforma NXT. Proporciona entrega confiable a múltiples canales (WebSocket, Email, SMS) con recuperación automática de mensajes sin conexión e historial persistente de notificaciones.

Este servicio resuelve dos desafíos críticos en arquitecturas de microservicios modernas:
1. **Persistencia Garantizada (At-Least-Once)**: Asegurar que cada evento emitido por los servicios backend se persista de forma confiable sin sobrecargar la base de datos primaria.
2. **Entrega en Tiempo Real e Recuperación Offline Instantánea**: Entregar notificaciones a sesiones activas de browser/mobile vía WebSockets en milisegundos, mientras automáticamente envía las notificaciones perdidas en el momento exacto en que el usuario se reconecta — **eliminando completamente el polling desde el frontend**.

---

## Stack Tecnológico

- **Runtime del Lenguaje:** Go 1.26+ (utilizando `net/http` con patrones de rutas de Go 1.22+, logging estructurado con `log/slog`).
- **WebSockets en Tiempo Real:** `github.com/gorilla/websocket` (v1.5.3) para actualización de conexiones, soporte de frames de heartbeat y escritura con buffer.
- **Integración AMQP con el Broker:** `github.com/rabbitmq/amqp091-go` (v1.12.0) para pooling de conexiones y operaciones de consenso durables (Quorum Queues).
- **Integración Stream con el Broker:** `github.com/rabbitmq/rabbitmq-stream-go-client` (v1.8.1) para conexión de alto rendimiento mediante protocolo Stream y recuperación de offsets (`QueryOffset`).
- **Driver y Mapeo de Base de Datos:**
  - `github.com/lib/pq` (v1.12.3) como driver puro de Go para PostgreSQL.
  - `github.com/jmoiron/sqlx` (v1.4.0) para binding estructural ligero y gestión de pool dual (primario/réplica).
- **Infraestructura Cloud:** `github.com/aws/aws-sdk-go-v2` + `secretsmanager` para consulta segura de credenciales de base de datos.
- **Documentación de API:** Swagger/OpenAPI vía `github.com/swaggo/http-swagger/v2`.
- **JWT Solo-Decodificación:** Decodificación Base64 del payload JWT de Cognito — sin verificación de firma (delegada al API Gateway).

---

## Características Principales

- ⚡ **Push WebSocket en Tiempo Real**: Hub de comunicación WebSocket bidireccional con gestión automatizada de sesiones, monitoreo de heartbeat (ping/pong) y eliminación de conexiones muertas.
- 🔄 **Recuperación Automática Offline**: Cuando un cliente establece una conexión WebSocket, el hub consulta automáticamente PostgreSQL por notificaciones no leídas durante su ventana offline y las envía de inmediato para poblar los badges de la UI.
- 🐇 **Topología de Mensajería Híbrida con RabbitMQ**:
  - **Quorum Queues**: Colas FIFO de alta disponibilidad con consenso Raft, dedicadas a la persistencia asíncrona en base de datos (consumidores competitivos).
  - **RabbitMQ Streams**: Estructuras de log append-only que proveen fan-out de broadcast no destructivo O(1) entre múltiples pods de Kubernetes.
- 📬 **Abstracción de Entrega Multicanal**: Interfaz `Notifier` tipo plug-in que soporta entrega WebSocket hoy, con slots de adaptadores vacíos para Email y SMS — nuevos canales se agregan sin cambiar los casos de uso.
- 🗄️ **PostgreSQL Particionado y Pool Lectura/Escritura**:
  - Particionamiento mensual declarativo de tabla (`notifications.notification_y2026m07`) para escalado horizontal infinito.
  - Índice B-Tree parcial en ítems no leídos (`status != 'read'`) para consultas de badge en sub-milisegundos.
  - Pools de conexión separados que aíslan el tráfico de escritura (DB primaria) de las consultas de lectura de alto volumen (réplicas).
- 🔐 **Autenticación JWT Zero-Trust**:
  - Parsing JWT solo-decodificación compatible con los estándares de Java Spring Security (`JwtAuthenticationFilter`).
  - Claims de AWS Cognito (`custom:iduser`, `custom:role`, `custom:hierarchyId`) extraídos del payload del token.
- ☁️ **Cloud Native y Listo para AWS**:
  - Cliente AWS Secrets Manager integrado para rotación automatizada de credenciales de base de datos.
  - Modos SSL/TLS de base de datos configurables (`require` para AWS Secrets Manager / RDS, `disable` para desarrollo local).

---

## Arquitectura y Patrones de Diseño

### Arquitectura Hexagonal (Puertos y Adaptadores)

El código sigue estrictamente la **Arquitectura Hexagonal (Puertos y Adaptadores)**, garantizando total aislamiento entre la lógica de negocio y las preocupaciones de infraestructura.

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

### Topología de Mensajería Híbrida: Quorum Queues vs. RabbitMQ Streams

Una característica arquitectónica definitoria es su **topología de mensajería híbrida**. En lugar de depender de un único tipo de cola, el sistema aprovecha las Quorum Queues para persistencia en base de datos y los RabbitMQ Streams para broadcast en tiempo real.

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

#### ¿Por qué Quorum Queues para Persistencia en Base de Datos?
- **Seguridad Transaccional y Consenso**: Las Quorum Queues utilizan el algoritmo de consenso Raft entre los nodos del clúster RabbitMQ.
- **Consumidores Competitivos (Modelo Work Queue)**: Cada mensaje se entrega a **exactamente una instancia de worker**, asegurando que las operaciones SQL `INSERT` se ejecuten sin duplicación.
- **Idempotencia via `ON CONFLICT DO NOTHING`**: Si un pod falla después de escribir pero antes del ACK, el mensaje reenviado se descarta silenciosamente.
- **Manejo de Mensajes Envenenados**: Los mensajes malformados se envían a dead-letter; las fallas transitorias de DB generan reencola con reentrega automática.

#### ¿Por qué RabbitMQ Streams para Fan-Out WebSocket?

> **Nota para el equipo**: Los Streams de RabbitMQ funcionan de manera fundamentalmente diferente a las colas clásicas o quórum. Si vienes de un background de AMQP 0-9-1, este modelo puede sorprenderte.

##### Los Streams son Logs Inmutables (Append-Only)

Las colas tradicionales son **destructivas**: los mensajes desaparecen una vez que se confirman con ACK. Los Streams, en cambio, son registros persistentes e inmutables donde solo se añade información al final — exactamente como Kafka.

**Los mensajes nunca se eliminan por ACK**. Solo abandonan el Stream según las políticas de retención configuradas (tiempo máximo de vida `x-max-age` o tamaño máximo del log). Esto significa que **múltiples consumidores pueden leer los mismos mensajes de forma independiente** — un requisito fundamental para el broadcast a todos los pods.

##### El Progreso del Consumidor: Offsets en lugar de ACKs

En lugar de ACKs por mensaje, los Streams utilizan **offsets** — enteros de 64 bits que representan la posición exacta de un mensaje en el log.

Cada pod del servicio se registra con un nombre de consumidor único (`{POD_NAME}:{STREAM_NAME}`) y el broker almacena su progreso de forma independiente. Esto habilita:

- **Fan-out O(1)**: Un único Stream, N lectores independientes — sin overhead de copia en el broker.
- **Recuperación de Offsets (`QueryOffset`)**: Al reiniciar, cada pod consulta su último offset confirmado y reanuda desde `offset + 1` — sin pérdida de mensajes ni re-broadcast innecesario.
- **Replay Completo**: A diferencia de las colas clásicas donde el ACK es irreversible, en Streams es posible volver a leer desde cualquier posición reiniciando el offset — invaluable para debugging y recuperación de desastres.

##### Comparativa de Modelos

| Característica | Colas Clásicas / Quórum | RabbitMQ Streams |
| :--- | :--- | :--- |
| **Progreso del Consumidor** | ACKs por mensaje (`basic.ack`) | Offsets (punteros de posición) |
| **Vida del Mensaje** | Se elimina tras el ACK | Definida por política de retención (edad/tamaño) |
| **Consumidores Competidores** | Distribuye mensajes *entre* consumidores | Cada consumidor puede leer *todos* los mensajes |
| **Replay** | Imposible una vez hecho el ACK | Totalmente soportado (reinicia el offset) |
| **Protocolo** | AMQP 0-9-1 | Stream Protocol nativo (puerto 5552) |

##### Implementación: Auto-Commit de Offsets

En lugar de confirmar cada mensaje individualmente (lo cual generaría overhead de red significativo), el consumidor utiliza **auto-commit con umbral dual**: confirma el offset al broker cada 500 mensajes procesados *o* cada 5 segundos — lo que ocurra primero. Esto garantiza una ventana de replay máxima predecible en caso de fallo del pod.

---

### Hub WebSocket en Tiempo Real y Recuperación Automática

El Hub WebSocket (`internal/adapter/outbound/websocket/hub.go`) actúa como un registro de sesiones en memoria y despachador de eventos. Mantiene mappings thread-safe de conexiones de clientes activas indexadas por `user_id`, soportando múltiples conexiones simultáneas por usuario (multi-dispositivo/multi-tab).

#### Flujo de Recuperación Automática

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

### Particionamiento de Base de Datos y Replicación Lectura/Escritura

1. **Particionamiento Mensual Declarativo**: La tabla maestra `notifications.notification` está particionada por rango en `created_at`. Las tablas hijas (`notification_y2026m07`, `notification_y2026m08`, etc.) se crean mediante migración. Esto mantiene los árboles de índice pequeños y permite archivado instantáneo sin bloqueos `VACUUM`.
2. **Índices Parciales de No Leídos**:
   ```sql
   CREATE INDEX IF NOT EXISTS idx_notif_user_unread
       ON notifications.notification (user_id, created_at DESC)
       WHERE status != 'read';
   ```
   Esto reduce el tamaño del índice en más del 95%, permitiendo que el conteo de badges de no leídos se ejecute en sub-milisegundos.
3. **Arquitectura Dual Pool**: El repositorio configura dos instancias `sqlx.DB` distintas:
   - **Pool Primario**: Dedicado exclusivamente a operaciones `INSERT` del consumidor Quorum y sentencias UPDATE.
   - **Pool de Réplica de Lectura**: Dedicado a consultas de paginación y lecturas de catch-up WebSocket, evitando que los picos de lectura impacten el throughput de ingesta.

### Abstracción de Entrega Multicanal

La interfaz `outbound.Notifier` define un único método `Send()`, permitiendo entrega a cualquier canal sin cambios en los casos de uso:

```go
type Notifier interface {
    Channel() domain.Channel
    Send(ctx context.Context, notification *domain.Notification) error
}
```

Adaptadores actualmente implementados:
- **WebSocket Hub** — push en tiempo real a sesiones browser/mobile conectadas.
- **Email** — placeholder (directorio listo para implementación).
- **SMS** — placeholder (directorio listo para implementación).

`DispatchUseCase.HandleRealTimeDispatch()` itera sobre los canales solicitados por el evento y los despacha a cada notifier registrado en una goroutine separada, descartando silenciosamente los canales no registrados.

---

## Modelo de Dominio y Ciclo de Vida de Notificaciones

### Entidad Principal: `Notification`

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

### Contrato de Evento Entrante: `NotificationEvent`

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

### Ciclo de Vida del Estado de Notificación

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

### Generación Determinística de IDs

Los IDs de notificaciones se generan como UUID v5 a partir de `(EventID + ":" + UserID)`, asegurando que el mismo evento produzca IDs idénticos tanto en el camino de persistencia en DB como en el de despacho en tiempo real. Esto se verifica con `TestHandleRealTimeDispatch_IDMatchesDBWriteID`.

---

## Estructura del Proyecto

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

## Requisitos y Prerrequisitos

| Componente | Versión / Requerimiento | Notas |
| :--- | :--- | :--- |
| **Go (SDK)** | `1.26+` | Requerido para pattern matching de `http.ServeMux` y `log/slog` |
| **PostgreSQL** | `16.0+` | Requiere particionamiento por rango e índices B-Tree parciales |
| **RabbitMQ** | `3.13+` | **Crítico**: Debe soportar Stream Protocol (Puerto 5552) y Quorum Queues |
| **Docker & Compose** | `24.0+` | Recomendado para levantar el stack de dependencias local |
| **AWS CLI / Credenciales** | Requerido para Cloud / Prod | Necesario solo cuando `DB_SECRET_NAME` está configurado |

---

## Configuración y Variables de Entorno

La aplicación se configura íntegramente mediante variables de entorno, siguiendo la metodología **12-Factor App**. Los nombres de exchange, colas y streams se derivan automáticamente de `APP_ENV` (ej. `sps-dev-notifications-exchange-events`, `sps-qa-notifications-queue-persist`).

| Variable | Valor por Defecto | Descripción |
| :--- | :--- | :--- |
| `APP_ENV` | `dev` | Perfil de entorno (`dev`, `qa`, `sbx`) — determina el nombrado de recursos |
| `SERVER_PORT` | `8085` | Puerto de escucha del servidor HTTP / WebSocket |
| `DB_HOST` | `localhost` | Host de base de datos primaria (escritura) |
| `DB_HOST_RO` | `localhost` | Host de réplica de lectura |
| `DB_PORT` | `5432` | Puerto de base de datos |
| `DB_NAME` | `notifications` | Nombre de base de datos |
| `DB_USER` | *(vacío)* | Usuario de base de datos |
| `DB_PASSWORD` | *(vacío)* | Contraseña de base de datos |
| `DB_SSL_MODE` | `disable` | Modo SSL (`disable` para Docker local, `require` para AWS/RDS) |
| `DB_SECRET_NAME` | *(vacío)* | Nombre del secreto en AWS Secrets Manager — sobreescribe DB_HOST/DB_USER/DB_PASSWORD cuando está configurado |
| `AMQP_URI` | `amqp://guest:guest@localhost:5672/` | String de conexión AMQP |
| `AMQP_EXCHANGE` | `sps-{env}-notifications-exchange-events` | Nombre del topic exchange |
| `AMQP_PERSIST_QUEUE` | `sps-{env}-notifications-queue-persist` | Nombre de la Quorum Queue |
| `AMQP_ROUTING_KEY` | `#` | Binding de routing key para ambos consumidores |
| `STREAM_URI` | `rabbitmq-stream://guest:guest@localhost:5552/` | String de conexión del protocolo Stream |
| `STREAM_NAME` | `sps-{env}-notifications-queue-broadcast` | Nombre del RabbitMQ Stream |
| `STREAM_MAX_AGE_SECS` | `86400` | Ventana de retención del Stream (24 horas) |
| `POD_NAME` | *(hostname)* | Nombre único del consumidor para tracking de offsets del Stream |

---

## Desarrollo Local y Ejecución

### Ejecución con Docker Compose

```bash
# 1. Iniciar las dependencias de infraestructura en segundo plano
docker compose up -d

# 2. Verificar que los contenedores están saludables
docker compose ps

# 3. Ejecutar la aplicación Go localmente
go run cmd/server/main.go
```

> **Nota**: Accede a la UI de gestión de RabbitMQ en `http://localhost:15672` (Credenciales: `guest` / `guest`). La Swagger UI está disponible en `http://localhost:8085/api/`.

### Ejecución Independiente

```bash
# 1. Descargar dependencias y verificar módulos
go mod download
go mod verify

# 2. Ejecutar pruebas unitarias y de arquitectura
go test -v -race ./...

# 3. Compilar binario optimizado
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o bin/nxt-notifications cmd/server/main.go

# 4. Ejecutar binario
./bin/nxt-notifications
```

---

## Referencia de API y WebSocket

### Endpoints REST

Todos los endpoints HTTP REST requieren un JWT válido en el header `Authorization`:
- `Authorization: Bearer <jwt_token>`

#### 1. Health Check (Verificación de Salud)
```http
GET /health
```
**Response (200 OK)**:
```json
{
  "status": "ok"
}
```

#### 2. Obtener Notificaciones Paginadas
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

#### 3. Obtener Conteo de Notificaciones No Leídas
```http
GET /v1/notifications/count
```
**Response (200 OK)**:
```json
{
  "unread_count": 14
}
```

#### 4. Marcar Notificación como Leída
```http
PATCH /v1/notifications/{id}/read
```
**Response (204 No Content)**

#### 5. Marcar Todas las Notificaciones como Leídas
```http
PATCH /v1/notifications/read-all
```
**Response (204 No Content)**

#### Swagger UI (Documentación Interactiva)
```http
GET /api/
```
Serves the Swagger UI documentation for all endpoints.

### Protocolo WebSocket

Para establecer una conexión WebSocket en tiempo real, conectarse al siguiente endpoint con el JWT como query parameter (ya que las APIs de WebSocket del browser no pueden enviar headers personalizados durante el handshake):

```http
GET /v1/notifications/ws?token=<jwt> HTTP/1.1
Host: api.nxt.com
Upgrade: websocket
Connection: Upgrade
```

#### Formato de Mensajes Push del Servidor

**Nueva Notificación (en vivo)**:
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

**Catch-Up (al conectar)**:
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
- **Ping del Servidor**: El servidor emite un frame WebSocket `PING` cada **54 segundos**.
- **Pong del Cliente**: El cliente debe responder con un frame `PONG` dentro de los **60 segundos**, de lo contrario el hub cierra la conexión y libera los recursos de sesión.

---

## Seguridad y Autenticación

### JWT Sin Estado (Solo Decodificación)

`nxt-msa-notifications` realiza parsing JWT de **solo decodificación** — sin verificación de firma, sin clave pública, sin secreto HMAC. La validación del token está delegada al API Gateway de borde (AWS Cognito / API Gateway / Kong).

1. **Extracción del Token**: Para endpoints REST se usa el header `Authorization: Bearer <token>`. Para conexiones WebSocket el token se pasa como `?token=<jwt>`.
2. **Extracción de Claims**: El middleware extrae `custom:iduser` (ID de usuario), `custom:role` (rol), `custom:hierarchyId` (scope organizacional) y `jti` (ID de sesión).
3. **Validación**: Los tokens con claims `jti` o `custom:iduser` faltantes son rechazados, reflejando la lógica Java de `JwtAuthenticationFilter.isInvalidRequest()`.
4. **Scope por Usuario**: Todas las consultas de base de datos están limitadas por `user_id`, asegurando aislamiento entre tenants.

---

## Integración con el Ecosistema Java (`nxt-msa-commons`)

Los microservicios backend del ecosistema NXT publican notificaciones usando el componente `NotificationPublisher` provisto por `nxt-msa-commons` (Java 21 / Spring Boot 4).

### Ejemplo de Productor Java

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

Cuando se llama a `notificationPublisher.publish()`:
1. El starter de Spring Boot publica JSON al exchange `sps-{env}-notifications-exchange-events` con routing key `#`.
2. RabbitMQ enruta una copia a `sps-{env}-notifications-queue-persist` (Quorum → DB) y `sps-{env}-notifications-queue-broadcast` (Stream → Go WebSockets).

---

## Estrategia de Pruebas

El proyecto utiliza una suite de pruebas unitarias limpia y en capas, construida exclusivamente con la **librería estándar de Go** — sin generadores de mocks de terceros ni frameworks de prueba. Las pruebas son rápidas, determinísticas y aisladas por capa arquitectónica.

### Ejecutar la Suite de Pruebas

```bash
# Run all unit tests with race detection
go test -v -race ./...

# Run a specific package
go test -v -race ./internal/domain/...
go test -v -race ./internal/usecase/...
go test -v -race ./internal/adapter/middleware/...
go test -v -race ./internal/adapter/inbound/http/...
```

### Archivos de Prueba

| Archivo | Paquete | Cobertura |
|---------|---------|----------|
| [`internal/domain/notification_test.go`](internal/domain/notification_test.go) | `domain` | Generación determinística de UUID v5, constantes de estado de entrega |
| [`internal/adapter/middleware/jwt_test.go`](internal/adapter/middleware/jwt_test.go) | `middleware` | Parsing JWT solo-decodificación, extracción de claims, stripping del prefijo `Bearer`, ensamblado de `DisplayName` |
| [`internal/usecase/dispatch_test.go`](internal/usecase/dispatch_test.go) | `usecase` | Persistencia en DB, despacho en tiempo real, alineación de IDs determinísticos, routing de canales y lógica de descarte |
| [`internal/adapter/inbound/http/handler_test.go`](internal/adapter/inbound/http/handler_test.go) | `http` | Todas las rutas REST — fallos `401` de autenticación, respuestas exitosas, precisión del conteo de no leídos, mark-read `204 No Content`, defaults de paginación, health check |

### Filosofía de Diseño

- **Sin librerías externas de mocks** — mocks en memoria delgados y artesanales implementan las interfaces `outbound.NotificationRepository` y `outbound.Notifier` directamente dentro de los archivos de prueba.
- **Aislamiento por capas** — cada archivo de prueba apunta a una única capa arquitectónica; ninguna prueba cruza los límites hexagonales.
- **Race-safe** — todas las pruebas pasan limpiamente con `-race` habilitado, verificando la thread-safety en flujos de despacho de goroutines concurrentes.
- **Contrato de ID determinístico** — `TestHandleRealTimeDispatch_IDMatchesDBWriteID` afirma explícitamente que el ID generado por el pipeline de persistencia en DB y el pipeline de despacho WebSocket en tiempo real son idénticos para el mismo par `(EventID, UserID)`.

---

## Despliegue y Consideraciones de Producción

### 1. Kubernetes y Sticky Sessions en el Load Balancer

Dado que las conexiones WebSocket son conexiones TCP persistentes y con estado, el despliegue en múltiples pods de Kubernetes requiere **Sticky Sessions (Session Affinity)** en la capa de Ingress / API Gateway:

```yaml
metadata:
  annotations:
    nginx.ingress.kubernetes.io/affinity: "cookie"
    nginx.ingress.kubernetes.io/session-cookie-name: "nxt_ws_affinity"
    nginx.ingress.kubernetes.io/session-cookie-max-age: "86400"
    nginx.ingress.kubernetes.io/proxy-read-timeout: "3600"
    nginx.ingress.kubernetes.io/proxy-send-timeout: "3600"
```

Mientras los RabbitMQ Streams garantizan que *todos* los pods reciben cada broadcast de notificación, las sticky sessions aseguran que los handshakes de reconexión y los frames ping/pong enruten de forma confiable.

### 2. Mantenimiento de Particiones de Base de Datos

Las particiones mensuales se crean mediante migración SQL. Programar un job mensual para generar particiones futuras:

```sql
SELECT create_monthly_partition('notifications.notification', CURRENT_DATE + INTERVAL '1 month');
```

Las particiones con más de 90 días deben eliminarse mediante job programado para liberar almacenamiento.

### 3. Retención del Stream y Recuperación de Offsets

La retención del Stream se configura vía `STREAM_MAX_AGE_SECS` (por defecto: 24 horas). Cada pod rastrea su offset de forma independiente usando el nombre de consumidor `{POD_NAME}:{STREAM_NAME}`. Al reiniciar, `QueryOffset` reanuda desde la última posición confirmada.

### 4. Apagado Graceful

El servidor implementa captura de señales del SO (`SIGINT`, `SIGTERM`). Al recibir una señal de terminación:
1. El servidor HTTP/WebSocket deja de aceptar nuevas conexiones.
2. Los consumidores Quorum y Stream de RabbitMQ se desuscriben y confirman sus offsets finales.
3. Los pools de conexión de base de datos (`sqlx.DB`) se drenan y cierran limpiamente.

---

## Licencia

Este proyecto es software propietario propiedad de **Smart Payment Services**. Todos los derechos reservados.
