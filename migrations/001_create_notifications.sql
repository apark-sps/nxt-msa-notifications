-- migrations/001_create_notifications.sql
-- Partitioned notification table with monthly range partitions.
-- Partitions older than 90 days are dropped via a scheduled job (cron/Lambda),
-- not CASCADE DELETE, to avoid table-lock overhead.

CREATE SCHEMA IF NOT EXISTS notifications;

CREATE TABLE IF NOT EXISTS notifications.notification (
    id            UUID          NOT NULL,
    user_id       VARCHAR(10)   NOT NULL,              -- "USERS####" — main cross-service identifier
    hierarchy_id  INTEGER,                             -- Organizational scope (nullable)
    type          VARCHAR(50)   NOT NULL,              -- e.g. "user.created", "hierarchy.updated"
    title         VARCHAR(255)  NOT NULL,
    body          TEXT          NOT NULL,
    metadata      JSONB         NOT NULL DEFAULT '{}',
    channels      TEXT[]        NOT NULL,              -- e.g. ARRAY['websocket', 'email']
    status        VARCHAR(20)   NOT NULL DEFAULT 'pending'
                      CHECK (status IN ('pending', 'delivered', 'read', 'failed')),
    created_at    TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    read_at       TIMESTAMPTZ,
    PRIMARY KEY (id, created_at)                       -- Partition key must be part of PK
) PARTITION BY RANGE (created_at);

-- Monthly partitions — add new ones via the partition management job
CREATE TABLE IF NOT EXISTS notifications.notification_y2026m07
    PARTITION OF notifications.notification
    FOR VALUES FROM ('2026-07-01 00:00:00+00') TO ('2026-08-01 00:00:00+00');

CREATE TABLE IF NOT EXISTS notifications.notification_y2026m08
    PARTITION OF notifications.notification
    FOR VALUES FROM ('2026-08-01 00:00:00+00') TO ('2026-09-01 00:00:00+00');

CREATE TABLE IF NOT EXISTS notifications.notification_y2026m09
    PARTITION OF notifications.notification
    FOR VALUES FROM ('2026-09-01 00:00:00+00') TO ('2026-10-01 00:00:00+00');

-- Partial index: covers the hot-path queries (unread count + catch-up fetch).
-- Only indexes rows where status is not 'read' — keeps the index small and tight.
CREATE INDEX IF NOT EXISTS idx_notif_user_unread
    ON notifications.notification (user_id, created_at DESC)
    WHERE status != 'read';

-- Full-scan index for paginated history (includes read notifications)
CREATE INDEX IF NOT EXISTS idx_notif_user_created
    ON notifications.notification (user_id, created_at DESC);

-- Index for the MarkAsRead UPDATE path
CREATE INDEX IF NOT EXISTS idx_notif_id_user
    ON notifications.notification (id, user_id);
