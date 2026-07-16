package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"nxt-msa-notifications/internal/domain"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

// Repository implements outbound.NotificationRepository backed by PostgreSQL.
// It uses a primary (read-write) and a read-only replica connection pool,
// mirroring the GenericDataSourceConfig dual-DataSource pattern from nxt-msa-commons.
type Repository struct {
	primary  *sqlx.DB // Write operations — SQL INSERT, UPDATE
	readOnly *sqlx.DB // Read operations — SELECT queries, COUNT
}

// NewRepository creates a Repository with separate primary and read-only connection pools.
func NewRepository(primaryDSN, readOnlyDSN string) (*Repository, error) {
	primary, err := sqlx.Connect("postgres", primaryDSN)
	if err != nil {
		return nil, fmt.Errorf("postgres: failed to connect to primary: %w", err)
	}
	primary.SetMaxOpenConns(25)
	primary.SetMaxIdleConns(5)
	primary.SetConnMaxLifetime(5 * time.Minute)

	readOnly, err := sqlx.Connect("postgres", readOnlyDSN)
	if err != nil {
		primary.Close()
		return nil, fmt.Errorf("postgres: failed to connect to read-replica: %w", err)
	}
	readOnly.SetMaxOpenConns(25)
	readOnly.SetMaxIdleConns(5)
	readOnly.SetConnMaxLifetime(5 * time.Minute)

	repo := &Repository{primary: primary, readOnly: readOnly}
	if err := repo.initializeSchema(context.Background()); err != nil {
		primary.Close()
		readOnly.Close()
		return nil, fmt.Errorf("postgres: failed to initialize schema: %w", err)
	}

	return repo, nil
}

// initializeSchema automatically provisions the notifications schema, notification table,
// indices, and the current month partition on startup if they do not exist.
func (r *Repository) initializeSchema(ctx context.Context) error {
	// 1. Create schema
	_, err := r.primary.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS notifications;")
	if err != nil {
		return fmt.Errorf("create schema: %w", err)
	}

	// 2. Create partition parent table
	_, err = r.primary.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS notifications.notification (
			id            UUID          NOT NULL,
			user_id       VARCHAR(10)   NOT NULL,
			hierarchy_id  INTEGER,
			type          VARCHAR(50)   NOT NULL,
			title         VARCHAR(255)  NOT NULL,
			body          TEXT          NOT NULL,
			metadata      JSONB         NOT NULL DEFAULT '{}',
			channels      TEXT[]        NOT NULL,
			status        VARCHAR(20)   NOT NULL DEFAULT 'pending'
							CHECK (status IN ('pending', 'delivered', 'read', 'failed')),
			created_at    TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
			read_at       TIMESTAMPTZ,
			PRIMARY KEY (id, created_at)
		) PARTITION BY RANGE (created_at);
	`)
	if err != nil {
		return fmt.Errorf("create parent table: %w", err)
	}

	// 3. Create indices
	indices := []string{
		`CREATE INDEX IF NOT EXISTS idx_notif_user_unread
		 ON notifications.notification (user_id, created_at DESC)
		 WHERE status != 'read';`,
		`CREATE INDEX IF NOT EXISTS idx_notif_user_created
		 ON notifications.notification (user_id, created_at DESC);`,
		`CREATE INDEX IF NOT EXISTS idx_notif_id_user
		 ON notifications.notification (id, user_id);`,
	}
	for i, query := range indices {
		if _, err := r.primary.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("create index %d: %w", i, err)
		}
	}

	// 4. Create dynamic partition for current month
	now := time.Now().UTC()
	currentYear, currentMonth, _ := now.Date()

	startOfCurrentMonth := time.Date(currentYear, currentMonth, 1, 0, 0, 0, 0, time.UTC)
	startOfNextMonth := startOfCurrentMonth.AddDate(0, 1, 0)

	partitionName := fmt.Sprintf("notifications.notification_y%04dm%02d", currentYear, int(currentMonth))

	createPartitionQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s
		PARTITION OF notifications.notification
		FOR VALUES FROM ('%%s') TO ('%%s');
	`, partitionName)

	formattedQuery := fmt.Sprintf(createPartitionQuery, startOfCurrentMonth.Format("2006-01-02 15:04:05Z"), startOfNextMonth.Format("2006-01-02 15:04:05Z"))

	if _, err := r.primary.ExecContext(ctx, formattedQuery); err != nil {
		return fmt.Errorf("create current month partition: %w", err)
	}

	return nil
}

// Save persists a new notification to the partitioned table.
// Uses INSERT ... ON CONFLICT DO NOTHING as an idempotency guard against
// quorum queue redelivery — if the pod crashes after write but before ACK,
// the redelivered message will be silently skipped.
func (r *Repository) Save(ctx context.Context, n *domain.Notification) error {
	metaJSON, err := json.Marshal(n.Metadata)
	if err != nil {
		return fmt.Errorf("save: marshal metadata: %w", err)
	}

	channels := make([]string, len(n.Channels))
	for i, ch := range n.Channels {
		channels[i] = string(ch)
	}

	_, err = r.primary.ExecContext(
		ctx, `
		INSERT INTO notifications.notification
			(id, user_id, hierarchy_id, type, title, body, metadata, channels, status, created_at)
		VALUES
			($1, $2, $3, $4, $5, $6, $7, $8::text[], $9, $10)
		ON CONFLICT (id, created_at) DO NOTHING`,
		n.ID, n.UserID, n.HierarchyID, n.Type, n.Title, n.Body,
		metaJSON, channels, string(n.Status), n.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("save: insert: %w", err)
	}
	return nil
}

// MarkAsRead sets a single notification's status to 'read' and records the timestamp.
// The user_id guard ensures users cannot mark other users' notifications.
func (r *Repository) MarkAsRead(ctx context.Context, notificationID, userID string) error {
	now := time.Now().UTC()
	result, err := r.primary.ExecContext(
		ctx, `
		UPDATE notifications.notification
		SET status = 'read', read_at = $1
		WHERE id = $2 AND user_id = $3 AND status != 'read'`,
		now, notificationID, userID,
	)
	if err != nil {
		return fmt.Errorf("markAsRead: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("markAsRead: notification %s not found or already read for user %s", notificationID, userID)
	}
	return nil
}

// MarkAllAsRead marks all pending/delivered notifications as read for a user.
func (r *Repository) MarkAllAsRead(ctx context.Context, userID string) error {
	now := time.Now().UTC()
	_, err := r.primary.ExecContext(
		ctx, `
		UPDATE notifications.notification
		SET status = 'read', read_at = $1
		WHERE user_id = $2 AND status != 'read'`,
		now, userID,
	)
	if err != nil {
		return fmt.Errorf("markAllAsRead: %w", err)
	}
	return nil
}

// FindByUser retrieves paginated notifications from the read-replica.
// The partial index idx_notif_user_unread ensures efficient unread-only queries.
func (r *Repository) FindByUser(ctx context.Context, userID string, unreadOnly bool, limit, offset int) ([]domain.Notification, error) {
	query := `
		SELECT id, user_id, hierarchy_id, type, title, body, status, created_at, read_at
		FROM notifications.notification
		WHERE user_id = $1`

	if unreadOnly {
		query += ` AND status != 'read'`
	}
	query += ` ORDER BY created_at DESC LIMIT $2 OFFSET $3`

	rows, err := r.readOnly.QueryxContext(ctx, query, userID, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("findByUser: %w", err)
	}
	defer rows.Close()

	var results []domain.Notification
	for rows.Next() {
		var n domain.Notification
		var hierarchyID sql.NullInt64
		var readAt sql.NullTime
		if err := rows.Scan(
			&n.ID, &n.UserID, &hierarchyID,
			&n.Type, &n.Title, &n.Body,
			&n.Status, &n.CreatedAt, &readAt,
		); err != nil {
			return nil, fmt.Errorf("findByUser: scan: %w", err)
		}
		if hierarchyID.Valid {
			v := int(hierarchyID.Int64)
			n.HierarchyID = &v
		}
		if readAt.Valid {
			n.ReadAt = &readAt.Time
		}
		results = append(results, n)
	}
	return results, rows.Err()
}

// CountUnread uses the read-replica and the partial index for O(log n) performance.
func (r *Repository) CountUnread(ctx context.Context, userID string) (int, error) {
	var count int
	err := r.readOnly.QueryRowContext(
		ctx, `
		SELECT COUNT(*) FROM notifications.notification
		WHERE user_id = $1 AND status != 'read'`,
		userID,
	).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("countUnread: %w", err)
	}
	return count, nil
}

func (r *Repository) CountAll(ctx context.Context, userID string, unreadOnly bool) (int, error) {
	query := `
		SELECT COUNT(*) FROM notifications.notification
		WHERE user_id = $1`

	if unreadOnly {
		query += ` AND status != 'read'`
	}

	var count int
	err := r.readOnly.QueryRowContext(ctx, query, userID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("countAll: %w", err)
	}
	return count, nil
}

// Close releases both connection pools.
func (r *Repository) Close() {
	r.primary.Close()
	r.readOnly.Close()
}
