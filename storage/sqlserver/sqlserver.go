package sqlserver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/italolelis/outboxer"
	"github.com/italolelis/outboxer/lock"
)

const (
	// DefaultEventStoreTable is the default table name.
	DefaultEventStoreTable = "event_store"
)

var (
	// ErrLocked is used when we can't acquire an explicit lock.
	ErrLocked = errors.New("can't acquire lock")

	// ErrNoDatabaseName is used when the database name is blank.
	ErrNoDatabaseName = errors.New("no database name")

	// ErrNoSchema is used when the schema name is blank.
	ErrNoSchema = errors.New("no schema")
)

// SQLServer implementation of the data store.
type SQLServer struct {
	conn     *sql.Conn
	isLocked bool

	SchemaName      string
	DatabaseName    string
	EventStoreTable string
}

// WithInstance creates a SQLServer data store with an existing db connection.
func WithInstance(ctx context.Context, db *sql.DB) (*SQLServer, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}

	s := SQLServer{conn: conn}

	if err := conn.QueryRowContext(ctx, `SELECT DB_NAME()`).Scan(&s.DatabaseName); err != nil {
		return nil, err
	}

	if len(s.DatabaseName) == 0 {
		return nil, ErrNoDatabaseName
	}

	if err := conn.QueryRowContext(ctx, `SELECT SCHEMA_NAME()`).Scan(&s.SchemaName); err != nil {
		return nil, err
	}

	if len(s.SchemaName) == 0 {
		return nil, ErrNoSchema
	}

	if len(s.EventStoreTable) == 0 {
		s.EventStoreTable = DefaultEventStoreTable
	}

	if err := s.ensureTable(ctx); err != nil {
		return nil, err
	}

	return &s, nil
}

// Close closes the db connection.
func (s *SQLServer) Close() error {
	if err := s.conn.Close(); err != nil {
		return fmt.Errorf("failed to close connection: %w", err)
	}

	return nil
}

// Add the message to the data store.
func (s *SQLServer) Add(ctx context.Context, evt *outboxer.OutboxMessage) error {
	// nolint
	query := fmt.Sprintf(`INSERT INTO [%s].[%s] (payload, options, headers) VALUES (@p1, @p2, @p3)`, s.SchemaName, s.EventStoreTable)
	if _, err := s.conn.ExecContext(ctx, query, evt.Payload, checkBinaryParam(evt.Options), checkBinaryParam(evt.Headers)); err != nil {
		return fmt.Errorf("failed to insert message into the data store: %w", err)
	}

	return nil
}

// AddWithinTx creates a transaction and then tries to execute anything within it.
func (s *SQLServer) AddWithinTx(ctx context.Context, evt *outboxer.OutboxMessage, fn func(outboxer.ExecerContext) error) error {
	tx, err := s.conn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return fmt.Errorf("transaction start failed: %w", err)
	}

	if err := fn(tx); err != nil {
		return err
	}

	// nolint
	query := fmt.Sprintf(`INSERT INTO [%s].[%s] (payload, options, headers) VALUES (@p1, @p2, @p3)`, s.SchemaName, s.EventStoreTable)

	if _, err := tx.ExecContext(ctx, query, evt.Payload, checkBinaryParam(evt.Options), checkBinaryParam(evt.Headers)); err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to insert message into the data store: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("transaction commit failed: %w", err)
	}

	return nil
}

// checkBinaryParam is a fix for issue with mssql driver converting nil value in varbinary to nvarchar.
// https://github.com/denisenkom/go-mssqldb/issues/530
func checkBinaryParam(p outboxer.DynamicValues) outboxer.DynamicValues {
	if p == nil {
		return map[string]interface{}{}
	}

	return p
}

// SetAsDispatched sets one message as dispatched.
func (s *SQLServer) SetAsDispatched(ctx context.Context, id int64) error {
	// nolint
	query := fmt.Sprintf(`
UPDATE [%s].[%s]
SET
    dispatched = 1,
    dispatched_at = GETDATE()
WHERE id = @p1;
`, s.SchemaName, s.EventStoreTable)
	if _, err := s.conn.ExecContext(ctx, query, id); err != nil {
		return fmt.Errorf("failed to set message as dispatched: %w", err)
	}

	return nil
}

// GetEvents retrieves all the relevant events.
func (s *SQLServer) GetEvents(ctx context.Context, batchSize int32) ([]*outboxer.OutboxMessage, error) {
	var events []*outboxer.OutboxMessage
	// nolint
	rows, err := s.conn.QueryContext(ctx, fmt.Sprintf("SELECT TOP %d * FROM [%s].[%s] WHERE dispatched = 0",
		batchSize, s.SchemaName, s.EventStoreTable))
	if err != nil {
		return events, fmt.Errorf("failed to get messages from the store: %w", err)
	}

	for rows.Next() {
		var e outboxer.OutboxMessage

		err = rows.Scan(&e.ID, &e.Dispatched, &e.DispatchedAt, &e.Payload, &e.Options, &e.Headers)
		if err != nil {
			return events, fmt.Errorf("failed to scan message: %w", err)
		}

		events = append(events, &e)
	}

	return events, nil
}

// Lock implements explicit locking.
func (s *SQLServer) lock(ctx context.Context) error {
	if s.isLocked {
		return ErrLocked
	}

	aid, err := lock.Generate(s.DatabaseName, s.SchemaName)
	if err != nil {
		return err
	}

	query := `EXEC sp_getapplock 
	@Resource = @p1,
	@LockOwner='Session',
	@LockMode = 'Exclusive';`

	if _, err := s.conn.ExecContext(ctx, query, aid); err != nil {
		return fmt.Errorf("try lock failed: %w", err)
	}

	s.isLocked = true

	return nil
}

// Unlock is the implementation of the unlock for explicit locking.
func (s *SQLServer) unlock(ctx context.Context) error {
	if !s.isLocked {
		return nil
	}

	aid, err := lock.Generate(s.DatabaseName, s.SchemaName)
	if err != nil {
		return err
	}

	query := `EXEC sp_releaseapplock  
	@Resource = @p1, 
	@LockOwner='Session';`

	if _, err := s.conn.ExecContext(ctx, query, aid); err != nil {
		return err
	}

	s.isLocked = false

	return nil
}

func (s *SQLServer) ensureTable(ctx context.Context) (err error) {
	if err = s.lock(ctx); err != nil {
		return err
	}

	defer func() {
		if e := s.unlock(ctx); e != nil {
			if err == nil {
				err = e
			} else {
				err = fmt.Errorf("failed to unlock table: %w", err)
			}
		}
	}()
	// nolint
	query := fmt.Sprintf(
		`IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='%[2]s' and xtype='U') CREATE TABLE %[1]s.%[2]s (
	id int IDENTITY(1,1) NOT NULL PRIMARY KEY,
	dispatched BIT NOT NULL DEFAULT 0,
	dispatched_at DATETIME,
	payload VARBINARY(MAX)  NOT NULL,
	options VARBINARY(MAX),
	headers VARBINARY(MAX)
);
`, s.SchemaName, s.EventStoreTable)

	if _, err = s.conn.ExecContext(ctx, query); err != nil {
		return err
	}

	return nil
}
