// Package postgres is the implementation of the postgres data store.
package postgres

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

// Postgres is the implementation of the data store.
type Postgres struct {
	// Locking and unlocking need to use the same connection.
	conn     *sql.Conn
	isLocked bool

	DatabaseName    string
	SchemaName      string
	EventStoreTable string
}

// WithInstance creates a postgres data store with an existing db connection.
func WithInstance(ctx context.Context, db *sql.DB) (*Postgres, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}

	p := Postgres{conn: conn}

	if err := conn.QueryRowContext(ctx, `SELECT CURRENT_DATABASE()`).Scan(&p.DatabaseName); err != nil {
		return nil, err
	}

	if len(p.DatabaseName) == 0 {
		return nil, ErrNoDatabaseName
	}

	if err := conn.QueryRowContext(ctx, `SELECT CURRENT_SCHEMA()`).Scan(&p.SchemaName); err != nil {
		return nil, err
	}

	if len(p.SchemaName) == 0 {
		return nil, ErrNoSchema
	}

	if len(p.EventStoreTable) == 0 {
		p.EventStoreTable = DefaultEventStoreTable
	}

	if err := p.ensureTable(ctx); err != nil {
		return nil, err
	}

	return &p, nil
}

// Close closes the db connection.
func (p *Postgres) Close() error {
	if err := p.conn.Close(); err != nil {
		return fmt.Errorf("failed to close connection: %w", err)
	}

	return nil
}

// GetEvents retrieves all the relevant events.
func (p *Postgres) GetEvents(ctx context.Context, batchSize int32) ([]*outboxer.OutboxMessage, error) {
	var events []*outboxer.OutboxMessage

	// nolint
	rows, err := p.conn.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s WHERE dispatched = false LIMIT %d", p.EventStoreTable, batchSize))
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

// Add adds the message to the data store.
func (p *Postgres) Add(ctx context.Context, evt *outboxer.OutboxMessage) error {
	tx, err := p.conn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return fmt.Errorf("transaction start failed: %w", err)
	}

	// nolint
	query := fmt.Sprintf(`INSERT INTO %s (payload, options, headers) VALUES ($1, $2, $3)`, p.EventStoreTable)
	if _, err := tx.ExecContext(ctx, query, evt.Payload, evt.Options, evt.Headers); err != nil {
		if err := tx.Rollback(); err != nil {
			return err
		}

		return fmt.Errorf("failed to insert message into the data store: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("transaction commit failed: %w", err)
	}

	return nil
}

// AddWithinTx creates a transaction and then tries to execute anything within it.
func (p *Postgres) AddWithinTx(ctx context.Context, evt *outboxer.OutboxMessage, fn func(outboxer.ExecerContext) error) error {
	tx, err := p.conn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return fmt.Errorf("transaction start failed: %w", err)
	}

	if err := fn(tx); err != nil {
		return err
	}

	// nolint
	query := fmt.Sprintf(`INSERT INTO %s (payload, options, headers) VALUES ($1, $2, $3)`, p.EventStoreTable)
	if _, err := tx.ExecContext(ctx, query, evt.Payload, evt.Options, evt.Headers); err != nil {
		if err := tx.Rollback(); err != nil {
			return err
		}

		return fmt.Errorf("failed to insert message into the data store: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("transaction commit failed: %w", err)
	}

	return nil
}

// SetAsDispatched sets one message as dispatched.
func (p *Postgres) SetAsDispatched(ctx context.Context, id int64) error {
	query := fmt.Sprintf(`
update %s
set
    dispatched = true,
    dispatched_at = now()
where id = $1;
`, p.EventStoreTable)
	if _, err := p.conn.ExecContext(ctx, query, id); err != nil {
		return fmt.Errorf("failed to set message as dispatched: %w", err)
	}

	return nil
}

// Lock implements explicit locking.
// https://www.postgresql.org/docs/9.6/static/explicit-locking.html#ADVISORY-LOCKS
func (p *Postgres) lock(ctx context.Context) error {
	if p.isLocked {
		return ErrLocked
	}

	aid, err := lock.Generate(p.DatabaseName, p.SchemaName)
	if err != nil {
		return err
	}

	// This will either obtain the lock immediately and return true,
	// or return false if the lock cannot be acquired immediately.
	query := `SELECT pg_advisory_lock($1)`
	if _, err := p.conn.ExecContext(ctx, query, aid); err != nil {
		return fmt.Errorf("try lock failed: %w", err)
	}

	p.isLocked = true

	return nil
}

// Unlock is the implementation of the unlock for explicit locking.
func (p *Postgres) unlock(ctx context.Context) error {
	if !p.isLocked {
		return nil
	}

	aid, err := lock.Generate(p.DatabaseName, p.SchemaName)
	if err != nil {
		return err
	}

	query := `SELECT pg_advisory_unlock($1)`
	if _, err := p.conn.ExecContext(ctx, query, aid); err != nil {
		return err
	}

	p.isLocked = false

	return nil
}

func (p *Postgres) ensureTable(ctx context.Context) (err error) {
	if err = p.lock(ctx); err != nil {
		return err
	}

	defer func() {
		if e := p.unlock(ctx); e != nil {
			if err == nil {
				err = e
			} else {
				err = fmt.Errorf("failed to unlock table: %w", err)
			}
		}
	}()

	query := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %[1]s (
	id SERIAL not null primary key, 
	dispatched boolean not null default false, 
	dispatched_at timestamp,
	payload bytea not null,
	options jsonb,
	headers jsonb
);

CREATE INDEX IF NOT EXISTS "index_dispatchedAt" ON %[1]s using btree (dispatched_at asc nulls last);
CREATE INDEX IF NOT EXISTS "index_dispatched" ON %[1]s using btree (dispatched asc nulls last);
`, p.EventStoreTable)

	if _, err = p.conn.ExecContext(ctx, query); err != nil {
		return err
	}

	return nil
}
