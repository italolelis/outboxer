// Package mysql is the implementation of the mysql data store.
package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/italolelis/outboxer/lock"

	"github.com/italolelis/outboxer"
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
)

// MySQL is the implementation of the data store.
type MySQL struct {
	// Locking and unlocking need to use the same connection.
	conn     *sql.Conn
	isLocked bool

	DatabaseName    string
	EventStoreTable string
}

// WithInstance creates a mysql data store with an existing db connection.
func WithInstance(ctx context.Context, db *sql.DB) (*MySQL, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to data storage: %w", err)
	}

	p := MySQL{conn: conn}

	if err := conn.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("could not ping to MySQL database: %w", err)
	}

	var databaseName sql.NullString
	if err := db.QueryRow(`SELECT DATABASE()`).Scan(&databaseName); err != nil {
		return nil, err
	}

	if len(databaseName.String) == 0 {
		return nil, ErrNoDatabaseName
	}

	p.DatabaseName = databaseName.String

	if len(p.EventStoreTable) == 0 {
		p.EventStoreTable = DefaultEventStoreTable
	}

	if err := p.ensureTable(ctx); err != nil {
		return nil, err
	}

	return &p, nil
}

// Close closes the db connection.
func (p *MySQL) Close() error {
	if err := p.conn.Close(); err != nil {
		return fmt.Errorf("failed to close connection: %w", err)
	}

	return nil
}

// GetEvents retrieves all the relevant events.
func (p *MySQL) GetEvents(ctx context.Context, batchSize int32) ([]*outboxer.OutboxMessage, error) {
	var events []*outboxer.OutboxMessage

	// nolint
	rows, err := p.conn.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s WHERE dispatched = false LIMIT %d", p.EventStoreTable, batchSize))
	if err != nil {
		return events, fmt.Errorf("failed to get messages from store: %w", err)
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
func (p *MySQL) Add(ctx context.Context, evt *outboxer.OutboxMessage) error {
	// nolint
	query := fmt.Sprintf(`INSERT INTO %s (payload, options, headers) VALUES (?, ?, ?)`, p.EventStoreTable)
	if _, err := p.conn.ExecContext(ctx, query, evt.Payload, evt.Options, evt.Headers); err != nil {
		return fmt.Errorf("failed to insert message into the data store: %w", err)
	}

	return nil
}

// AddWithinTx creates a transaction and then tries to execute anything within it.
func (p *MySQL) AddWithinTx(ctx context.Context, evt *outboxer.OutboxMessage, fn func(outboxer.ExecerContext) error) error {
	tx, err := p.conn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return fmt.Errorf("transaction start failed: %w", err)
	}

	if err := fn(tx); err != nil {
		return err
	}

	// nolint
	query := fmt.Sprintf(`INSERT INTO %s (payload, options, headers) VALUES (?, ?, ?)`, p.EventStoreTable)
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
func (p *MySQL) SetAsDispatched(ctx context.Context, id int64) error {
	query := fmt.Sprintf(`
update %s
set
    dispatched = true,
    dispatched_at = now()
where id = ?;
`, p.EventStoreTable)
	if _, err := p.conn.ExecContext(ctx, query, id); err != nil {
		return fmt.Errorf("failed to set message as dispatched: %w", err)
	}

	return nil
}

// Lock implements explicit locking.
func (p *MySQL) lock(ctx context.Context) error {
	if p.isLocked {
		return ErrLocked
	}

	aid, err := lock.Generate(p.DatabaseName, p.EventStoreTable)
	if err != nil {
		return err
	}

	query := "SELECT GET_LOCK(?, 10)"

	var success bool
	if err := p.conn.QueryRowContext(ctx, query, aid).Scan(&success); err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}

	if success {
		p.isLocked = true
		return nil
	}

	return ErrLocked
}

// Unlock is the implementation of the unlock for explicit locking.
func (p *MySQL) unlock(ctx context.Context) error {
	if !p.isLocked {
		return nil
	}

	aid, err := lock.Generate(p.DatabaseName, p.EventStoreTable)
	if err != nil {
		return err
	}

	query := `SELECT RELEASE_LOCK(?)`
	if _, err := p.conn.ExecContext(ctx, query, aid); err != nil {
		return err
	}

	p.isLocked = false

	return nil
}

func (p *MySQL) ensureTable(ctx context.Context) (err error) {
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
	id BIGINT AUTO_INCREMENT not null primary key, 
	dispatched BOOL not null default false, 
	dispatched_at DATETIME,
	payload BLOB not null,
	options json,
	headers json
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
`, p.EventStoreTable)

	if _, err = p.conn.ExecContext(ctx, query); err != nil {
		return err
	}

	return nil
}
