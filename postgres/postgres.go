// Package postgres is the implementation of the postgres data store.
package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"hash/crc32"
	"strings"
	"time"

	"github.com/italolelis/outboxer"
)

const (
	// DefaultEventStoreTable is the default table name
	DefaultEventStoreTable      = "event_store"
	advisoryLockIDSalt     uint = 1486364155
)

var (
	// ErrLocked is used when we can't acquire an explicit lock
	ErrLocked = errors.New("can't acquire lock")

	// ErrNoDatabaseName is used when the database name is blank
	ErrNoDatabaseName = errors.New("no database name")

	// ErrNoSchema is used when the schema name is blank
	ErrNoSchema = errors.New("no schema")
)

// Postgres is the implementation of the data store
type Postgres struct {
	// Locking and unlocking need to use the same connection
	conn     *sql.Conn
	db       *sql.DB
	isLocked bool

	DatabaseName    string
	SchemaName      string
	EventStoreTable string
}

// WithInstance creates a postgres data store with an existing db connection
func WithInstance(ctx context.Context, db *sql.DB) (*Postgres, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}

	p := Postgres{conn: conn, db: db}

	if err := db.QueryRow(`SELECT CURRENT_DATABASE()`).Scan(&p.DatabaseName); err != nil {
		return nil, err
	}

	if len(p.DatabaseName) == 0 {
		return nil, ErrNoDatabaseName
	}

	if err := db.QueryRow(`SELECT CURRENT_SCHEMA()`).Scan(&p.SchemaName); err != nil {
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

// Close closes the db connetion
func (p *Postgres) Close() error {
	connErr := p.conn.Close()
	dbErr := p.db.Close()
	if connErr != nil || dbErr != nil {
		return fmt.Errorf("conn: %v, db: %v", connErr, dbErr)
	}
	return nil
}

// GetEvents retrieves all the relevant events
func (p *Postgres) GetEvents(context.Context) ([]*outboxer.OutboxMessage, error) {
	var events []*outboxer.OutboxMessage

	rows, err := p.db.Query(fmt.Sprintf("SELECT * FROM %s WHERE dispatched = true LIMIT 1000", p.EventStoreTable))
	if err != nil {
		return events, fmt.Errorf("could not get events from the store: %s", err)
	}

	for rows.Next() {
		var e outboxer.OutboxMessage
		err = rows.Scan(&e.ID, &e.Dispatched, &e.DispatchedAt, &e.Payload, &e.Options, &e.Headers)
		if err != nil {
			return events, fmt.Errorf("could not scan event: %s", err)
		}
		events = append(events, &e)
	}

	return events, nil
}

// Add adds the message to the data store
func (p *Postgres) Add(ctx context.Context, evt *outboxer.OutboxMessage) error {
	tx, err := p.conn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return fmt.Errorf("transaction start failed: %s", err)
	}

	query := fmt.Sprintf(`INSERT INTO %s (dispatched, dispatched_at, payload, options, headers) VALUES ($1, $2, $3, $4, $5)`, p.EventStoreTable)
	if _, err := tx.ExecContext(ctx, query, evt.Dispatched, evt.DispatchedAt, evt.Payload, evt.Options, evt.Headers); err != nil {
		tx.Rollback()
		return fmt.Errorf("could not insert the event into the data store: %s", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("transaction commit failed: %s", err)
	}

	return nil
}

// AddWithinTx creates a transaction and then tries to execute anything within it
func (p *Postgres) AddWithinTx(ctx context.Context, evt *outboxer.OutboxMessage, fn func(driver.Tx) error) error {
	tx, err := p.conn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return fmt.Errorf("transaction start failed: %s", err)
	}

	err = fn(tx)
	if err != nil {
		return err
	}

	query := fmt.Sprintf(`INSERT INTO %s (dispatched, dispatched_at, payload, options, headers) VALUES ($1, $2, $3, $4, $5)`, p.EventStoreTable)
	if _, err := tx.ExecContext(ctx, query, evt.Dispatched, evt.DispatchedAt, evt.Payload, evt.Options, evt.Headers); err != nil {
		tx.Rollback()
		return fmt.Errorf("could not insert the event into the data store: %s", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("transaction commit failed: %s", err)
	}

	return nil
}

// SetAsDispatched sets one message as dispatched
func (p *Postgres) SetAsDispatched(ctx context.Context, id int64) error {
	query := fmt.Sprintf(`
update %s
set
    dispatched = true,
    dispatched_at = now(),
    options = '{}',
	headers = '{}'
where id = $1;
`, p.EventStoreTable)
	if _, err := p.db.ExecContext(ctx, query, id); err != nil {
		return fmt.Errorf("could not insert the event into the data store: %s", err)
	}

	return nil
}

// Remove removes old messages from the data store
func (p *Postgres) Remove(ctx context.Context) error {
	tx, err := p.conn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return fmt.Errorf("transaction start failed: %s", err)
	}

	q := `
DELETE FROM %s
WHERE ctid in
(
    select ctid
    from %s
    where
        "dispatched" = true and
        "dispatched_at" < $1
    limit 1000
)
`
	dispatchedBefore := time.Now().AddDate(0, 0, -5)

	query := fmt.Sprintf(q, p.EventStoreTable, p.EventStoreTable)
	if _, err := tx.ExecContext(ctx, query, dispatchedBefore.Unix()); err != nil {
		tx.Rollback()
		return fmt.Errorf("could not insert the event into the data store: %s", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("transaction commit failed: %s", err)
	}

	return nil
}

// Lock implements explicit locking
// https://www.postgresql.org/docs/9.6/static/explicit-locking.html#ADVISORY-LOCKS
func (p *Postgres) Lock(ctx context.Context) error {
	if p.isLocked {
		return ErrLocked
	}

	aid, err := GenerateAdvisoryLockID(p.DatabaseName, p.SchemaName)
	if err != nil {
		return err
	}

	// This will either obtain the lock immediately and return true,
	// or return false if the lock cannot be acquired immediately.
	query := `SELECT pg_advisory_lock($1)`
	if _, err := p.conn.ExecContext(ctx, query, aid); err != nil {
		return fmt.Errorf("try lock failed: %s", err)
	}

	p.isLocked = true
	return nil
}

// Unlock is the implementation of the unlock for explicit locking
func (p *Postgres) Unlock(ctx context.Context) error {
	if !p.isLocked {
		return nil
	}

	aid, err := GenerateAdvisoryLockID(p.DatabaseName, p.SchemaName)
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
	if err = p.Lock(ctx); err != nil {
		return err
	}

	defer func() {
		if e := p.Unlock(ctx); e != nil {
			if err == nil {
				err = e
			} else {
				err = fmt.Errorf("could not unlock the table: %s", err)
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

// GenerateAdvisoryLockID inspired by rails migrations, see https://goo.gl/8o9bCT
func GenerateAdvisoryLockID(databaseName string, additionalNames ...string) (string, error) {
	if len(additionalNames) > 0 {
		databaseName = strings.Join(append(additionalNames, databaseName), "\x00")
	}
	sum := crc32.ChecksumIEEE([]byte(databaseName))
	sum = sum * uint32(advisoryLockIDSalt)
	return fmt.Sprintf("%v", sum), nil
}
