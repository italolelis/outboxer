package sqlserver

import (
	"context"
	"errors"
	"regexp"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/italolelis/outboxer"
	"github.com/italolelis/outboxer/lock"
)

func TestSQLServer_WithInstance_must_return_SQLServerDataStore(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	defer db.Close()

	initDatastoreMock(t, mock)

	ds, err := WithInstance(ctx, db)
	if err != nil {
		t.Fatalf("failed to setup the data store: %s", err)
	}

	defer ds.Close()

	if ds.SchemaName != "test_schema" {
		t.Errorf("Expected schema name %s but got %s", "test_schema", ds.SchemaName)
	}

	if ds.DatabaseName != "test" {
		t.Errorf("Expected database name %s but got %s", "test", ds.DatabaseName)
	}
}

func TestSQLServer_WithInstance_should_return_error_when_no_db_selected(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	defer db.Close()

	mock.ExpectQuery(`SELECT DB_NAME() `).
		WillReturnRows(sqlmock.NewRows([]string{"DB_NAME()"}).AddRow(""))

	_, err = WithInstance(ctx, db)
	if err != ErrNoDatabaseName {
		t.Fatalf("Expected ErrNoDatabaseName to be returned when no database selected : %s", err)
	}
}

func TestSQLServer_WithInstance_should_return_error_when_no_schema_selected(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	defer db.Close()

	mock.ExpectQuery(`SELECT DB_NAME() `).
		WillReturnRows(sqlmock.NewRows([]string{"DB_NAME()"}).AddRow("test"))
	mock.ExpectQuery(`SELECT SCHEMA_NAME()`).
		WillReturnRows(sqlmock.NewRows([]string{"SCHEMA_NAME()"}).AddRow(""))

	_, err = WithInstance(ctx, db)
	if err != ErrNoSchema {
		t.Fatalf("Expected ErrNoSchema to be returned when no schema selected : %s", err)
	}
}

func TestSQLServer_should_add_message(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	defer db.Close()

	initDatastoreMock(t, mock)

	ds, err := WithInstance(ctx, db)
	if err != nil {
		t.Fatalf("failed to setup the data store: %s", err)
	}

	defer ds.Close()

	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO [test_schema].[event_store]`)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := ds.Add(ctx, &outboxer.OutboxMessage{
		Payload: []byte("test payload"),
	}); err != nil {
		t.Fatalf("failed to add message in the data store: %s", err)
	}
}

func TestSQLServer_should_add_message_with_tx(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	defer db.Close()

	initDatastoreMock(t, mock)

	ds, err := WithInstance(ctx, db)
	if err != nil {
		t.Fatalf("failed to setup the data store: %s", err)
	}

	defer ds.Close()

	mock.ExpectBegin()
	mock.ExpectExec(`SELECT (.+) from event_store`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO [test_schema].[event_store]`)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	fn := func(tx outboxer.ExecerContext) error {
		_, err := tx.ExecContext(ctx, "SELECT * from event_store")
		return err
	}

	if err := ds.AddWithinTx(ctx, &outboxer.OutboxMessage{
		Payload: []byte("test payload"),
	}, fn); err != nil {
		t.Fatalf("failed to add message in the data store: %s", err)
	}
}

func TestSQLServer_add_message_with_tx_should_rollback_on_error(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	defer db.Close()

	initDatastoreMock(t, mock)

	ds, err := WithInstance(ctx, db)
	if err != nil {
		t.Fatalf("failed to setup the data store: %s", err)
	}

	defer ds.Close()

	mock.ExpectBegin()
	mock.ExpectExec(`SELECT (.+) from event_store`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO [test_schema].[event_store]`)).
		WillReturnError(errors.New("Failed to insert"))
	mock.ExpectRollback()

	fn := func(tx outboxer.ExecerContext) error {
		_, err := tx.ExecContext(ctx, "SELECT * from event_store")
		return err
	}

	if err := ds.AddWithinTx(ctx, &outboxer.OutboxMessage{
		Payload: []byte("test payload"),
	}, fn); err == nil {
		t.Fatalf("This should fail and rollback: %s", err)
	}
}

func TestSQLServer_should_get_events(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	defer db.Close()

	initDatastoreMock(t, mock)

	ds, err := WithInstance(ctx, db)
	if err != nil {
		t.Fatalf("failed to setup the data store: %s", err)
	}

	defer ds.Close()

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT TOP 10 * FROM [test_schema].[event_store] WHERE dispatched = 0`)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "dispatched", "dispatched_at", "payload", "options", "headers"}).
			AddRow(1, false, time.Now(), []byte("test payload"), outboxer.DynamicValues{}, outboxer.DynamicValues{}))

	msgs, err := ds.GetEvents(ctx, 10)
	if err != nil {
		t.Fatalf("failed to retrieve messages from the data store: %s", err)
	}

	if len(msgs) != 1 {
		t.Fatalf("was expecting 1 message in the data store but got %d", len(msgs))
	}
}

func TestSQLServer_should_set_as_dispatched(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	defer db.Close()

	initDatastoreMock(t, mock)

	ds, err := WithInstance(ctx, db)
	if err != nil {
		t.Fatalf("failed to setup the data store: %s", err)
	}

	defer ds.Close()

	mock.ExpectExec(regexp.QuoteMeta(`UPDATE [test_schema].[event_store] SET`)).
		WithArgs(1).
		WillReturnResult(sqlmock.NewResult(0, 1))

	err = ds.SetAsDispatched(ctx, 1)
	if err != nil {
		t.Fatalf("failed to set message as dispatched: %s", err)
	}
}

func initDatastoreMock(t *testing.T, mock sqlmock.Sqlmock) {
	mock.ExpectQuery(`SELECT DB_NAME() `).
		WillReturnRows(sqlmock.NewRows([]string{"DB_NAME()"}).AddRow("test"))
	mock.ExpectQuery(`SELECT SCHEMA_NAME()`).
		WillReturnRows(sqlmock.NewRows([]string{"SCHEMA_NAME()"}).AddRow("test_schema"))

	initLockMock(t, mock)
}

func initLockMock(t *testing.T, mock sqlmock.Sqlmock) {
	aid, err := lock.Generate("test", "test_schema")
	if err != nil {
		t.Fatalf("failed to generate the lock value: %s", err)
	}

	mock.ExpectExec(`EXEC sp_getapplock
	@Resource = @p1,
	@LockOwner='Session',
	@LockMode = 'Exclusive'; `).
		WithArgs(aid).
		WillReturnResult(sqlmock.NewResult(0, 1))

	mock.ExpectExec(
		regexp.QuoteMeta(`IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='event_store' and xtype='U')
		CREATE TABLE test_schema.event_store`)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	mock.ExpectExec(`EXEC sp_releaseapplock
	@Resource = @p1,
	@LockOwner='Session'; `).
		WithArgs(aid).
		WillReturnResult(sqlmock.NewResult(0, 1))
}
