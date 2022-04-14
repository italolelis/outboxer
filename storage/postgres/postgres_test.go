package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/italolelis/outboxer"
	"github.com/italolelis/outboxer/lock"
)

// nolint
func TestPostgres_AddSuccessfully(t *testing.T) {
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
	mock.ExpectExec(`INSERT INTO event_store (.+) VALUES (.+)`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	mock.ExpectQuery(`SELECT (.+) FROM event_store WHERE dispatched = false LIMIT 10`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "dispatched", "dispatched_at", "payload", "options", "headers"}).
			AddRow(1, false, time.Now(), []byte("test payload"), outboxer.DynamicValues{}, outboxer.DynamicValues{}))

	mock.ExpectExec(`update event_store set (.+)where id = ?`).
		WithArgs(1).
		WillReturnResult(sqlmock.NewResult(0, 1))

	mock.ExpectBegin()
	mock.ExpectExec(`DELETE FROM (.+) WHERE ctid IN (.+)`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := ds.Add(ctx, &outboxer.OutboxMessage{
		Payload: []byte("test payload"),
	}); err != nil {
		t.Fatalf("failed to add message in the data store: %s", err)
	}

	msgs, err := ds.GetEvents(ctx, 10)
	if err != nil {
		t.Fatalf("failed to retrieve messages from the data store: %s", err)
	}

	if len(msgs) != 1 {
		t.Fatalf("was expecting 1 message in the data store but got %d", len(msgs))
	}

	for _, m := range msgs {
		err := ds.SetAsDispatched(ctx, m.ID)
		if err != nil {
			t.Fatalf("failed to set message as dispatched: %s", err)
		}
	}
}

func TestPostgres_WithInstanceNoDB(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	defer db.Close()

	mock.ExpectQuery(`SELECT CURRENT_DATABASE()`).
		WillReturnError(errors.New("missing database"))

	_, err = WithInstance(ctx, db)
	if err == nil {
		t.Fatal("setting up the DS instance is supposed to error", err)
	}
}

func TestPostgres_WithInstanceWithEmptyDBName(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	defer db.Close()

	mock.ExpectQuery(`SELECT CURRENT_DATABASE()`).
		WillReturnRows(sqlmock.NewRows([]string{"DATABASE()"}).AddRow(""))

	_, err = WithInstance(ctx, db)
	if err == nil {
		t.Fatal("setting up the DS instance is supposed to error", err)
	}
}

func TestPostgres_WithInstanceNoSchema(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	defer db.Close()

	mock.ExpectQuery(`SELECT CURRENT_SCHEMA()`).
		WillReturnError(errors.New("missing database"))

	_, err = WithInstance(ctx, db)
	if err == nil {
		t.Fatal("setting up the DS instance is supposed to error", err)
	}
}

func TestPostgres_WithInstanceWithEmptySchemaName(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	defer db.Close()

	mock.ExpectQuery(`SELECT CURRENT_SCHEMA()`).
		WillReturnRows(sqlmock.NewRows([]string{"CURRENT_SCHEMA()"}).AddRow(""))

	_, err = WithInstance(ctx, db)
	if err == nil {
		t.Fatal("setting up the DS instance is supposed to error", err)
	}
}

func TestPostgres_AddWithinTx(t *testing.T) {
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
	mock.ExpectExec(`SELECT (.+) from event_store LIMIT 1`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO event_store (.+) VALUES (.+)`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	fn := func(tx outboxer.ExecerContext) error {
		_, err := tx.ExecContext(ctx, "SELECT * from event_store LIMIT 1")
		return err
	}

	if err := ds.AddWithinTx(ctx, &outboxer.OutboxMessage{
		Payload: []byte("test payload"),
	}, fn); err != nil {
		t.Fatalf("failed to add message in the data store: %s", err)
	}
}

func initDatastoreMock(t *testing.T, mock sqlmock.Sqlmock) {
	mock.ExpectQuery(`SELECT CURRENT_DATABASE()`).
		WillReturnRows(sqlmock.NewRows([]string{"CURRENT_DATABASE()"}).AddRow("test"))
	mock.ExpectQuery(`SELECT CURRENT_SCHEMA()`).
		WillReturnRows(sqlmock.NewRows([]string{"CURRENT_SCHEMA()"}).AddRow("test_schema"))

	initLockMock(t, mock)
}

func initLockMock(t *testing.T, mock sqlmock.Sqlmock) {
	aid, err := lock.Generate("test", "test_schema")
	if err != nil {
		t.Fatalf("failed to generate the lock value: %s", err)
	}

	mock.ExpectExec(`SELECT pg_advisory_lock(.+)`).
		WithArgs(aid).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS event_store (.+);`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`SELECT pg_advisory_unlock(.+)`).
		WithArgs(aid).
		WillReturnResult(sqlmock.NewResult(0, 1))
}
