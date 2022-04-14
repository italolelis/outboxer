package mysql

import (
	"context"
	"errors"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/italolelis/outboxer"
	"github.com/italolelis/outboxer/lock"
)

var eventStoreRows = []string{"id", "dispatched", "dispatched_at", "payload", "options", "headers"}

func TestMySQL_CloseSuccessfully(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ds, mock := getDatastore(ctx, t)
	mock.ExpectClose()

	if err := ds.Close(); err != nil {
		t.Errorf("error was not expected while closing connection: %s", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestMySQL_GetEventsSuccessfully(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ds, mock := getDatastore(ctx, t)
	mock.ExpectQuery(`SELECT (.+) FROM (.+) WHERE (.+) LIMIT (.+)`).
		WillReturnRows(sqlmock.NewRows(eventStoreRows).
			AddRow(1, false, time.Now(), []byte("test payload"), outboxer.DynamicValues{}, outboxer.DynamicValues{}))

	msgs, err := ds.GetEvents(ctx, 10)
	if err != nil {
		t.Fatalf("failed to retrieve messages from the data store: %s", err)
	}

	if len(msgs) != 1 {
		t.Fatalf("was expecting 1 message in the data store but got %d", len(msgs))
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestMySQL_GetEventsWhenThereIsNoMessage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ds, mock := getDatastore(ctx, t)
	mock.ExpectQuery(`SELECT (.+) FROM (.+) WHERE (.+) LIMIT (.+)`).
		WillReturnRows(sqlmock.NewRows(eventStoreRows))

	if _, err := ds.GetEvents(ctx, 10); err != nil {
		t.Fatalf("failed to retrieve messages from the data store: %s", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestMySQL_GetEventsWithAnSQLError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ds, mock := getDatastore(ctx, t)
	mock.ExpectQuery(`SELECT (.+) FROM (.+) WHERE (.+) LIMIT (.+)`).
		WillReturnError(errors.New("failed to fatch data"))

	if _, err := ds.GetEvents(ctx, 10); err == nil {
		t.Fatal("an error is exected when retrieving messages")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestMySQL_GetEventsWithAFailedScan(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ds, mock := getDatastore(ctx, t)
	mock.ExpectQuery(`SELECT (.+) FROM (.+) WHERE (.+) LIMIT (.+)`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))

	if _, err := ds.GetEvents(ctx, 10); err == nil {
		t.Fatal("an error is exected when retrieving messages")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestMySQL_SetAsDispatchedSuccessfully(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ds, mock := getDatastore(ctx, t)

	mock.ExpectExec(`update (.+) set (.+) where id = ?`).
		WithArgs(1).
		WillReturnResult(sqlmock.NewResult(0, 1))

	err := ds.SetAsDispatched(ctx, 1)
	if err != nil {
		t.Fatalf("error was not expected: %s", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestMySQL_SetAsDispatchedWithSQLError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ds, mock := getDatastore(ctx, t)

	mock.ExpectExec(`update (.+) set (.+) where id = ?`).
		WillReturnError(errors.New("failed to set message as dispatched"))

	err := ds.SetAsDispatched(ctx, 1)
	if err == nil {
		t.Fatal("error was expected")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestMySQL_AddSuccessfully(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ds, mock := getDatastore(ctx, t)

	mock.ExpectExec(`INSERT INTO event_store (.+) VALUES (.+)`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := ds.Add(ctx, &outboxer.OutboxMessage{
		Payload: []byte("test payload"),
	}); err != nil {
		t.Fatalf("failed to add message in the data store: %s", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestMySQL_AddFails(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ds, mock := getDatastore(ctx, t)
	mock.ExpectExec(`INSERT INTO event_store (.+) VALUES (.+)`).
		WillReturnError(errors.New("failed to remove messages"))

	if err := ds.Add(ctx, &outboxer.OutboxMessage{
		Payload: []byte("test payload"),
	}); err == nil {
		t.Fatal("an error was expected")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestMySQL_WithInstanceNoDB(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	defer db.Close()

	mock.ExpectQuery(`SELECT DATABASE()`).
		WillReturnError(errors.New("missing database"))

	_, err = WithInstance(ctx, db)
	if err == nil {
		t.Fatal("setting up the DS instance is supposed to error", err)
	}
}

func TestMySQL_WithInstanceWithEmptyDBName(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	defer db.Close()

	mock.ExpectQuery(`SELECT DATABASE()`).
		WillReturnRows(sqlmock.NewRows([]string{"DATABASE()"}).AddRow(""))

	_, err = WithInstance(ctx, db)
	if err == nil {
		t.Fatal("setting up the DS instance is supposed to error", err)
	}
}

func TestMySQL_AddWithinTx(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ds, mock := getDatastore(ctx, t)

	mock.ExpectBegin()
	mock.ExpectExec(`SELECT (.+) from (.+) LIMIT (.+)`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO (.+) (.+) VALUES (.+)`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	fn := func(tx outboxer.ExecerContext) error {
		_, err := tx.ExecContext(ctx, "SELECT * from (.+) LIMIT (.+)")
		return err
	}

	if err := ds.AddWithinTx(ctx, &outboxer.OutboxMessage{
		Payload: []byte("test payload"),
	}, fn); err != nil {
		t.Fatalf("failed to add message in the data store: %s", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func getDatastore(ctx context.Context, t *testing.T) (*MySQL, sqlmock.Sqlmock) {
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

	return ds, mock
}

func initDatastoreMock(t *testing.T, mock sqlmock.Sqlmock) {
	mock.ExpectQuery(`SELECT DATABASE()`).
		WillReturnRows(sqlmock.NewRows([]string{"DATABASE()"}).AddRow("test"))
	initLockMock(t, mock)
}

func initLockMock(t *testing.T, mock sqlmock.Sqlmock) {
	aid, err := lock.Generate("test", "event_store")
	if err != nil {
		t.Fatalf("failed to generate the lock value: %s", err)
	}

	mock.ExpectQuery(`SELECT GET_LOCK(.+)`).
		WithArgs(aid).
		WillReturnRows(sqlmock.NewRows([]string{"GET_LOCK"}).AddRow(true))
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS event_store (.+) ENGINE=InnoDB DEFAULT CHARSET=utf8;`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`SELECT RELEASE_LOCK(.+)`).
		WithArgs(aid).
		WillReturnResult(sqlmock.NewResult(0, 1))
}
