package mysql

import (
	"context"
	"database/sql"
	"github.com/go-sql-driver/mysql"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/italolelis/outboxer"
)

func TestDatastore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		scenario string
		function func(context.Context, *testing.T, outboxer.DataStore)
	}{
		{
			"successfully add the message into the event store",
			testAddSuccessfully,
		},
		{
			"successfully set the message as dispatched",
			testSetDispatchedSuccessfully,
		},
		{
			"add messages within a transaction",
			testAddTx,
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	u, err := url.Parse(os.Getenv("MYSQL_DS_DSN"))
	if err != nil {
		t.Fatalf("failed to parse MYSQL DSN: %s", err)
	}

	c, err := urlToMySQLConfig(*u)
	if err != nil {
		t.Fatalf("failed parse the DSN into a MYSQL Config: %s", err)
	}

	db, err := sql.Open("mysql", c.FormatDSN())
	if err != nil {
		t.Fatalf("failed to connect to mysql: %s", err)
	}

	ds, err := WithInstance(ctx, db)
	if err != nil {
		t.Fatalf("failed to setup the data store: %s", err)
	}
	defer ds.Close()

	if err := ds.Remove(ctx, time.Now(), 20); err != nil {
		t.Fatalf("failed to clean the data store before starting tests for mysql: %s", err)
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			test.function(ctx, t, ds)
		})
	}
}

func testAddSuccessfully(ctx context.Context, t *testing.T, ds outboxer.DataStore) {
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

	if err := ds.Remove(ctx, time.Now(), 10); err != nil {
		t.Fatalf("failed to remove messages: %s", err)
	}
}

func testSetDispatchedSuccessfully(ctx context.Context, t *testing.T, ds outboxer.DataStore) {
	if err := ds.Add(ctx, &outboxer.OutboxMessage{
		Payload: []byte("test payload"),
	}); err != nil {
		t.Fatalf("failed to add message in the data store: %s", err)
	}

	msgs, err := ds.GetEvents(ctx, 10)
	if err != nil {
		t.Fatalf("failed to retrieve messages from the data store: %s", err)
	}

	for _, m := range msgs {
		err := ds.SetAsDispatched(ctx, m.ID)
		if err != nil {
			t.Fatalf("failed to set message as dispatched: %s", err)
		}
	}

	if err := ds.Remove(ctx, time.Now(), 10); err != nil {
		t.Fatalf("failed to remove messages: %s", err)
	}
}

func testAddTx(ctx context.Context, t *testing.T, ds outboxer.DataStore) {
	fn := func(tx outboxer.ExecerContext) error {
		_, err := tx.ExecContext(ctx, "SELECT * from event_store LIMIT 1")
		return err
	}

	if err := ds.AddWithinTx(ctx, &outboxer.OutboxMessage{
		Payload: []byte("test payload"),
	}, fn); err != nil {
		t.Fatalf("failed to add message in the data store: %s", err)
	}

	msgs, err := ds.GetEvents(ctx, 10)
	if err != nil {
		t.Fatalf("failed to retrieve messages from the data store: %s", err)
	}

	for _, m := range msgs {
		err := ds.SetAsDispatched(ctx, m.ID)
		if err != nil {
			t.Fatalf("failed to set message as dispatched: %s", err)
		}
	}

	if err := ds.Remove(ctx, time.Now(), 10); err != nil {
		t.Fatalf("failed to remove messages: %s", err)
	}
}

// urlToMySQLConfig takes a net/url URL and returns a go-sql-driver/mysql Config.
// Manually sets username and password to avoid net/url from url-encoding the reserved URL characters
func urlToMySQLConfig(u url.URL) (*mysql.Config, error) {
	origUserInfo := u.User
	u.User = nil

	c, err := mysql.ParseDSN(strings.TrimPrefix(u.String(), "mysql://"))
	if err != nil {
		return nil, err
	}
	if origUserInfo != nil {
		c.User = origUserInfo.Username()
		if p, ok := origUserInfo.Password(); ok {
			c.Passwd = p
		}
	}
	return c, nil
}
