package sqlserver_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	"github.com/italolelis/outboxer/sqlserver"
)

func ExampleSQLServer() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := sql.Open("sqlserver", os.Getenv("DS_DSN"))
	if err != nil {
		fmt.Printf("failed to connect to SQLServer: %s", err)
		return
	}

	ds, err := sqlserver.WithInstance(ctx, db)
	if err != nil {
		fmt.Printf("failed to setup the data store: %s", err)
		return
	}

	defer ds.Close()
}
