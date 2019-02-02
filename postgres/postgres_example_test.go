package postgres_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	"github.com/italolelis/outboxer/postgres"
)

func ExamplePostgres() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := sql.Open("postgres", os.Getenv("DS_DSN"))
	if err != nil {
		fmt.Printf("failed to connect to postgres: %s", err)
		return
	}

	ds, err := postgres.WithInstance(ctx, db)
	if err != nil {
		fmt.Printf("failed to setup the data store: %s", err)
		return
	}
	defer ds.Close()
}
