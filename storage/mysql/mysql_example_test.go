package mysql_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	"github.com/italolelis/outboxer/storage/mysql"
)

func ExampleMySQL() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := sql.Open("mysql", os.Getenv("DS_DSN"))
	if err != nil {
		fmt.Printf("failed to connect to mysql: %s", err)
		return
	}

	ds, err := mysql.WithInstance(ctx, db)
	if err != nil {
		fmt.Printf("failed to setup the data store: %s", err)
		return
	}

	ds.Close()
}
