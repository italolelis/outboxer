package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/Shopify/sarama"
	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/italolelis/outboxer"
	"github.com/italolelis/outboxer/es/kafka"
	"github.com/italolelis/outboxer/storage/mysql"
	"time"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Creates database connection
	db, err := sql.Open("mysql", "user:pass@(localhost:5578)/core?charset=utf8&parseTime=true")
	if err != nil {
		fmt.Printf("could not connect to mysql: %s", err)
		return
	}

	// Creates kafka sarama configuration
	cfg := sarama.NewConfig()
	// must set these as this guarantee  delivery to kafka
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	client, err := sarama.NewClient([]string{
		"kafka-development-01.b2bdev.pro:9092",
		"kafka-development-02.b2bdev.pro:9092",
		"kafka-development-03.b2bdev.pro:9092",
	}, cfg)
	if err != nil {
		fmt.Printf("could not connect to kafka: %s", err)
		return
	}

	es, err := kafka.NewSyncKafka(client)
	if err != nil {
		fmt.Printf("could not init kafka event stream: %s", err)
		return
	}

	ds, err := mysql.WithInstance(ctx, db)
	// we need to create a data store instance first
	if err != nil {
		fmt.Printf("could not setup the data store: %s", err)
		return
	}
	defer ds.Close()

	//// we create an event stream passing the amqp connection
	//es := amqpOut.NewAMQP(conn)

	// now we create an outboxer instance passing the data store and event stream
	o, err := outboxer.New(
		outboxer.WithDataStore(ds),
		outboxer.WithEventStream(es),
		outboxer.WithCheckInterval(1*time.Second),
	)
	if err != nil {
		fmt.Printf("could not create an outboxer instance: %s", err)
		return
	}

	// here we initialize the outboxer checks and cleanup go rotines
	o.Start(ctx)
	defer o.Stop()

	// finally we are ready to send messages
	if err = o.Send(ctx, &outboxer.OutboxMessage{
		Payload: []byte("test payload"),
		Options: outboxer.DynamicValues{
			kafka.Topic: "zorin_test",
		},
		Headers: outboxer.DynamicValues{
			"uuid": uuid.New().String(),
		},
	}); err != nil {
		fmt.Printf("could not send message: %s", err)
		return
	}

	for {
		select {
		case err := <-o.ErrChan():
			fmt.Printf("could not send message: %s", err)
		case <-o.OkChan():
			fmt.Printf("message received")
			return
		}
	}
}
