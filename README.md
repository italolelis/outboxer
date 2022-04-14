# Outboxer

[![Build Status](https://github.com/italolelis/outboxer/workflows/Main/badge.svg)](https://github.com/italolelis/outboxer/actions)
[![codecov](https://codecov.io/gh/italolelis/outboxer/branch/master/graph/badge.svg?token=8G6G1B3QE6)](https://codecov.io/gh/italolelis/outboxer)
[![Go Report Card](https://goreportcard.com/badge/github.com/italolelis/outboxer)](https://goreportcard.com/report/github.com/italolelis/outboxer)
[![GoDoc](https://godoc.org/github.com/italolelis/outboxer?status.svg)](https://godoc.org/github.com/italolelis/outboxer)

Outboxer is a go library that implements the [outbox pattern](http://www.kamilgrzybek.com/design/the-outbox-pattern/).

## Getting Started

Outboxer was designed to simplify the tough work of orchestrating message reliabilty. Essentially we are trying to solve this question:

> How can producers reliably send messages when the broker/consumer is unavailable?

If you have a distributed system architecture and especially is dealing 
with [Event Driven Architecture](https://martinfowler.com/articles/201701-event-driven.html), you might 
want to use *outboxer*.

The first thing to do is include the package in your project

```sh
go get github.com/italolelis/outboxer
```

### Initial Configuration
Let's setup a simple example where you are using `RabbitMQ` and `Postgres` as your outbox pattern components:

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

db, err := sql.Open("postgres", os.Getenv("DS_DSN"))
if err != nil {
    fmt.Printf("could not connect to postgres: %s", err)
    return
}

conn, err := amqp.Dial(os.Getenv("ES_DSN"))
if err != nil {
    fmt.Printf("could not connect to amqp: %s", err)
    return
}

// we need to create a data store instance first
ds, err := postgres.WithInstance(ctx, db)
if err != nil {
    fmt.Printf("could not setup the data store: %s", err)
    return
}
defer ds.Close()

// we create an event stream passing the amqp connection
es := amqpOut.NewAMQP(conn)

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
    Options: map[string]interface{}{
        amqpOut.ExchangeNameOption: "test",
        amqpOut.ExchangeTypeOption: "topic",
        amqpOut.RoutingKeyOption:   "test.send",
    },
}); err != nil {
    fmt.Printf("could not send message: %s", err)
    return
}

// we can also listen for errors and ok messages that were send
for {
    select {
    case err := <-o.ErrChan():
        fmt.Printf("could not send message: %s", err)
    case <-o.OkChan():
        fmt.Printf("message received")
        return
    }
}
```

## Features

Outboxer comes with a few implementations of Data Stores and Event Streams.

### Data Stores

- [Postgres DataStore](storage/postgres/)
- [MySQL DataStore](storage/mysql/)
- [SQLServer DataStore](storage/sqlserver/)

### Event Streams

- [AMQP EventStream](es/amqp/)
- [Kinesis EventStream](es/kinesis/)
- [SQS EventStream](es/sqs/)
- [GCP PubSub](es/pubsub/)
- [Kafka EventStream](es/kafka/)

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, and the process for submitting pull requests to us.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details
