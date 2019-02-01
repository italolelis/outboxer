# Outboxer

[![Build Status](https://travis-ci.com/italolelis/outboxer.svg?branch=master)](https://travis-ci.com/italolelis/outboxer)

Outboxer is a go library that implements the [outbox pattern](http://gistlabs.com/2014/05/the-outbox/).

## Getting Started

Outboxer was designed to simplify the tough work of orchestrating message reliabilty. Essentially we are trying to solve this question:

> How can producers reliably send messages when the broker/consumer is unavailable?

If you have a distributed system architecture and especially is dealing with Event Driven Architecture, you might want to use *outboxer*.

The first thing to do is include the package in your project

```sh
go get github.com/italolelis/outboxer
```

### Initial Configuration
Let's setup a simple example where you are using `RabbitMQ` and `Postgres` as your outbox pattern components:

```go
// we need to create a data store instance first
ds, err := WithInstance(ctx, db)
if err != nil {
    t.Fatalf("could not setup the data store: %s", err)
}

// we create an event stream passing the amqp connection
es := NewAMQP(conn)

// now we create an outboxer instance passing the data store and event stream
o, err := outboxer.New(
    outboxer.WithDataStore(&inMemDS{}),
    outboxer.WithEventStream(es),
    outboxer.WithCheckInterval(1*time.Second),
    outboxer.WithCleanupInterval(5*time.Second),
)
if err != nil {
    fmt.Errorf("could not create an outboxer instance: %s", err)
}

// here we initialize the outboxer checks and cleanup go rotines
o.Start(ctx)
defer o.Stop()

// finally we are ready to send messages
if err = o.Send(ctx, &outboxer.OutboxMessage{
    Payload: []byte("test payload"),
    Options: map[string]interface{}{
        amqp.ExchangeNameOption: "test",
        amqp.ExchangeTypeOption: "topic",
        amqp.RoutingKeyOption: "test.send",
    },
}); err != nil {
    fmt.Errorf("could not send message: %s", err)
}

// we can also listen for errors and ok messages that were send
go func() {
   for {
        select {
        case err := <-o.ErrChan():
            fmt.Errorf("could not send message: %s", err)
        case <-o.OkChan():
            fmt.Print("message received")
        }
    }
}()
```

## Features

Outboxer comes with a few implementations of Data Stores and Event Streams.

- [Postgres DataStore](postgres/)
- [AMQP EventStream](amqp/)

## Documentation

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, and the process for submitting pull requests to us.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details
