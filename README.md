# Kafka Aggregator Service with Go

## Setup

Start kafka

```sh
docker compose up
```

Install dependencies

```sh
go get -u github.com/confluentinc/confluent-kafka-go/kafka@v1.9.2
```

Produce messages

```sh
go run .
```
