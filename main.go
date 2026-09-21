package main

import (
	"context"
	"log"
	"os"

	"github.com/redis/go-redis/v9"
)

func main() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:       "localhost:6379",
		ClientName: "redis-lock-lab",
	})

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatal(err)
	}

	if len(os.Args) < 2 {
		log.Fatal("usage: go run main.go [producer|worker]")
	}

	switch os.Args[1] {
	case "producer":
		runProducer(ctx, rdb)

	case "worker":
		go runRecoveryWorker(ctx, rdb)
		go runRetryScheduler(ctx, rdb)
		runWorker(ctx, rdb)

	default:
		log.Fatal("unknown command")
	}
}
