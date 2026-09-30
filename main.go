package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

const shutdownGracePeriod = 20 * time.Second

func main() {
	shutdownCtx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	rdb := redis.NewClient(&redis.Options{
		Addr:       "localhost:6379",
		ClientName: "redis-lock-lab",
	})

	if err := rdb.Ping(shutdownCtx).Err(); err != nil {
		log.Fatal(err)
	}

	if len(os.Args) < 2 {
		log.Fatal("usage: go run main.go [producer|worker]")
	}

	switch os.Args[1] {
	case "producer":
		runProducer(shutdownCtx, rdb)

	case "worker":
		go runRecoveryWorker(shutdownCtx, rdb)
		go runRetryScheduler(shutdownCtx, rdb)
		runWorker(shutdownCtx, rdb)

	default:
		log.Fatal("unknown command")
	}
}
