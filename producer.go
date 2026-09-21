package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func runProducer(
	ctx context.Context,
	rdb *redis.Client,
) {
	if len(os.Args) < 3 {
		log.Fatal("usage: go run . producer <order-id>")
	}

	orderID := os.Args[2]
	jobID := uuid.NewString()

	_, err := enqueueJobScript.Run(
		ctx,
		rdb,
		[]string{
			jobData,
			pendingQueue,
		},
		jobID,
		orderID,
	).Result()

	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf(
		"enqueued job=%s order=%s\n",
		jobID,
		orderID,
	)
}
