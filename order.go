package main

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

func processOrder(
	ctx context.Context,
	workerID string,
	orderID string,
) error {

	fmt.Printf(
		"[%s] processing order=%s\n",
		workerID,
		orderID,
	)

	select {
	case <-ctx.Done():
		return fmt.Errorf(
			"processing cancelled: %w",
			ctx.Err(),
		)
	case <-time.After(30 * time.Second):
		fmt.Printf(
			"[%s] completed order=%s\n",
			workerID,
			orderID,
		)

		return nil
	}
}

func isOrderCompleted(
	ctx context.Context,
	rdb *redis.Client,
	orderID string,
) (bool, error) {

	completed, err := rdb.SIsMember(
		ctx,
		completedSet,
		orderID,
	).Result()

	if err != nil {
		return false, err
	}

	return completed, nil
}
