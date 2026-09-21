package main

import (
	"context"
	"fmt"
	"time"
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
