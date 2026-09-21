package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

func runRecoveryWorker(
	ctx context.Context,
	rdb *redis.Client,
) {

	ticker := time.NewTicker(5 * time.Second)

	defer ticker.Stop()

	for {
		select {

		case <-ctx.Done():
			return

		case <-ticker.C:
			recoverExpiredJobs(ctx, rdb)
		}
	}
}

func recoverExpiredJobs(ctx context.Context, rdb *redis.Client) {

	now := time.Now().Unix()

	expiredJobs, err := rdb.ZRangeByScore(
		ctx,
		leaseSet,
		&redis.ZRangeBy{
			Min: "-inf",
			Max: strconv.FormatInt(now, 10),
		},
	).Result()

	if err != nil {
		log.Printf(
			"recovery scan failed: %v",
			err,
		)

		return
	}

	for _, jobID := range expiredJobs {

		fmt.Printf(
			"[recovery] expired job=%s\n",
			jobID,
		)

		if err := recoverJob(ctx, rdb, jobID); err != nil {
			log.Printf(
				"[recovery] job=%s failed: %v",
				jobID,
				err,
			)
		}
	}
}
