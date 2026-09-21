package main

import (
	"context"
	"log"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

func getRedisUnixTime(
	ctx context.Context,
	rdb *redis.Client,
) (int64, error) {

	t, err := rdb.Time(ctx).Result()
	if err != nil {
		return 0, err
	}

	return t.Unix(), nil
}

func findRetryableJobs(
	ctx context.Context,
	rdb *redis.Client,
) ([]string, error) {

	now, err := getRedisUnixTime(ctx, rdb)
	if err != nil {
		return nil, err
	}

	return rdb.ZRangeByScore(
		ctx,
		retrySet,
		&redis.ZRangeBy{
			Min: "-inf",
			Max: strconv.FormatInt(now, 10),
		},
	).Result()
}

func runRetryScheduler(
	ctx context.Context,
	rdb *redis.Client,
) {

	ticker := time.NewTicker(
		1 * time.Second,
	)

	defer ticker.Stop()

	for {
		select {

		case <-ctx.Done():
			return

		case <-ticker.C:

			jobs, err := findRetryableJobs(
				ctx,
				rdb,
			)

			if err != nil {
				log.Printf(
					"[retry] scan error: %v",
					err,
				)
				continue
			}

			for _, jobID := range jobs {

				if err := promoteRetryJob(
					ctx,
					rdb,
					jobID,
				); err != nil {

					log.Printf(
						"[retry] promote job=%s error=%v",
						jobID,
						err,
					)
				}
			}
		}
	}
}
