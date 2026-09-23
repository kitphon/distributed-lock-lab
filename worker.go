package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const idlePollInterval = 500 * time.Millisecond

func runWorker(
	ctx context.Context,
	rdb *redis.Client,
) {
	workerID := uuid.NewString()[:8]

	fmt.Printf(
		"[%s] worker started\n",
		workerID,
	)

	for {
		select {
		case <-ctx.Done():
			return

		default:
		}

		job, err := claimJob(
			ctx,
			rdb,
			workerID,
		)

		if err != nil {
			log.Printf(
				"[%s] claim error: %v",
				workerID,
				err,
			)

			time.Sleep(idlePollInterval)
			continue
		}

		if job == nil {
			time.Sleep(idlePollInterval)
			continue
		}

		fmt.Printf(
			"[%s] claimed job=%s order=%s token=%s\n",
			workerID,
			job.JobID,
			job.OrderID,
			job.Token,
		)

		err = handleJob(ctx, rdb, workerID, job)

		if err != nil {

			log.Printf(
				"[%s] process job=%s order=%s failed: %v",
				workerID,
				job.JobID,
				job.OrderID,
				err,
			)

			if errors.Is(err, ErrResourceLocked) {
				if deferErr := deferJob(ctx, rdb, job); deferErr != nil {
					log.Printf(
						"[%s] defer job=%s order=%s failed: %v",
						workerID,
						job.JobID,
						job.OrderID,
						deferErr,
					)

					continue
				}

				fmt.Printf(
					"[%s] DEFER job=%s order=%s delay=%s\n",
					workerID,
					job.JobID,
					job.OrderID,
					deferDelay,
				)

				continue
			}

			result, nackErr := nackJob(ctx, rdb, job)
			if nackErr != nil {
				log.Printf(
					"[%s] NACK job=%s failed: %v",
					workerID,
					job.JobID,
					nackErr,
				)

				continue
			}

			if result.DeadLetter {
				fmt.Printf(
					"[%s] DLQ job=%s order=%s retries=%d\n",
					workerID,
					job.JobID,
					job.OrderID,
					result.RetryCount-1,
				)

				continue
			}

			fmt.Printf(
				"[%s] NACK job=%s retry=%d delay=%ds\n",
				workerID,
				job.JobID,
				result.RetryCount,
				result.RetryDelay,
			)

			continue
		}

		if err := ackJob(ctx, rdb, job); err != nil {
			log.Printf(
				"[%s] ACK order=%s failed: %v",
				workerID,
				job.OrderID,
				err,
			)

			continue
		}

		fmt.Printf(
			"[%s] ACK order=%s\n",
			workerID,
			job.OrderID,
		)

	}
}
