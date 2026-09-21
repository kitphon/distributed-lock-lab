package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

var ErrResourceLocked = errors.New("resource already locked")

func handleJob(
	ctx context.Context,
	rdb *redis.Client,
	workerID string,
	job *ClaimedJob,
) error {

	token := workerID + ":" + uuid.NewString()

	acquired, err := acquireLock(ctx, rdb, job.OrderID, token, 10*time.Second)

	if err != nil {
		return err
	}

	if !acquired {
		fmt.Printf(
			"[%s] order=%s already locked, skip\n",
			workerID,
			job.OrderID,
		)

		return ErrResourceLocked
	}

	fmt.Printf(
		"[%s] acquired order=%s token=%s\n",
		workerID,
		job.OrderID,
		token,
	)

	jobCtx, cancelJob := context.WithCancel(ctx)

	var heartbeatWG sync.WaitGroup

	heartbeatWG.Add(2)

	go func() {
		defer heartbeatWG.Done()

		runLeaseHeartbeat(
			jobCtx,
			rdb,
			job,
			cancelJob,
		)
	}()

	go func() {
		defer heartbeatWG.Done()

		runLockHeartbeat(
			jobCtx,
			rdb,
			job.OrderID,
			token,
			cancelJob,
		)
	}()

	err = processOrder(
		jobCtx,
		workerID,
		job.OrderID,
	)

	cancelJob()

	heartbeatWG.Wait()

	if releaseErr := releaseLock(ctx, rdb, job.OrderID, token); releaseErr != nil {
		log.Printf(
			"[%s] release lock order=%s failed: %v",
			workerID,
			job.OrderID,
			releaseErr,
		)
	}

	return err
}

func runLeaseHeartbeat(
	ctx context.Context,
	rdb *redis.Client,
	job *ClaimedJob,
	onOwnershipLost context.CancelFunc,
) {

	ticker := time.NewTicker(heartbeatInterval)

	defer ticker.Stop()

	for {
		select {

		case <-ctx.Done():
			return

		case <-ticker.C:
			renewed, err := renewLease(
				ctx,
				rdb,
				job,
			)

			if err != nil {
				if ctx.Err() != nil {
					return
				}

				log.Printf(
					"[heartbeat] order=%s error=%v",
					job.OrderID,
					err,
				)

				continue
			}

			if ctx.Err() != nil {
				return
			}

			if !renewed {
				log.Printf(
					"[heartbeat] order=%s ownership lost",
					job.OrderID,
				)

				onOwnershipLost()
				return
			}

			fmt.Printf(
				"[heartbeat] renewed order=%s\n",
				job.OrderID,
			)
		}
	}
}

func runLockHeartbeat(
	ctx context.Context,
	rdb *redis.Client,
	orderID string,
	lockToken string,
	onOwnershipLost context.CancelFunc,
) {
	ticker := time.NewTicker(
		resourceLockHeartbeat,
	)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			renewed, err := renewLock(
				ctx,
				rdb,
				orderID,
				lockToken,
				resourceLockTTL,
			)

			if err != nil {
				if ctx.Err() != nil {
					return
				}

				log.Printf(
					"[lock-heartbeat] order=%s error=%v",
					orderID,
					err,
				)
				continue
			}

			if ctx.Err() != nil {
				return
			}

			if !renewed {
				log.Printf(
					"[lock-heartbeat] order=%s ownership lost",
					orderID,
				)

				onOwnershipLost()
				return
			}

			fmt.Printf(
				"[lock-heartbeat] renewed order=%s\n",
				orderID,
			)
		}
	}
}
