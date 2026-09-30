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
	shutdownCtx context.Context,
	rdb *redis.Client,
) {
	workerID := uuid.NewString()[:8]

	fmt.Printf(
		"[%s] worker started\n",
		workerID,
	)

	for {
		select {
		case <-shutdownCtx.Done():
			fmt.Printf(
				"[%s] shutdown requested: stop claiming\n",
				workerID,
			)
			return

		default:
		}

		job, err := claimJob(
			shutdownCtx,
			rdb,
			workerID,
		)

		if err != nil {
			if shutdownCtx.Err() != nil {
				fmt.Printf(
					"[%s] shutdown requested during claim\n",
					workerID,
				)
				return
			}

			log.Printf(
				"[%s] claim error: %v",
				workerID,
				err,
			)

			time.Sleep(idlePollInterval)
			continue
		}

		if job == nil {
			select {
			case <-shutdownCtx.Done():
				fmt.Printf(
					"[%s] shutdown requested while idle\n",
					workerID,
				)
				return

			case <-time.After(idlePollInterval):
				continue
			}
		}

		fmt.Printf(
			"[%s] claimed job=%s order=%s token=%s\n",
			workerID,
			job.JobID,
			job.OrderID,
			job.Token,
		)

		jobCtx, cancelJobCtx := context.WithCancel(
			context.Background(),
		)

		jobResult := make(chan error, 1)

		go func() {
			jobResult <- handleJob(
				jobCtx,
				rdb,
				workerID,
				job,
			)
		}()

		shutdownDuringJob := false
		graceExpired := false

		select {
		case err = <-jobResult:
			cancelJobCtx()

		case <-shutdownCtx.Done():
			shutdownDuringJob = true

			fmt.Printf(
				"[%s] shutdown requested: draining job=%s\n",
				workerID,
				job.JobID,
			)

			timer := time.NewTimer(
				shutdownGracePeriod,
			)

			select {
			case err = <-jobResult:
				timer.Stop()

				fmt.Printf(
					"[%s] job=%s finished during grace period\n",
					workerID,
					job.JobID,
				)

			case <-timer.C:
				graceExpired = true

				fmt.Printf(
					"[%s] grace period exceeded: cancelling job=%s\n",
					workerID,
					job.JobID,
				)

				cancelJobCtx()

				err = <-jobResult
			}

			cancelJobCtx()
		}

		if shutdownDuringJob && graceExpired {
			cleanupCtx, cancelCleanup := newCleanupContext()
			abandonErr := abandonJob(
				cleanupCtx,
				rdb,
				job,
			)

			cancelCleanup()

			if abandonErr != nil {
				log.Printf(
					"[%s] ABANDON job=%s order=%s failed: %v",
					workerID,
					job.JobID,
					job.OrderID,
					abandonErr,
				)

				return
			}

			fmt.Printf(
				"[%s] ABANDON job=%s order=%s -> pending\n",
				workerID,
				job.JobID,
				job.OrderID,
			)

			return
		}

		if err != nil {
			log.Printf(
				"[%s] process job=%s order=%s failed: %v",
				workerID,
				job.JobID,
				job.OrderID,
				err,
			)

			if errors.Is(err, ErrResourceLocked) {
				cleanupCtx, cancelCleanup := newCleanupContext()
				deferErr := deferJob(cleanupCtx, rdb, job)

				cancelCleanup()

				if deferErr != nil {
					log.Printf(
						"[%s] defer job=%s order=%s failed: %v",
						workerID,
						job.JobID,
						job.OrderID,
						deferErr,
					)

					if shutdownDuringJob {
						return
					}

					continue
				}

				fmt.Printf(
					"[%s] DEFER job=%s order=%s delay=%s\n",
					workerID,
					job.JobID,
					job.OrderID,
					deferDelay,
				)

				if shutdownDuringJob {
					return
				}

				continue
			}

			cleanupCtx, cancelCleanup := newCleanupContext()
			result, nackErr := nackJob(cleanupCtx, rdb, job)

			cancelCleanup()

			if nackErr != nil {
				log.Printf(
					"[%s] NACK job=%s failed: %v",
					workerID,
					job.JobID,
					nackErr,
				)

				if shutdownDuringJob {
					return
				}

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

				if shutdownDuringJob {
					return
				}

				continue
			}

			fmt.Printf(
				"[%s] NACK job=%s retry=%d delay=%ds\n",
				workerID,
				job.JobID,
				result.RetryCount,
				result.RetryDelay,
			)

			if shutdownDuringJob {
				return
			}

			continue
		}

		cleanupCtx, cancelCleanup := newCleanupContext()

		ackErr := ackJob(cleanupCtx, rdb, job)

		cancelCleanup()

		if ackErr != nil {
			log.Printf(
				"[%s] ACK order=%s failed: %v",
				workerID,
				job.OrderID,
				ackErr,
			)

			if shutdownDuringJob {
				return
			}

			continue
		}

		fmt.Printf(
			"[%s] ACK order=%s\n",
			workerID,
			job.OrderID,
		)

		if shutdownDuringJob {
			fmt.Printf(
				"[%s] shutdown complete after draining job=%s\n",
				workerID,
				job.JobID,
			)

			return
		}

	}
}

func newCleanupContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
}
