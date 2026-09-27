package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const (
	pendingQueue    = "jobs:orders:pending"
	processingQueue = "jobs:orders:processing"

	leaseSet      = "jobs:orders:leases"
	ownerSet      = "jobs:orders:owners"
	jobData       = "jobs:orders:data"
	retrySet      = "jobs:orders:retry"
	retryCountSet = "jobs:orders:retry-count"
	completedSet  = "jobs:orders:completed"
	effectCount   = "jobs:orders:effect-count"

	deadLetterQueue = "jobs:orders:dlq"

	leaseDuration     = 15 * time.Second
	heartbeatInterval = 5 * time.Second

	deferDelay = 3 * time.Second

	retryDelay     = 5 * time.Second
	baseRetryDelay = 1 * time.Second
	maxRetryDelay  = 30 * time.Second

	maxRetries = 5

	resourceLockTTL       = 10 * time.Second
	resourceLockHeartbeat = 3 * time.Second
)

var releaseLockScript = redis.NewScript(`
	if redis.call("GET", KEYS[1]) == ARGV[1] then
		return redis.call("DEL", KEYS[1])
	else
		return 0
	end
`)

var enqueueJobScript = redis.NewScript(`
    redis.call(
        "HSET",
        KEYS[1],
        ARGV[1],
        ARGV[2]
    )

    redis.call(
        "RPUSH",
        KEYS[2],
        ARGV[1]
    )

    return 1
`)

var claimJobScript = redis.NewScript(`
    local jobID = redis.call(
        "LPOP",
        KEYS[1]
    )

    if not jobID then
        return nil
    end

	local orderID = redis.call(
        "HGET",
        KEYS[5],
        jobID
    )

    if not orderID then
        return {
            "ERROR",
            jobID
        }
    end

	local redisTime = redis.call("TIME")
    local now = tonumber(redisTime[1])

    local leaseDuration =
        tonumber(ARGV[1])

    local leaseUntil =
		now + leaseDuration

    redis.call(
        "RPUSH",
        KEYS[2],
        jobID
    )

    redis.call(
        "ZADD",
        KEYS[3],
        leaseUntil,
        jobID
    )

	redis.call(
        "HSET",
        KEYS[4],
        jobID,
        ARGV[2]
    )

    return {
        jobID,
        orderID
    }
`)

var ackJobScript = redis.NewScript(`
	local currentOwner = redis.call(
        "HGET",
        KEYS[3],
        ARGV[1]
    )

	if not currentOwner then
        return 0
    end

	if currentOwner ~= ARGV[2] then
        return -1
    end

    local removed = redis.call(
        "LREM",
        KEYS[1],
        1,
        ARGV[1]
    )

    if removed == 0 then
        return 0
    end

    redis.call(
        "ZREM",
        KEYS[2],
        ARGV[1]
    )

	redis.call(
        "HDEL",
        KEYS[3],
        ARGV[1]
    )

    redis.call(
        "HDEL",
        KEYS[4],
        ARGV[1]
    )

    return 1
`)

var nackJobScript = redis.NewScript(`
    local currentOwner = redis.call(
        "HGET",
        KEYS[3],
        ARGV[1]
    )

    if not currentOwner then
        return {0, 0}
    end

    if currentOwner ~= ARGV[2] then
        return {-1, 0}
    end

    local removed = redis.call(
        "LREM",
        KEYS[1],
        1,
        ARGV[1]
    )

    if removed == 0 then
        return {-2, 0}
    end

    redis.call(
        "ZREM",
        KEYS[2],
        ARGV[1]
    )

    redis.call(
        "HDEL",
        KEYS[3],
        ARGV[1]
    )

    local retryCount = redis.call(
        "HINCRBY",
        KEYS[5],
        ARGV[1],
        1
    )

	local maxRetries = tonumber(ARGV[5])

	if retryCount > maxRetries then
        redis.call(
            "RPUSH",
            KEYS[6],
            ARGV[1]
        )

        return { retryCount, -1 }
    end

    local baseDelay = tonumber(ARGV[3])
    local maxDelay = tonumber(ARGV[4])

	local maxBackoff = baseDelay * (2 ^ (retryCount - 1))
	if maxBackoff > maxDelay then
		maxBackoff = maxDelay
	end

	local jitter = tonumber(ARGV[6])
	local retryDelay = math.floor(maxBackoff * jitter)
	if retryDelay < 1 then
		retryDelay = 1
	end

    local redisTime = redis.call("TIME")
    local now = tonumber(redisTime[1])

    local retryAt = now + retryDelay

    redis.call(
        "ZADD",
        KEYS[4],
        retryAt,
        ARGV[1]
    )

    return {
        retryCount,
        retryDelay
    }
`)

var recoverJobScript = redis.NewScript(`
    local leaseUntil = redis.call(
        "ZSCORE",
        KEYS[3],
        ARGV[1]
    )

    if not leaseUntil then
        return 0
    end

	local redisTime = redis.call("TIME")
    local now = tonumber(redisTime[1])

    if tonumber(leaseUntil) > now then
        return -1
    end

    local removed = redis.call(
        "LREM",
        KEYS[1],
        1,
        ARGV[1]
    )

     if removed == 0 then
        redis.call(
            "ZREM",
            KEYS[3],
            ARGV[1]
        )

        redis.call(
            "HDEL",
            KEYS[4],
            ARGV[1]
        )

        return 0
    end

    redis.call(
        "RPUSH",
        KEYS[2],
        ARGV[1]
    )

    redis.call(
        "ZREM",
        KEYS[3],
        ARGV[1]
    )

    redis.call(
        "HDEL",
        KEYS[4],
        ARGV[1]
    )

    return 1
`)

var renewLockScript = redis.NewScript(`
    local currentToken = redis.call(
        "GET",
        KEYS[1]
    )

    if not currentToken then
        return 0
    end

    if currentToken ~= ARGV[1] then
        return -1
    end

    redis.call(
        "EXPIRE",
        KEYS[1],
        ARGV[2]
    )

    return 1
`)

var renewLeaseScript = redis.NewScript(`
    local currentOwner = redis.call(
        "HGET",
        KEYS[1],
        ARGV[1]
    )

    if not currentOwner then
        return 0
    end

    if currentOwner ~= ARGV[2] then
        return -1
    end

	local redisTime = redis.call("TIME")
	local now = tonumber(redisTime[1])
	
	local leaseDuration =
        tonumber(ARGV[3])

	local newLeaseUntil = now + leaseDuration

    redis.call(
        "ZADD",
        KEYS[2],
        newLeaseUntil,
        ARGV[1]
    )

    return 1
`)

var promoteRetryJobScript = redis.NewScript(`
    local retryAt = redis.call(
        "ZSCORE",
        KEYS[1],
        ARGV[1]
    )

    if not retryAt then
        return 0
    end

    local redisTime = redis.call("TIME")
    local now = tonumber(redisTime[1])

    if tonumber(retryAt) > now then
        return -1
    end

    local removed = redis.call(
        "ZREM",
        KEYS[1],
        ARGV[1]
    )

    if removed == 0 then
        return 0
    end

    redis.call(
        "RPUSH",
        KEYS[2],
        ARGV[1]
    )

    return 1
`)

var deferJobScript = redis.NewScript(`
    local currentOwner = redis.call(
        "HGET",
        KEYS[3],
        ARGV[1]
    )

    if not currentOwner then
        return 0
    end

    if currentOwner ~= ARGV[2] then
        return -1
    end

    local removed = redis.call(
        "LREM",
        KEYS[1],
        1,
        ARGV[1]
    )

    if removed == 0 then
        return -2
    end

    redis.call(
        "ZREM",
        KEYS[2],
        ARGV[1]
    )

    redis.call(
        "HDEL",
        KEYS[3],
        ARGV[1]
    )

    local redisTime = redis.call("TIME")
    local now = tonumber(redisTime[1])

    local delay = tonumber(ARGV[3])
    local retryAt = now + delay

    redis.call(
        "ZADD",
        KEYS[4],
        retryAt,
        ARGV[1]
    )

    return delay
`)

var commitOrderScript = redis.NewScript(`
    local completed = redis.call(
        "SISMEMBER",
        KEYS[1],
        ARGV[1]
    )

    if completed == 1 then
        return 0
    end

    redis.call(
        "HINCRBY",
        KEYS[2],
        ARGV[1],
        1
    )

    redis.call(
        "SADD",
        KEYS[1],
        ARGV[1]
    )

    return 1
`)

func acquireLock(
	ctx context.Context,
	rdb *redis.Client,
	resourceID string,
	ownerToken string,
	ttl time.Duration,
) (bool, error) {

	key := "lock:" + resourceID

	return rdb.SetNX(
		ctx,
		key,
		ownerToken,
		ttl,
	).Result()
}

func releaseLock(
	ctx context.Context,
	rdb *redis.Client,
	resourceID string,
	token string,
) error {
	key := "lock:" + resourceID

	return releaseLockScript.Run(
		ctx,
		rdb,
		[]string{key},
		token,
	).Err()
}

func claimJob(
	ctx context.Context,
	rdb *redis.Client,
	workerID string,
) (*ClaimedJob, error) {

	claimToken := workerID + ":" + uuid.NewString()
	leaseDurationSeconds := int64(leaseDuration.Seconds())

	result, err := claimJobScript.Run(
		ctx,
		rdb,
		[]string{
			pendingQueue,
			processingQueue,
			leaseSet,
			ownerSet,
			jobData,
		},
		leaseDurationSeconds,
		claimToken,
	).Result()

	if errors.Is(err, redis.Nil) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	values, ok := result.([]interface{})
	if !ok || len(values) != 2 {
		return nil, fmt.Errorf(
			"unexpected claim result: %#v",
			result,
		)
	}

	jobID, ok := values[0].(string)
	if !ok {
		return nil, fmt.Errorf(
			"unexpected jobID: %#v",
			values[0],
		)
	}

	orderID, ok := values[1].(string)
	if !ok {
		return nil, fmt.Errorf(
			"unexpected orderID: %#v",
			values[1],
		)
	}

	return &ClaimedJob{
		JobID:   jobID,
		OrderID: orderID,
		Token:   claimToken,
	}, nil
}

func ackJob(
	ctx context.Context,
	rdb *redis.Client,
	job *ClaimedJob,
) error {

	result, err := ackJobScript.Run(
		ctx,
		rdb,
		[]string{
			processingQueue,
			leaseSet,
			ownerSet,
			jobData,
		},
		job.JobID,
		job.Token,
	).Int()

	if err != nil {
		return err
	}

	switch result {

	case 1:
		return nil

	case 0:
		return fmt.Errorf(
			"order=%s is no longer processing",
			job.OrderID,
		)

	case -1:
		return fmt.Errorf(
			"order=%s ownership lost",
			job.OrderID,
		)

	default:
		return fmt.Errorf(
			"unexpected ACK result=%d",
			result,
		)
	}
}

func recoverJob(
	ctx context.Context,
	rdb *redis.Client,
	jobID string,
) error {

	result, err := recoverJobScript.Run(
		ctx,
		rdb,
		[]string{
			processingQueue,
			pendingQueue,
			leaseSet,
			ownerSet,
		},
		jobID,
	).Int()

	if err != nil {
		return err
	}

	switch result {

	case 1:
		fmt.Printf(
			"[recovery] requeued job=%s\n",
			jobID,
		)

	case 0:
		return nil

	case -1:
		// lease ถูก renew หลังจาก scanner เจอ
		fmt.Printf(
			"[recovery] skip job=%s: lease renewed\n",
			jobID,
		)
	}

	return nil
}

func renewLock(
	ctx context.Context,
	rdb *redis.Client,
	resourceID string,
	token string,
	ttl time.Duration,
) (bool, error) {

	lockKey := "lock:" + resourceID

	result, err := renewLockScript.Run(
		ctx,
		rdb,
		[]string{
			lockKey,
		},
		token,
		int64(ttl.Seconds()),
	).Int()

	if err != nil {
		return false, err
	}

	switch result {
	case 1:
		return true, nil

	case 0:
		return false, nil

	case -1:
		return false, nil

	default:
		return false, fmt.Errorf(
			"unexpected renew lock result=%d",
			result,
		)
	}
}

func renewLease(
	ctx context.Context,
	rdb *redis.Client,
	job *ClaimedJob,
) (bool, error) {

	leaseDurationSeconds := int64(leaseDuration.Seconds())

	result, err := renewLeaseScript.Run(
		ctx,
		rdb,
		[]string{
			ownerSet,
			leaseSet,
		},
		job.JobID,
		job.Token,
		leaseDurationSeconds,
	).Int()

	if err != nil {
		return false, err
	}

	switch result {

	case 1:
		return true, nil

	case 0:
		return false, nil

	case -1:
		return false, nil

	default:
		return false, fmt.Errorf(
			"unexpected renew result=%d",
			result,
		)
	}
}

type NackResult struct {
	RetryCount int64
	RetryDelay int64
	DeadLetter bool
}

func nackJob(
	ctx context.Context,
	rdb *redis.Client,
	job *ClaimedJob,
) (*NackResult, error) {

	// jitter to avoid thundering herd problem
	jitter := rand.Float64()

	baseRetryDelaySeconds := int64(baseRetryDelay.Seconds())
	maxRetryDelaySeconds := int64(maxRetryDelay.Seconds())

	result, err := nackJobScript.Run(
		ctx,
		rdb,
		[]string{
			processingQueue,
			leaseSet,
			ownerSet,
			retrySet,
			retryCountSet,
			deadLetterQueue,
		},
		job.JobID,
		job.Token,
		baseRetryDelaySeconds,
		maxRetryDelaySeconds,
		maxRetries,
		jitter,
	).Int64Slice()

	if err != nil {
		return nil, err
	}

	code := result[0]

	switch code {

	case 0:
		return nil, fmt.Errorf(
			"job=%s has no owner",
			job.JobID,
		)

	case -1:
		return nil, fmt.Errorf(
			"job=%s ownership lost",
			job.JobID,
		)

	case -2:
		return nil, fmt.Errorf(
			"job=%s invariant violation",
			job.JobID,
		)
	}

	retryCount := result[0]
	retryDelay := result[1]

	if retryDelay == -1 {
		return &NackResult{
			RetryCount: retryCount,
			DeadLetter: true,
		}, nil
	}

	return &NackResult{
		RetryCount: retryCount,
		RetryDelay: retryDelay,
		DeadLetter: false,
	}, nil
}

func promoteRetryJob(
	ctx context.Context,
	rdb *redis.Client,
	jobID string,
) error {

	result, err := promoteRetryJobScript.Run(
		ctx,
		rdb,
		[]string{
			retrySet,
			pendingQueue,
		},
		jobID,
	).Int()

	if err != nil {
		return err
	}

	switch result {
	case 1:
		fmt.Printf(
			"[retry] promoted job=%s\n",
			jobID,
		)
		return nil

	case 0:
		return nil

	case -1:
		return nil

	default:
		return fmt.Errorf(
			"unexpected retry promotion result=%d",
			result,
		)
	}
}

func deferJob(
	ctx context.Context,
	rdb *redis.Client,
	job *ClaimedJob,
) error {

	result, err := deferJobScript.Run(
		ctx,
		rdb,
		[]string{
			processingQueue,
			leaseSet,
			ownerSet,
			retrySet,
		},
		job.JobID,
		job.Token,
		int64(deferDelay.Seconds()),
	).Int()

	if err != nil {
		return err
	}

	switch {
	case result > 0:
		return nil

	case result == 0:
		return fmt.Errorf(
			"cannot defer job=%s: owner not found",
			job.JobID,
		)

	case result == -1:
		return fmt.Errorf(
			"cannot defer job=%s: ownership lost",
			job.JobID,
		)

	case result == -2:
		return fmt.Errorf(
			"cannot defer job=%s: processing invariant violated",
			job.JobID,
		)

	default:
		return fmt.Errorf(
			"unexpected defer result=%d",
			result,
		)
	}
}

func commitOrderEffect(
	ctx context.Context,
	rdb *redis.Client,
	orderID string,
) (bool, error) {

	result, err := commitOrderScript.Run(
		ctx,
		rdb,
		[]string{
			completedSet,
			effectCount,
		},
		orderID,
	).Int()

	if err != nil {
		return false, err
	}

	switch result {
	case 1:
		return true, nil

	case 0:
		return false, nil

	default:
		return false, fmt.Errorf(
			"unexpected commit result=%d",
			result,
		)
	}
}
