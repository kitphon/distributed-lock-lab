# Reliable Queue with Redis Locks (PoC)

Lightweight proof-of-concept demonstrating a reliable job queue in Go using Redis for locking and persistence.

## Overview

- Producer pushes jobs onto a Redis-backed queue.
- Worker processes jobs and uses a Redis-based lock to claim work safely (`redis_wrapper.go`).
- Recovery and retry workers handle stuck or failed jobs to ensure eventual processing.

This repository is intended as an experimental lab to explore reliability patterns with Redis locks.

## Prerequisites

- Go 1.18+ installed
- Docker & Docker Compose (for running Redis locally)

## Quickstart

1. Start Redis:

```bash
docker-compose up -d
```

2. Run a worker (claims and processes jobs):

```bash
go run . worker
```

3. Run a producer (pushes jobs onto the queue):

```bash
go run . producer
```

4. Run background helpers if needed:

```bash
go run . recovery-worker   # rescues stuck jobs
go run . retry-worker      # retries failed jobs
```

Note: the exact command names accepted by the program mirror the file responsibilities; if an argument differs, check `main.go`.

## Project Structure

- `claim_job.go` — logic for claiming jobs with a Redis lock.
- `job.go` — job model definition.
- `job_handler.go` — code that processes a claimed job.
- `producer.go` — example producer that enqueues jobs.
- `worker.go` — main worker loop that claims and processes jobs.
- `recovery_worker.go` — rescues jobs that were claimed but not completed.
- `retry_worker.go` — retries jobs that have failed and are eligible for retry.
- `redis_wrapper.go` — small Redis helper and lock abstraction.
- `order.go` — helper types used in example jobs.
- `claim_job.go` — mechanics for claiming a job (locks, timeouts).
- `main.go` — program entrypoint and CLI/role dispatch.
- `docker-compose.yml` — docker compose file to run Redis for local development.
- `go.mod` — Go module file.

## How it works (high level)

1. The producer enqueues jobs into a Redis list or stream.
2. A worker polls the queue and attempts to claim a job by creating a short-lived Redis lock.
3. If the claim succeeds the worker processes the job and removes it from the queue (or marks it complete).
4. If the worker crashes or a job remains claimed beyond a safety window, the recovery worker reclaims the job for reprocessing.
5. Failed jobs may be pushed to a retry path and retried by the retry worker according to backoff/limits.

## Configuration

Edit `docker-compose.yml` to change Redis settings for local testing. For production use a managed Redis instance and tune lock timeouts and retry/backoff strategies.

## Development notes

- Preserve existing behavior if refactoring; this repo is an experiment and tests may be limited.
- Add instrumentation (metrics/logs) around claim/complete/retry operations to measure reliability.

## License

This PoC is provided as-is for experimentation.
