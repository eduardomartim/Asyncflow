# AsyncFlow

A backend service for running background tasks reliably. A client submits a task over
HTTP and gets an id back immediately. A separate worker process runs the task, retries
transient failures with exponential backoff, and moves tasks that keep failing to a
dead-letter queue where they can be inspected instead of being lost.

Built with FastAPI, PostgreSQL (async SQLAlchemy + Alembic) and RabbitMQ (aio-pika).

## The problem it solves

"Save to the database, then publish to the queue" has a gap: if the process dies
between the two steps, the task exists but is never processed (or the message exists
but the task doesn't). AsyncFlow closes that gap with the **transactional outbox**
pattern: the task row and an `outbox_events` row are written in the same database
transaction, and a relay publishes the outbox row to RabbitMQ afterwards. Nothing is
published that wasn't committed, and nothing committed is left unpublished.

On top of that it handles the usual consequences of asynchronous processing:

- **Duplicate submissions**: an optional `idempotency_key`, enforced by a unique index.
- **Duplicate deliveries**: the worker re-reads the task from Postgres before acting
  and skips tasks that are already finished.
- **Transient failures**: retries with exponential backoff, implemented with RabbitMQ
  message TTLs rather than sleeping in the worker.
- **Permanent failures**: the task is marked `dead_lettered` and a notice is placed on
  the `tasks.dead_letter` queue.

## Architecture

```
 client
   | POST /tasks
   v
 FastAPI ──(one transaction)──> tasks row (status=queued)
                                 + outbox_events row "task_created"
   |
 Outbox relay (runs inside the API process AND inside the worker process;
   |           each pending row is claimed with FOR UPDATE SKIP LOCKED)
   v
 exchange "tasks" ──> queue "tasks" <──────────────────────────────┐
                          |                                         | per-message TTL
                          v                                         | expires, DLX routes
 Worker: re-read task in Postgres, skip if already finished,        | it back to "tasks"
         run the handler for task_type                              |
   |-- success ─────────> status=succeeded + outbox "task_succeeded" (audit only)
   |-- transient error ─> status=retrying ──> queue "tasks.retry" ──┘
   |                      (2s, 4s, 8s, 16s ... capped)
   └-- permanent error or attempts used up
                        ─> status=dead_lettered + outbox "task_dead_lettered"
                             └─ relay ──> exchange "dead_letter" ──> queue "tasks.dead_letter"
```

| Component | Role |
|---|---|
| **API** (`app/api/`) | `POST /tasks`, `GET /tasks`, `GET /tasks/{id}`, `/health`. Writes the task and its outbox event in one transaction. |
| **PostgreSQL** | Source of truth for task state (`tasks`) and for events that still have to reach the broker (`outbox_events`). |
| **Outbox relay** (`app/outbox/relay.py`) | Publishes pending outbox events to RabbitMQ and marks them `published` only after the broker confirmed the publish. |
| **RabbitMQ** (`app/mq/`) | `tasks` (work queue), `tasks.retry` (delay queue, TTL + dead-letter exchange back to `tasks`), `tasks.dead_letter` (terminal queue). All durable, messages persistent. |
| **Worker** (`app/workers/worker.py`) | Consumes `tasks`, runs the handler, records the outcome, schedules retries, dead-letters. Runs its own outbox relay. |
| **Dead-letter queue** | `tasks.dead_letter` holds one notice per dead-lettered task (`task_id`, `task_type`, `reason`). It is also the queue-level DLX of `tasks`, so a message rejected by an unexpected error lands there instead of being dropped. |
| **Migrations** (`migrations/`) | Alembic is the only thing that creates or changes the schema. |

### Outbox event types

| Event | Written by | What the relay does | Final row state |
|---|---|---|---|
| `task_created` | API, with the task | publishes to the `tasks` queue | `status=published`, `published=true` |
| `task_dead_lettered` | worker, with the `dead_lettered` status | publishes the notice to `tasks.dead_letter` | `status=published`, `published=true` |
| `task_succeeded` | worker, with the `succeeded` status | nothing to publish (no consumer exists) | `status=recorded`, `published=false` |

`published=true` is only set after `exchange.publish` returned on a channel with
publisher confirms enabled (aio-pika's default), so it means RabbitMQ acknowledged the
message. If a publish raises, the row stays `pending`, its `attempts` counter goes up,
and the relay moves on to the next row. After `max_attempts` (3) failed publishes the
row becomes `status=failed` and is no longer retried. Failed rows need manual
attention; nothing re-drives them automatically.

The dead-letter notice goes through the outbox, not straight from the worker, so it
can't be lost if the worker crashes right after committing `dead_lettered`.

## Delivery guarantees

- **At-least-once, not exactly-once.** A relay can publish an event twice if it
  crashes after RabbitMQ confirmed the publish but before the database commit that
  marks the row `published`. RabbitMQ redelivers unacknowledged messages after a
  worker crash.
- **Duplicates are harmless for tasks**: the worker skips a task whose status is
  already terminal (`succeeded`, `failed`, `dead_lettered`, `cancelled`). A duplicate
  dead-letter notice is possible in the crash window above; consumers of
  `tasks.dead_letter` should de-duplicate by `task_id`.
- **Concurrent relays don't double-publish**: each row is claimed with
  `SELECT ... FOR UPDATE SKIP LOCKED` in its own transaction.
- **Idempotent creation**: `idempotency_key` is backed by a unique index. Two
  concurrent requests with the same key can't create two tasks; the loser gets the
  existing task back (HTTP 200).

## Running it locally (Windows)

Prerequisites: Python 3.11+ (developed and tested on 3.14), Docker Desktop, and
Windows PowerShell. All commands below were run in Windows PowerShell 5.1 from the
project directory.

### 1. Configure the environment

```powershell
Copy-Item .env.example .env
```

The app reads its settings from environment variables or `.env`, and
`docker-compose.yml` reads the same `.env` for the container credentials and ports, so
the two stay in sync. The defaults (`change_me`, `guest`/`guest`) are for local
development only. Postgres and RabbitMQ apply credentials only when their volume is
first created, so changing them later requires changing them inside the running
services too.

| Variable | Default | Meaning |
|---|---|---|
| `POSTGRES_HOST` / `POSTGRES_PORT` | `localhost` / `5432` | Database address |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB` | `asyncflow` / `change_me` / `asyncflow` | Database credentials |
| `RABBITMQ_HOST` / `RABBITMQ_PORT` | `localhost` / `5672` | Broker address |
| `RABBITMQ_USER` / `RABBITMQ_PASSWORD` | `guest` / `guest` | Broker credentials |
| `RABBITMQ_HTTP_PORT` | `15672` | Host port of the RabbitMQ management UI (docker-compose only) |
| `API_HOST` | `127.0.0.1` | Interface the `asyncflow-api` script listens on; the API has no authentication, so use `0.0.0.0` only on a trusted network |
| `MAX_TASK_RETRIES` | `5` | **Total attempts** per task, despite the name: 1 first try + 4 retries |
| `RETRY_BACKOFF_BASE_SECONDS` / `RETRY_BACKOFF_MAX_SECONDS` | `2.0` / `60.0` | Retry *n* waits `min(base * 2^(n-1), max)` seconds |
| `WORKER_CONCURRENCY` | `5` | Messages processed concurrently per worker (RabbitMQ prefetch) |
| `GRACEFULLY_SHUTDOWN_TIMEOUT_SECONDS` | `30` | How long a stopping worker waits for in-flight tasks |

`.env` entries are validated strictly: an unknown variable in `.env` (for example a
`SECRET_KEY` line left over from an older version) stops the app at startup with
`Extra inputs are not permitted`. Remove such lines.

### 2. Start PostgreSQL and RabbitMQ

```powershell
docker compose up -d
docker compose ps    # wait until both show "(healthy)"
```

Data is kept in the named volumes `asyncflow_postgres_data` and
`asyncflow_rabbitmq_data`; `docker compose up -d` again does not wipe them. Postgres
(5432), RabbitMQ (5672) and the management UI (15672) are published on `127.0.0.1` only,
because they use the development credentials above. RabbitMQ's
management UI is at `http://localhost:15672` (guest/guest).

### 3. Install

```powershell
python -m pip install -e ".[dev]"
```

Installs the `app` package in editable mode plus the dev tools (`pytest`,
`pytest-asyncio`, `httpx`, `ruff`, `mypy`).

### 4. Create or upgrade the schema

```powershell
alembic upgrade head
```

The API, the worker and the tests never create tables. At startup they read
`alembic_version` and refuse to start if the database isn't at the latest revision
(`SchemaNotMigratedError: database schema revision [...] != expected
['6c10892b4578']; run `alembic upgrade head` first`).
Re-run this command after pulling new migrations. It is safe on an existing database:
it only applies revisions that aren't there yet.

Revision chain: `0b5e1a7c2d3f` (baseline tables) → `fc46ce9cb5a6` → `6c10892b4578`
(head). The baseline revision exists because `fc46ce9cb5a6` was originally
autogenerated against tables created outside Alembic and failed on an empty database;
databases already at `fc46ce9cb5a6` or later are unaffected.

### 5. Start the API and a worker (two terminals)

```powershell
python -m uvicorn app.api.main:app --host 127.0.0.1 --port 8000
```

```powershell
python -m app.workers.worker
```

The installed console scripts `asyncflow-api` and `asyncflow-worker` do the same
(note: `asyncflow-api` binds `127.0.0.1:8000` unless `API_HOST` is set). Stop the worker with Ctrl+C: it stops
taking new messages, waits up to `GRACEFULLY_SHUTDOWN_TIMEOUT_SECONDS` for running
tasks, then exits. (On Windows `asyncio.run` delivers Ctrl+C as a cancellation of the
main coroutine; `serve_until_stopped` in `app/workers/worker.py` turns that into the
same graceful stop.)

Check it's up:

```powershell
curl.exe http://127.0.0.1:8000/health
# {"status":"ok","database":"ok","rabbitmq":"ok"}
```

Interactive API docs: `http://127.0.0.1:8000/docs`.

## Using the API

`task_type` and `idempotency_key` are query parameters; the JSON body is the task's
payload and must be a JSON object. The `Content-Type: application/json` header is
required (without it the API answers 422). `task_type` must match
`[a-zA-Z0-9_.-]{1,100}`.

```powershell
curl.exe -X POST "http://127.0.0.1:8000/tasks?task_type=flaky&idempotency_key=demo-001" `
  -H "Content-Type: application/json" `
  -d '{\"fail_until_attempt\": 2}'
```

Returns HTTP 201:

```json
{"id":"bb248ef8-...","status":"queued","task_type":"flaky","idempotency_key":"demo-001","message":"Task submitted successfully"}
```

Sending the same `idempotency_key` again returns the existing task with HTTP 200 and
`"message":"Task already submitted with this idempotency key"`.

```powershell
curl.exe "http://127.0.0.1:8000/tasks/<id>"                         # one task
curl.exe "http://127.0.0.1:8000/tasks?status=succeeded&page=1&size=20"  # list, filtered
```

### Demo task types

The worker registers two handlers so retries and dead-lettering can be reproduced
deterministically (they depend on the attempt number, not on timing or randomness):

- `flaky`: payload `{"fail_until_attempt": N}` fails transiently on attempts before
  `N`, then succeeds.
- `always_fail`: raises a permanent error on the first attempt.
- Any other `task_type` succeeds immediately with a placeholder result.

### Reproduce success, retry and dead-lettering

With the API and a worker running and the default settings:

```powershell
# Succeeds on the 3rd attempt, after waiting 2s and 4s:
curl.exe -X POST "http://127.0.0.1:8000/tasks?task_type=flaky" -H "Content-Type: application/json" -d '{\"fail_until_attempt\": 3}'

# Fails transiently on all 5 attempts (waits 2s, 4s, 8s, 16s), then dead-letters (~30s):
curl.exe -X POST "http://127.0.0.1:8000/tasks?task_type=flaky" -H "Content-Type: application/json" -d '{\"fail_until_attempt\": 99}'

# Permanent error: dead-lettered at once, no retries:
curl.exe -X POST "http://127.0.0.1:8000/tasks?task_type=always_fail" -H "Content-Type: application/json" -d '{}'
```

Poll `GET /tasks/<id>` to follow `status` and `attempts`. Observed with these exact
commands:

| Task | Final status | `attempts` / `max_attempts` | `error` |
|---|---|---|---|
| `flaky`, `fail_until_attempt: 3` | `succeeded` | 2 / 5 | — |
| `flaky`, `fail_until_attempt: 99` | `dead_lettered` | 5 / 5 | `retries exhausted (5/5): simulated transient failure on attempt 5` |
| `always_fail` | `dead_lettered` | 0 / 5 | `simulated permanent failure` |

For the exhausted task, `attempts` went 1 → 2 → 3 → 4 → `dead_lettered` at about
+2.1s, +4.0s, +8.3s and +16.0s apart. Queue depths for `tasks`, `tasks.retry` and
`tasks.dead_letter` can be watched in the management UI.

## Tests

```powershell
docker compose up -d       # the tests use the real Postgres and RabbitMQ
alembic upgrade head       # the suite refuses to run against an unmigrated database
python -m pytest tests/ -v
```

25 tests. All of them talk to the real containers except the 3 in
`test_constants.py`; nothing is mocked.

| File | What it proves |
|---|---|
| `test_constants.py` | Task state machine: allowed transitions, which error categories are retryable. |
| `test_api_tasks.py` | Create (201), validation (422 for bad `task_type` / non-object payload), idempotent create (200 + same id), get, 404, list filtered by status with pagination. |
| `test_outbox_and_worker.py` | Task and outbox event in one transaction; relay publishes `task_created` to the real `tasks` queue (matched by task_id); end-to-end processing to `succeeded` (and `task_succeeded` ends `recorded`, not `published`); redelivery of a finished task is a no-op; retry then success; **permanent failure** → `dead_lettered`, `attempts=0`; **retries exhausted** → `max_attempts=3`, backoff 0.2s/0.4s, at least 0.6s elapsed, `attempts=3`, `error="retries exhausted (3/3): ..."`, `tasks.retry` empty. Both dead-letter tests check that the `task_dead_lettered` event goes `pending` → `published` and that the notice for that task_id is on `tasks.dead_letter`. |
| `test_outbox_delivery.py` | A failing publish doesn't raise out of the relay, doesn't block the next event in the batch, increments `attempts`, and ends `failed` after 3 tries; two relays running concurrently publish 20 events exactly once each; audit-only events become `recorded` with nothing sent; a dead-letter event without `task_type` in its payload is published with the task's type. |
| `test_worker_shutdown.py` | `stop()` during a running task, and a Ctrl+C-style cancellation (what `asyncio.run` does on Windows): in both cases the worker waits for the task, it ends `succeeded`, and its message is acknowledged. |
| `test_migrations.py` | On a throwaway database (created and dropped by the test): the startup guard rejects it, `alembic upgrade head` builds the schema, `alembic check` finds no drift; downgrading one revision and upgrading again works; downgrading below `fc46ce9cb5a6` fails and leaves the database at head. |

Tests clean up after themselves: rows whose `task_type` starts with `test_`, plus rows
registered by id for tests that need the `flaky`/`always_fail` handlers. Queue
assertions look messages up by task_id and put other messages back untouched, so
existing data and messages are left alone.

Don't run the API/worker and the test suite at the same time: both would consume the
same `tasks` queue.

## Code quality

```powershell
python -m ruff check .
```

Reports `All checks passed!`. Ruff is configured in `pyproject.toml` (line length 100,
rule sets `E`, `F`, `I`, `UP`, `B`, `ARG`). Only `B008` is disabled, because
`Depends(...)` as a default argument is how FastAPI declares dependencies.

`mypy` is listed as a dev dependency but has **not** been run successfully: on the
development machine Windows Application Control blocks its compiled modules
(`ImportError: DLL load failed while importing fscache`). No type-checking result is
claimed.

## Status: implemented, tested, observed

| Capability | Implemented | Automated test | Observed with running API + worker |
|---|---|---|---|
| Schema built from an empty database by Alembic alone | Yes | Yes | — |
| Task create / get / list, validation | Yes | Yes | Yes |
| Idempotent create (same key → same task, HTTP 200) | Yes | Yes | Yes |
| Task + outbox event in one transaction | Yes | Yes | Yes |
| Relay publishes `task_created` to RabbitMQ | Yes | Yes | Yes |
| Relay: failed publish retried, then `failed`; batch not blocked | Yes | Yes | No |
| Two relays don't publish the same event twice | Yes | Yes | No (both relays ran, no duplicates seen) |
| Worker processes to `succeeded` | Yes | Yes | Yes |
| Redelivered finished task is skipped | Yes | Yes | No |
| Retry with exponential backoff | Yes | Yes | Yes (≈2s / 4s / 8s / 16s) |
| Dead-letter after retries exhausted | Yes | Yes | Yes (5/5 attempts) |
| Dead-letter on permanent error | Yes | Yes | Yes |
| Dead-letter notice delivered via outbox | Yes | Yes | Yes (published by the worker's relay) |
| Graceful worker shutdown (drain in-flight task) | Yes | Yes (in-process `stop()` and simulated Ctrl+C cancellation) | No (no real Ctrl+C sent to a running process) |
| Recovery after an actual process crash | Partial (relies on redelivery + idempotency) | No | No |

## Known limitations

- **At-least-once delivery.** Duplicated messages are possible (see above); task
  processing tolerates them, dead-letter consumers must de-duplicate by `task_id`.
- **No crash test.** No test kills a real process mid-flight. The mechanisms that
  handle it (redelivery of unacked messages, idempotent worker, outbox) are tested;
  the crash itself is not.
- **A failed retry publish leaves the task in `retrying`** (from reading the code; not
  covered by a test). If publishing to `tasks.retry` raises (broker unavailable), the
  original message is rejected and, through the `tasks` queue's DLX, ends on
  `tasks.dead_letter` as a raw task message, while the task row stays `retrying`.
- **Outbox rows marked `failed` are not re-driven** automatically.
- **No outbox cleanup.** `outbox_events` grows indefinitely.
- **Windows signals.** Ctrl+C drains the worker; `Stop-Process`/Task Manager kill it
  without draining (unacked messages are redelivered, so nothing is lost, but running
  tasks are cut off and re-run). Only the simulated Ctrl+C path is tested.
- **Downgrades below `fc46ce9cb5a6` are not supported**: that revision's downgrade
  casts text ids/keys back to `uuid`, which Postgres rejects. One-step downgrade to
  `fc46ce9cb5a6` works.
- `alembic check` compares tables, columns, types and indexes but not server defaults
  (Alembic's default). The database has server defaults (`status` `'queued'` /
  `'pending'`, counters `0`) that the ORM models only set on the Python side.
- mypy has not been run (see Code quality).

## Before exposing it publicly

This is a portfolio/learning project. It has not been load-tested, deployed or used in
production. Before putting it on a network you don't control, it would need at least:

1. **Authentication and authorization** on every endpoint. There is none: anyone who
   can reach the API can submit tasks and read every task's payload and result.
2. **Real credentials** for Postgres and RabbitMQ (not `change_me` / `guest`),
   supplied through a secret store, and TLS for both connections.
3. **Rate limiting and request size limits** on `POST /tasks`.
4. **Idempotency-key expiry** if keys are meant to be reusable. Today a key is
   reserved forever by its task.
5. **Operational pieces** this project leaves out: metrics and alerting on queue
   depth / DLQ / `failed` outbox rows, outbox and task retention, a way to re-drive
   dead-lettered tasks, and running `alembic upgrade head` as a deliberate deployment
   step.
6. **A process manager that stops workers with SIGINT/SIGTERM (Linux) or Ctrl+C**, not
   a hard kill, so workers drain on shutdown.

## Project layout

```
app/
  api/            FastAPI app, routes, request/response schemas
  config/         Settings (pydantic-settings)
  core/           enums/state machine, exceptions, JSON helpers
  db/             engine/session, ORM models, repositories, schema guard
  mq/             RabbitMQ connection, topology, publisher
  outbox/         outbox relay
  workers/        worker and demo handlers
migrations/       Alembic environment and revisions
tests/            integration and unit tests
docker-compose.yml, alembic.ini, pyproject.toml, .env.example
```
