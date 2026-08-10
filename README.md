# Payload Service

Asynchronous REST service for text and file payloads (documents, images, video).
Uploads stream straight into object storage, a durable job queue hands them to
an external processing service, and the whole path is traced, logged and
measured.

```
                  ┌──────────────┐
   client ───────▶│  API (chi)   │──── stream ────▶ object storage (S3/MinIO)
                  │  APP_MODE=api│
                  └──────┬───────┘
                         │ INSERT job (pending)
                         ▼
                  ┌──────────────┐
                  │  PostgreSQL  │  queue: FOR UPDATE SKIP LOCKED + lease
                  └──────┬───────┘
                         │ claim batch
                         ▼
                  ┌──────────────┐   presigned GET URL   ┌──────────────────┐
                  │   Worker     │──────────────────────▶│ external processor│
                  │APP_MODE=worker│◀──────  summary  ─────│    (mocked)      │
                  └──────┬───────┘                       └──────────────────┘
                         │
                         ▼
              OTLP traces → Collector → Jaeger
              /metrics     → Prometheus → Grafana
```

## Quick start

```bash
make compose-up     # full stack: api, worker, postgres, minio, mock, observability
make smoke          # submits jobs and polls them to completion
```

| | |
|---|---|
| API | http://localhost:8080 |
| Grafana (dashboard provisioned) | http://localhost:3000 |
| Prometheus | http://localhost:9090 |
| Jaeger | http://localhost:16686 |
| MinIO console | http://localhost:9001 (`minioadmin` / `minioadmin`) |

```bash
# text only
curl -s -X POST http://localhost:8080/api/v1/jobs -F 'text=hello world'

# document / image / video
curl -s -X POST http://localhost:8080/api/v1/jobs -F 'text=report' -F 'file=@doc.pdf'
curl -s -X POST http://localhost:8080/api/v1/jobs -F 'file=@photo.png'
curl -s -X POST http://localhost:8080/api/v1/jobs -F 'file=@clip.mp4'

# poll
curl -s http://localhost:8080/api/v1/jobs/<id>
```

`POST` returns `202 Accepted` with a `Location` header and the job in `pending`.
Full contract: [api/openapi.yaml](api/openapi.yaml).

Retrying a submission is safe if you send a key — the original job comes back
instead of a duplicate, flagged with `Idempotency-Replayed: true`:

```bash
curl -s -X POST http://localhost:8080/api/v1/jobs \
  -H 'Idempotency-Key: order-4711' -F 'file=@invoice.pdf'
```

Set `API_KEYS` to require authentication on `/api/v1`
(`Authorization: Bearer <key>` or `X-API-Key: <key>`); probes and `/metrics`
stay open so kubelet and Prometheus keep working.

## Requirements mapping

| Requirement | Where |
|---|---|
| REST API | `POST /api/v1/jobs`, `GET /api/v1/jobs/{id}` |
| Text payload | multipart field `text` |
| File upload (document / image / video) | multipart `file`, MIME allowlist + magic-byte verification |
| Scalable — Docker instances | api and worker tiers behind nginx, `--scale api=N --scale worker=M` |
| Scalable — k8s replicas | Deployments, HPA, PDB, topology spread, KEDA queue-depth scaling |
| Observability | structured logs with trace ids, OTLP traces, Prometheus metrics, Grafana dashboard |
| Payload processing replaced by a mock external service | `cmd/mockprocessor`, reached over HTTP with a presigned URL |
| Git project | this repository |

## Scaling

Both tiers are stateless; all coordination happens in Postgres.

```bash
make compose-scale API=3 WORKERS=4
docker compose -f deploy/compose/docker-compose.yml ps
```

The API publishes no host port — nginx fans out across replicas and re-resolves
Docker DNS every 5s, so scaling needs no restart. Prometheus discovers replicas
the same way, so `payload_jobs_claimed_total` splits across workers as they
join.

On Kubernetes:

```bash
make k8s-deploy      # builds the image and applies deploy/k8s/
```

Manifests include a namespace, dev Postgres/MinIO/Jaeger/Collector, a migration
Job, the api and worker Deployments, Services, HPAs, PDBs, probes, resource
limits, `securityContext` and topology spread constraints.

**Autoscaling workers on the right signal.** Workers are I/O bound — they wait
on object storage and on the processor — so CPU barely moves while the backlog
grows. `payload_jobs_pending` is the metric that tracks real demand, and
[deploy/k8s/06-keda-worker.yaml](deploy/k8s/06-keda-worker.yaml) scales on it
via KEDA. The CPU HPA stays in the default manifests as the portable fallback.

## Design decisions

**Why asynchronous?** A 200 MiB video upload must not hold an HTTP connection
open through the load balancer while it is processed. The API's only job is to
land the bytes and record the work; `202` plus a polling endpoint keeps every
replica responsive.

**Why Postgres as the queue instead of Kafka/NATS?** At this volume `SELECT …
FOR UPDATE SKIP LOCKED` gives competing consumers, exactly-once claim and
durable retries with one dependency we already run, and it is transactional
with the job row itself. A broker is the right move once throughput outgrows a
single database — that is a swap of `JobRepository`, not a rewrite.

**Delivery semantics: at-least-once.** A claim stamps `locked_by` and a lease.
A worker that dies has its lease expire and the reaper requeues the job.
Transient failures back off exponentially (`next_attempt_at`), permanent ones
(`4xx` from the processor) fail immediately instead of burning the retry
budget, and `MAX_ATTEMPTS` bounds both. The mock processor is idempotent on
`job_id`, which is what makes at-least-once safe.

**Why does the external processor get a presigned URL?** It never receives our
credentials and never proxies bytes through our API. It fetches the object
directly from storage with a short-lived signed GET, the same way a real
third-party service would.

**Why not trust `Content-Type`?** It is attacker-controlled. Every accepted
type carries a magic-byte verifier: `%PDF-` for PDF, an ISO base media box for
MP4/MOV, EBML for WebM, `PK\x03\x04` for OOXML, OLE2 for legacy `.doc`, plus
the image signatures. Declaring `video/mp4` over an ELF binary is rejected with
`415` and never reaches storage — [there is a test for
exactly that](internal/api/http/handlers_test.go).

For video the check validates the box size and accepts any of the top-level
atoms QuickTime may open with, not just `ftyp`. Requiring `ftyp` alone is the
tempting version and it rejects legitimate `.mov` files; validating the size
field keeps the wider type list from degrading into "anything with four bytes
at offset 4".

**How is upload memory bounded?** Unknown-length bodies (the normal case for
`multipart/form-data`) are read one part at a time into pooled buffers; small
payloads become a single `PutObject`, larger ones a concurrent multipart
upload. Worst-case resident memory is
`S3_PART_SIZE × (1 + 2 × S3_PART_CONCURRENCY) × MAX_CONCURRENT_UPLOADS` —
≈1.1 GiB with the defaults, which is what the api container's memory limit is
sized against. The service logs this number at startup. Beyond that limit it
sheds load with `503` and `Retry-After` rather than being OOM-killed.

**Why is submission idempotent?** A client whose request times out cannot tell
whether the job was created, so retrying is the natural thing to do — and
without a key that silently doubles the work. `Idempotency-Key` is enforced by
a partial unique index rather than a read-then-write check, so two concurrent
retries cannot both win. A lookup before reading the body is the fast path that
avoids re-uploading the payload; the loser of a race deletes the object it just
wrote.

**Why is auth a shared key?** The assignment does not ask for authentication,
but an unguarded ingest endpoint invites the question. `API_KEYS` is the
smallest thing that closes it: keys are compared as SHA-256 digests in constant
time, and every key is checked so the timing does not reveal which one matched.
A real deployment terminates OAuth or mTLS at the gateway and passes identity
down — that replaces this middleware without touching anything else.

**Object then row, never the reverse.** The row is inserted after the upload
succeeds, so a job never references an object that does not exist. If the
insert fails, the object is deleted; if the request is abandoned mid-flight,
the deferred `Discard` removes it.

**Graceful shutdown order.** Readiness starts failing first, then `DRAIN_DELAY`
gives the load balancer time to remove the pod, then the HTTP server drains,
then the worker stops. Jobs already claimed but not yet started are released
back to `pending` so another replica takes them immediately instead of waiting
out the lease, and results are written on a context that survives cancellation.

**Migrations.** Run once by a Kubernetes Job before rollout, and guarded by a
`pg_advisory_lock` in the application besides, so replicas starting together
cannot race inside goose's version table.

## Observability

| Signal | Where |
|---|---|
| Logs | JSON on stdout, every line carrying `trace_id` / `span_id` / `request_id` |
| Traces | OTLP → Collector → Jaeger; one trace covers the API, the worker and the external processor |
| Metrics | `/metrics` on every pod, scraped by Prometheus, dashboard provisioned in Grafana |

The ingest span and the worker span belong to the same trace: the W3C
traceparent is persisted on the job row and restored when it is claimed, so one
Jaeger view spans an upload that happened minutes before its processing. A
single trace looks like this:

```
payload-service [api]      POST /api/v1/jobs
payload-service [api]      JobService.Commit
payload-service [worker]   Worker.ProcessJob
payload-service [worker]   Processor.Process
payload-service [worker]   HTTP POST            ─┐ crosses the service boundary
mock-processor  [mock]     POST /v1/process     ─┘
```

Key metrics: `payload_http_requests_total` and
`payload_http_request_duration_seconds` (RED), `payload_jobs_pending` (queue
depth, the autoscaling signal), `payload_jobs_claimed_total`,
`payload_jobs_retried_total`, `payload_jobs_reaped_total`,
`payload_uploads_in_flight`, `payload_uploads_rejected_total`,
`payload_s3_upload_duration_seconds`.

`/healthz` is liveness (process only). `/readyz` is readiness and checks
Postgres and object storage, so a pod that lost a dependency stops receiving
traffic without being restarted.

## Tests

```bash
make test              # unit + race detector, no external dependencies
make test-integration  # spins up a throwaway Postgres and runs everything
make cover
```

- `internal/storage/s3` drives the real upload code against an in-process S3
  API stub: multipart reassembly is verified byte-for-byte, and a failing part
  or a client that hangs up mid-body must abort the upload.
- `internal/storage/postgres` runs against a real Postgres: eight goroutines
  racing for the same rows must partition the queue exactly once, backoff must
  make a job unclaimable, expired leases must be requeued, and concurrent
  migrations must serialise.
- `test/e2e` wires the real handler, service, repository and worker together
  and drives them through the public HTTP surface — including recovery from a
  flaky processor, giving up after `MAX_ATTEMPTS`, and reaping a job abandoned
  by a dead worker.

`make check` runs `gofmt`, `go mod tidy`, lint and the unit suite with `-race`.
For the full suite against Postgres use `make test-integration`; manifests can
be validated with `kubeconform` over `deploy/k8s/`.

## Configuration

Every setting is an environment variable with a default; see
[.env.example](.env.example). Invalid combinations are rejected at startup with
a specific message rather than failing later under load — for example an
`S3_PART_SIZE` below the 5 MiB S3 minimum, or a `WORKER_LEASE` short enough
that the reaper would steal jobs from live workers.

`APP_MODE` selects which halves run: `api`, `worker` or `all`. One binary, one
image; Kubernetes splits the tiers.

## Layout

```
cmd/server           entrypoint; wiring and lifecycle
cmd/mockprocessor    stand-in for the external processing service
internal/domain      entities and ports (no dependencies)
internal/service     use cases; the multipart Draft lives here
internal/api/http    handlers, routing, MIME verification, middleware
internal/worker      poller, workers, heartbeat, reaper
internal/storage     PostgreSQL and S3 adapters
internal/processor   HTTP client for the external service
internal/observability logging, tracing, metrics
deploy/              Dockerfile, compose stack, Kubernetes manifests
test/e2e             end-to-end pipeline tests
```

Dependencies point inward: `domain` defines the interfaces, adapters implement
them, and nothing in the core imports a driver or an SDK.

## Deliberately out of scope

Multi-tenancy and per-tenant quotas, a message broker, object lifecycle/TTL
cleanup for terminal jobs, multi-file batches, and real ML/OCR processing —
the last of which the assignment explicitly asks to mock.

---

## На русском

Асинхронный REST-сервис для текстовых и файловых payload (документы, изображения,
видео). API стримит загрузку в object storage (S3/MinIO), ставит задачу в очередь
на PostgreSQL (`FOR UPDATE SKIP LOCKED` + lease), worker отдаёт объект во внешний
процессор по короткоживущему presigned URL. Наблюдаемость: JSON-логи с
`trace_id`, OTLP → Jaeger, Prometheus → Grafana.

### Быстрый старт

```bash
make compose-up   # поднять весь стек
make smoke        # отправить jobs и дождаться completed
make help         # список всех make-таргетов
```

| Сервис | URL |
|---|---|
| API | http://localhost:8080 |
| Grafana | http://localhost:3000 |
| Prometheus | http://localhost:9090 |
| Jaeger | http://localhost:16686 |
| MinIO | http://localhost:9001 (`minioadmin` / `minioadmin`) |

```bash
curl -s -X POST http://localhost:8080/api/v1/jobs -F 'text=hello world'
curl -s -X POST http://localhost:8080/api/v1/jobs -F 'file=@photo.png'
curl -s http://localhost:8080/api/v1/jobs/<id>
```

`POST` отвечает `202 Accepted` + заголовок `Location`, статус `pending`.
Контракт: [api/openapi.yaml](api/openapi.yaml).

### Команды Makefile

| Команда | Что делает |
|---|---|
| `make help` | Список доступных таргетов |
| `make build` | Собрать `bin/server` и `bin/mockprocessor` |
| `make docker-build` | Собрать Docker-образ |
| `make run` | Запустить сервер локально (зависимости уже должны быть подняты) |
| `make test` | Юнит-тесты с `-race`, без внешних зависимостей |
| `make test-integration` | Полный suite против throwaway Postgres (нужен Docker) |
| `make test-e2e` | Только e2e (`TEST_DATABASE_URL`) |
| `make cover` | Отчёт покрытия |
| `make lint` | `golangci-lint` (или `go vet`) |
| `make fmt` | `gofmt -w .` |
| `make tidy` | `go mod tidy` |
| `make check` | То, что гоняет CI: fmt-check + tidy-check + lint + test |
| `make compose-up` | Поднять полный compose-стек |
| `make compose-scale API=3 WORKERS=4` | Масштабировать api/worker |
| `make compose-logs` | Логи api и worker |
| `make compose-down` | Остановить стек и удалить volumes |
| `make smoke` | Smoke-тест против живого стека |
| `make k8s-deploy` | Собрать образ и применить `deploy/k8s/` |
| `make k8s-delete` | Удалить k8s-манифесты |
| `make clean` | Удалить `bin/` и `coverage.out` |

### Ключевые решения

- **Асинхронность** — тяжёлый upload не держит HTTP-соединение до конца обработки.
- **Postgres как очередь** — меньше зависимостей; брокер можно подставить позже сменой `JobRepository`.
- **at-least-once** — lease + reaper + backoff; постоянные ошибки (`4xx`) не жгут retry-бюджет.
- **Presigned URL** — процессор не получает наши credentials и не гоняет байты через API.
- **Magic bytes** — `Content-Type` не доверяем; ELF под видом `video/mp4` → `415`.
- **Лимит памяти upload** — слоты + multipart; при переполнении → `503` + `Retry-After`.
- **Вне скоупа** — auth/multi-tenancy, брокер, TTL объектов, реальный ML/OCR (по ТЗ — mock).
