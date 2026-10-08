# Design Note

## 1. Database design

Table `webhook_events` (migration: [migrations/versions/0001_create_webhook_events.py](../migrations/versions/0001_create_webhook_events.py)):

| Column | Type (Postgres) | Purpose |
|---|---|---|
| `id` | `BIGSERIAL` PK | Surrogate key: small, monotonic, cheap to index; used in queue messages |
| `event_id` | `VARCHAR(100)` **UNIQUE** | Sender's idempotency key |
| `shop_id`, `event_type` | `VARCHAR(100)` | Routing and filtering |
| `occurred_at` | `TIMESTAMPTZ` | Sender's `timestamp` |
| `payload` | `JSONB` | Full original body, for audit and replay |
| `status` | `VARCHAR(20)` + CHECK | `RECEIVED / PROCESSING / RETRYING / PROCESSED / FAILED` |
| `retry_count` | `INT` + CHECK ≥ 0 | Failed attempts so far |
| `error_message` | `TEXT` | Last failure (truncated to 2,000 chars) |
| `next_retry_at` | `TIMESTAMPTZ` | When the scheduled retry is due |
| `locked_at`, `lock_token` | `TIMESTAMPTZ`, `VARCHAR(36)` | Worker lease and fencing token |
| `created_at`, `updated_at`, `processed_at` | `TIMESTAMPTZ` | Lifecycle timestamps, all UTC |

**Keys and constraints.** The PK is a `BIGINT` surrogate. `event_id` is a separate unique key because it's an external string of unknown format. A narrow integer PK keeps secondary indexes and queue messages small. `UNIQUE(event_id)` assumes the platform issues globally unique IDs. If IDs were only unique per shop, the constraint would become `UNIQUE(shop_id, event_id)`.

**Indexes and the queries they serve:**

| Index | Query |
|---|---|
| `uq_webhook_events_event_id` | Duplicate detection on insert; `GET /webhooks/events/{event_id}` |
| PK `id` | Worker claim/finalize: `UPDATE … WHERE id = ?` |
| `ix_webhook_events_status_updated_at` | Sweeper and ops: "events in status X older than T"; FAILED (dead-letter) listing |
| `ix_webhook_events_shop_id_created_at` | Per-shop history and support lookups |

Write cost was weighed against these: every index slows the hot `INSERT` path, so there are no single-column indexes on low-selectivity columns such as `status` alone.

## 2. Preventing duplicates

There are two layers, and only the database one is relied on for correctness:

1. **Ingest:** the API does a plain `INSERT`. If two deliveries of the same event race on different API instances, the `UNIQUE` constraint lets exactly one commit. The other gets an `IntegrityError`, rolls back, reads the existing row and returns `200 duplicate` without enqueueing. The original code did "SELECT, then INSERT if missing". That has a race window where both requests see "missing", so it was replaced. This is covered by `test_concurrent_duplicate_deliveries_store_exactly_one_row`.
2. **Processing:** the queue is at-least-once, so a task can run twice through redelivery after a crash, a sweeper re-enqueue or a duplicate publish. The worker therefore *claims* the row before doing anything (next section). `PROCESSED` and `FAILED` rows are never claimable, so re-running a task is a no-op.

## 3. Concurrent workers

```sql
-- claim (one statement, atomic)
UPDATE webhook_events
   SET status='PROCESSING', locked_at=:now, lock_token=:uuid, updated_at=:now
 WHERE id=:id
   AND (status IN ('RECEIVED','RETRYING')
        OR (status='PROCESSING' AND locked_at < :now - lease));
-- rowcount = 1 → we own it; 0 → someone else does / it's done → skip
```

- The database serialises concurrent UPDATEs on a row, so exactly one worker gets `rowcount = 1`. This needs no `SELECT … FOR UPDATE` held across the network call, and it works on Postgres, MySQL and SQLite.
- The downstream call happens **outside** any transaction, so no DB locks are held during slow I/O.
- **Lease:** if a worker dies mid-task, the row stays `PROCESSING`. After `PROCESSING_LEASE_SECONDS` another worker, or the sweeper, may reclaim it.
- **Fencing:** the final `UPDATE … WHERE id=:id AND status='PROCESSING' AND lock_token=:token` only succeeds for the current owner. A worker that stalled past its lease, for example during a long GC pause, can't overwrite the new owner's result. This is covered by `test_expired_lease_is_reclaimed_and_stale_worker_is_fenced_off`.
- The retry limit is read from `retry_count` in the DB, not from Celery's in-memory retry counter. So it survives worker restarts and redeliveries.

Celery settings that support this: `task_acks_late=True` and `task_reject_on_worker_lost=True` make a crash cause redelivery rather than loss, and `worker_prefetch_multiplier=1`.

**Lost-message recovery (outbox-lite).** The row is committed *before* publishing. If the broker is down, the API still answers 202 and logs the failure. The beat-scheduled sweeper re-enqueues three kinds of event: `RECEIVED` older than the grace period, `RETRYING` past `next_retry_at`, and `PROCESSING` with an expired lease. Because claims are idempotent, an occasional double enqueue is harmless.

## 4. When the table reaches tens of millions of rows

- **Partition by time** with Postgres declarative range partitioning on `created_at`, one partition per day or week. Retention becomes `DROP PARTITION` instead of a huge `DELETE`. Because a unique index on a partitioned table must include the partition key, global dedup moves to a small, separate `processed_event_ids(event_id PK, created_at)` table. Alternatively, dedup only within a bounded window (senders don't redeliver after days) via Redis `SET NX` plus partition-local uniqueness.
- **Keep the hot set small.** Workers and the sweeper only touch non-terminal rows. A **partial index** `ON (status, updated_at) WHERE status IN ('RECEIVED','RETRYING','PROCESSING')` stays tiny even when the table holds 100M `PROCESSED` rows.
- **Archive** old payloads to object storage such as S3 or Parquet. Keep metadata online and drop `payload` from cold partitions.
- **Read replicas** for status lookups and reporting, so the primary serves only ingest and claim writes.
- `BIGINT` PK from the start (done), so the key never overflows.

## 5. Scaling to about 10,000 requests per second

**API instances.** The API is stateless: verify the HMAC, one INSERT, one publish. Run N replicas behind a load balancer with HPA on CPU and p99 latency. At roughly 500–1,000 req/s per instance (uvicorn plus several workers per pod), 10k req/s needs about 15–25 pods with headroom. Use PgBouncer in transaction mode so the replicas don't exhaust Postgres connections. Keep the request path minimal: no downstream calls and no extra reads.

**Workers.** Workers scale horizontally and independently of the API. Autoscale on **queue depth and age**, for example with KEDA. Throughput ≈ workers × concurrency / downstream latency: at 100 ms latency, 10k/s needs about 1,000 concurrent slots. Use gevent or eventlet pools or async workers for I/O-bound calls. Use separate queues per priority, and shard by shop so one noisy shop can't starve the others. Enforce per-shop rate limits toward downstream.

**If the database becomes the bottleneck:**
1. Batch the hot path. Have the API publish straight to a durable log such as Kafka or Kinesis and return 202. Ingestion workers then write to the DB in batches with multi-row `INSERT … ON CONFLICT (event_id) DO NOTHING`, which cuts per-request round trips by 50–100x. Durability then comes from Kafka's replicated log rather than the DB commit.
2. Use partitioning, the partial index and lean secondary indexes, as described in section 4.
3. Add a Redis dedup front-check (`SET event_id NX EX 7d`) so most duplicates never reach the DB. The DB constraint stays as the source of truth.
4. As a final option, **shard by `shop_id`** across Postgres clusters (or Citus). `event_id` is then looked up via the shop.

**Duplicate events** are handled in three layers: the Redis `SET NX` fast path, the DB unique constraint as the authority, and the idempotent worker claim. The downstream call carries `event_id` as an idempotency key, which covers the "crashed after the side effect" gap.

**Queue failures.** The commit happens before publish, and the sweeper recovers anything not published, so a broker outage delays processing but loses nothing. Run Redis with persistence and replication (Sentinel or ElastiCache), or move to RabbitMQ quorum queues or SQS for stronger delivery guarantees. With Kafka in front, the API keeps accepting even while the DB or workers are down.

**Downstream failures.** Retries use exponential backoff with full jitter and a cap. Transient and permanent errors are classified separately. Add a **circuit breaker** per downstream so workers stop hammering a failing service and messages wait in the queue instead of burning retries. After `MAX_RETRIES`, events go to `FAILED`, the dead letter, with the error recorded, and a replay tool requeues them after the incident.

**Where Redis or caching helps:**
- dedup fast path (`SET NX`)
- the Celery broker
- per-shop rate limiting and token buckets
- circuit-breaker state shared across workers
- caching per-shop secrets and config, if each shop gets its own HMAC secret
- short-TTL caching of status lookups

**Monitoring and alerting:**
- *Metrics* (Prometheus/OpenTelemetry):
  - request rate and p50/p99 latency by status code
  - 401 rate (spikes suggest a misconfigured sender or an attack)
  - duplicate rate
  - queue depth and oldest-message age
  - processing latency (`processed_at - created_at`)
  - outcome counts (processed, retry, failed)
  - downstream error rate and latency
  - sweeper re-enqueue count
  - DB pool saturation, replication lag, lock waits
- *Alerts:*
  - oldest unprocessed event older than N minutes
  - FAILED rate above a threshold
  - sweeper re-enqueues above 0 for a sustained period (the broker is losing messages)
  - 5xx rate
  - 401 spike
  - DB CPU or connections near their limits
  - circuit breaker open
- *Logs:* JSON logs (already implemented) carry `event_id`/`shop_id`, so an event can be traced end to end. Add trace IDs propagated through task headers.
- *Dashboards:* per-shop volume and failure breakdown.
