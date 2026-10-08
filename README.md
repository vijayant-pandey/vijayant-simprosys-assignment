# Webhook Ingestion Service

Receives order webhooks from shops, authenticates them with HMAC-SHA256, stores each event **exactly once**, and processes it **asynchronously** with retries, a dead-letter state, and safety against concurrent workers.

**Stack:** FastAPI, SQLAlchemy 2, Alembic, Celery (Redis broker), PostgreSQL (SQLite for local dev and tests), pytest.

Other documents:
- [docs/DESIGN.md](docs/DESIGN.md): database design, concurrency and idempotency, scaling to about 10k req/s
- [AI_USAGE.md](AI_USAGE.md): AI usage log
- [docs/schema.postgres.sql](docs/schema.postgres.sql): generated PostgreSQL DDL

---

# Step-by-step guide: how to run and test this project

This guide assumes you have never run this project before. Follow the steps in order, and don't skip any.

There are two ways to test:
- **Part A – Automated tests.** One command checks everything for you. It takes about 5 minutes to set up and 10 seconds to run.
- **Part B – Manual testing.** You run the real service and send webhooks to it yourself, so you can see each piece working.

Do Part A first. Part B is optional but fun.

> **Windows or Mac/Linux?** Most commands are the same. Where they differ, both versions are shown. Windows users should use **Command Prompt (cmd)**; the commands below are written for it.

---

## Step 0 – Check that Python is installed

You need **Python 3.11 or newer**. Open a terminal:
- **Windows:** press the `Windows` key, type `cmd`, press Enter.
- **Mac:** press `Cmd + Space`, type `Terminal`, press Enter.

Type this and press Enter:

```
python --version
```

You should see something like `Python 3.13.1`. On Mac/Linux, if `python` doesn't work, try `python3 --version`, and use `python3` instead of `python` in every step below.

**If you get "not recognized" or "command not found":**
1. Install Python from https://www.python.org/downloads/
2. **Windows only:** on the first screen of the installer, **tick the box "Add python.exe to PATH"** before you click Install. This is very important.
3. Close the terminal, open a new one, and try `python --version` again.

You do **not** need Docker, Redis or PostgreSQL to test this project.

---

## Step 1 – Open a terminal inside the project folder

All commands must be run from inside the project folder, the one that contains `README.md` and the `app` folder.

- **Windows (easy way):** open the project folder in File Explorer, click the address bar at the top, type `cmd` and press Enter. A terminal opens already in the right folder.
- **Any system:** use `cd` with the folder's path, for example:
  ```
  cd C:\Users\YourName\Desktop\webhook-services
  ```

To check you're in the right place, run `dir` (Windows) or `ls` (Mac/Linux). You should see `README.md`, `app`, `tests` and `requirements.txt` in the list.

---

## Step 2 – Create a virtual environment

A virtual environment ("venv") is a private folder that holds this project's Python libraries, so they don't mix with anything else on your computer.

> ⚠️ **If the project folder you received already has a `venv` folder in it, delete that folder first.** A venv only works on the computer where it was created; copying it to another computer breaks it.

Create a new one:

```
python -m venv venv
```

This takes a few seconds and creates a new `venv` folder.

---

## Step 3 – Activate the virtual environment

**You must do this every time you open a new terminal for this project.**

| Your terminal | Command |
|---|---|
| Windows – Command Prompt (cmd) | `venv\Scripts\activate` |
| Windows – PowerShell | `venv\Scripts\Activate.ps1` |
| Mac / Linux | `source venv/bin/activate` |

When it works, you'll see **`(venv)`** at the start of your terminal line, like this:

```
(venv) C:\Users\YourName\Desktop\webhook-services>
```

**PowerShell says "running scripts is disabled on this system"?** Run this once, type `Y` to confirm, then try activating again:
```
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
```
(Or just use Command Prompt instead of PowerShell.)

---

## Step 4 – Install the required libraries

With `(venv)` showing, run:

```
pip install -r requirements.txt
```

This downloads everything the project needs (FastAPI, Celery, SQLAlchemy, pytest, …). It takes 1–2 minutes. Wait until you get your prompt back. Some yellow "notice" lines about upgrading pip are fine to ignore.

---

## Step 5 – Create your settings file (`.env`)

The app reads its settings from a file called `.env`. A ready-made template, `.env.example`, comes with the project. Copy it:

| System | Command |
|---|---|
| Windows | `copy .env.example .env` |
| Mac / Linux | `cp .env.example .env` |

Now open the new `.env` file in any text editor (Notepad, VS Code, …) and change **two things**:

**1. Set a secret key.** This is the password that webhook senders use to sign their requests. Generate a random one:

```
python -c "import secrets; print(secrets.token_hex(32))"
```

Copy the long random text it prints, and put it after `WEBHOOK_SECRET=`, for example:

```
WEBHOOK_SECRET=3f9a1c...your-long-random-text...b72e
```

**2. Use the simple local queue (no Redis needed).** Find these two lines:

```
CELERY_BROKER_URL=redis://localhost:6379/0
# CELERY_BROKER_URL=sqla+sqlite:///./celery-broker.db
```

and change them to this (add `#` to the first line, remove `#` from the second):

```
# CELERY_BROKER_URL=redis://localhost:6379/0
CELERY_BROKER_URL=sqla+sqlite:///./celery-broker.db
```

Save the file.

> **Why?** The background worker needs a message queue. Production uses Redis, but that's extra software to install. The `sqla+sqlite` option stores the queue in a small local file instead, so no extra software is needed.

---

## Step 6 – Create the database

```
alembic upgrade head
```

The last line should say something like:

```
INFO  [alembic.runtime.migration] Running upgrade  -> 0001, create webhook_events
```

This creates a file called `webhook.db`, a small SQLite database, with the `webhook_events` table inside.

✅ **Setup is finished.** You only need to do Steps 2–6 once.

---

## Part A – Run the automated tests

With `(venv)` showing, run:

```
pytest
```

After a few seconds you should see a row of green dots and, at the end:

```
48 passed in 4.21s
```

**48 passed = everything works.** The tests use their own temporary database, so they don't touch your `webhook.db`, and you don't need to start anything else first.

To see the name of every test as it runs, use `pytest -v`.

What the tests check:

| What is tested | What "pass" proves |
|---|---|
| Valid webhook | A correctly signed webhook is accepted (202), saved in the database, and sent to the queue |
| Invalid signature | Missing, wrong, or tampered signatures are rejected (401) and nothing is saved |
| Invalid data | Bad payloads (missing fields, negative amount, wrong date, …) are rejected (422) |
| Duplicate `event_id` | Sending the same event twice saves it only once and processes it only once |
| Background processing | The worker processes an event and marks it `PROCESSED` |
| Failure and retry | A failing event is retried; after too many failures it's marked `FAILED` with the error saved |
| Concurrency | 8 copies of the same event sent at the same moment → still only 1 saved and processed once |
| Queue outage | If the queue is down, the webhook is still saved safely, and a cleanup job picks it up later |

---

## Part B – Manual testing (see it working live)

You'll need **3 terminal windows** open at the same time, all in the project folder (Step 1) and **all with the venv activated (Step 3)**.

### Terminal 1 – Start the background worker

```
celery -A app.worker worker --pool=solo --loglevel=INFO
```

Wait until you see a line ending in **`ready.`**. Leave this window open; it will show what the worker is doing.

> Type the pool name exactly: `--pool=solo` (s-o-l-o). `--pool=solo` is needed on Windows and also works on Mac/Linux.

### Terminal 2 – Start the API (web server)

```
uvicorn app.main:app --reload
```

Wait until you see:

```
Uvicorn running on http://127.0.0.1:8000
```

Leave this window open too.

**Quick check:** open http://127.0.0.1:8000/health in your web browser. You should see:

```json
{"status":"ok","database":"ok"}
```

You can also open http://127.0.0.1:8000/docs to see an interactive page listing every endpoint.

### Terminal 3 – Send test webhooks

We provide a helper script that builds a sample order webhook and **signs it with your secret** automatically, just like a real shop would.

**Test 1 – Send a new, valid webhook**

```
python scripts/send_webhook.py --event-id evt_test_1
```

Expected:

```
202 {"event_id":"evt_test_1","status":"RECEIVED","duplicate":false,"message":"Webhook accepted"}
```

- `202` means the webhook was accepted and saved.
- Look at **Terminal 1** (the worker): within a second you'll see log lines with `"downstream accepted event"` and `"event processed"`.

**Test 2 – Check the event's status**

Open this in your browser:

http://127.0.0.1:8000/webhooks/events/evt_test_1

You should see `"status":"PROCESSED"`. The worker finished the job in the background.

> If you see `"status":"RETRYING"` instead, that's normal. The settings file makes the fake downstream service fail 20% of the time on purpose (`DOWNSTREAM_FAILURE_RATE=0.2`) so retries can be demonstrated. Wait a few seconds and refresh the page; it will change to `PROCESSED`.

**Test 3 – Send the same event again (duplicate)**

```
python scripts/send_webhook.py --event-id evt_test_1
```

Expected:

```
200 {"event_id":"evt_test_1","status":"PROCESSED","duplicate":true,"message":"Event already received"}
```

- `"duplicate":true` means the service recognized it had already seen this event.
- Look at Terminal 1: **nothing new happens.** The event is not processed a second time.

**Test 4 – Send a webhook with a wrong signature (fake sender)**

```
python scripts/send_webhook.py --bad-signature
```

Expected:

```
401 {"detail":"Invalid webhook signature"}
```

The request is rejected and nothing is saved.

**Test 5 – Watch automatic retries**

Here we make the fake downstream service fail often, so you can watch the retries.

1. Open `.env` and set `DOWNSTREAM_FAILURE_RATE=0.5` (50% of calls will fail). To see retries faster, also set:
   ```
   RETRY_BACKOFF_BASE_SECONDS=1
   RETRY_BACKOFF_MAX_SECONDS=4
   ```
2. **Restart the worker** so it reads the new settings: click Terminal 1, press `Ctrl + C`, wait for it to stop, then run the `celery ...` command again.
3. In Terminal 3, send several events. Run this a few times; leaving out `--event-id` gives a new random ID each time:
   ```
   python scripts/send_webhook.py
   ```
4. In Terminal 1 you'll see some `"processing failed, scheduling retry"` lines, followed a few seconds later by `"event processed"` for the same event.
5. Check one of them in the browser using the `event_id` printed by the script, for example `http://127.0.0.1:8000/webhooks/events/evt_ab12cd34ef`. Events that were retried show `"retry_count"` of 1 or more and end with `"status":"PROCESSED"`.

To see an event give up completely and become `FAILED`, set `DOWNSTREAM_FAILURE_RATE=1` (every call fails), restart the worker, and send one event. After about 6 attempts (1 try + 5 retries), its status becomes `"FAILED"`, with the error saved in `"error_message"`.

When you're done, set `DOWNSTREAM_FAILURE_RATE` back to `0.2` (or `0`).

### Stopping everything

In Terminal 1 and Terminal 2, press **`Ctrl + C`**.

### Starting over with an empty database (optional)

Stop the API and the worker, delete `webhook.db` and `celery-broker.db` (plus any `webhook.db-shm` / `webhook.db-wal` files), then run `alembic upgrade head` again.

---

## Troubleshooting

| Problem / error message | What it means | How to fix |
|---|---|---|
| `'celery' is not recognized…` / `'uvicorn' is not recognized…` / `'pytest' is not recognized…` | The venv isn't active in this terminal | Run the activate command from Step 3. You must see `(venv)` |
| `Invalid value for '-P' / '--pool': 'sol'…` | Typo in the command | Use `--pool=solo` |
| `WEBHOOK_SECRET … Field required` | The `.env` file is missing or `WEBHOOK_SECRET` is empty | Redo Step 5. Make sure the file is named exactly `.env` (not `.env.txt`) and is in the project folder |
| `webhook_secret … Value should have at least 16 items` | Your secret is empty or too short | Use the long random value from Step 5 |
| `no such table: webhook_events` | The database wasn't created | Run `alembic upgrade head` (Step 6) |
| `Error 10061 connecting to localhost:6379` or `Connection refused` mentioning Redis | The app is trying to use Redis, which isn't running | Do part 2 of Step 5 (switch to `sqla+sqlite`), then restart the worker and API |
| The script always gets `401`, even without `--bad-signature` | The API is still using an old secret | You changed `.env` after starting the API. Restart it (`Ctrl + C`, then run `uvicorn` again) |
| `ConnectError` / `Connection refused` when running the script | The API isn't running | Start it in Terminal 2 and wait for `Uvicorn running on…` |
| `[Errno 10048] … address already in use` | Port 8000 is used by another program | Start the API on another port: `uvicorn app.main:app --port 8001`, and send with `python scripts/send_webhook.py --url http://127.0.0.1:8001/webhooks/order` |
| Event stays `RECEIVED` and never becomes `PROCESSED` | The worker isn't running or is using different settings | Make sure Terminal 1 shows `ready.`, and that the API and worker were both started after your last `.env` change |
| Windows file name shows as `.env.txt` | Notepad added `.txt` | In File Explorer, turn on View → "File name extensions" and rename the file to `.env` |
| `pip install` fails building `psycopg` | Very old Python or pip | Use Python 3.11+ and run `python -m pip install --upgrade pip`, then retry Step 4 |

---

# Technical reference

## How it works

```
 Shop ──POST /webhooks/order──▶ API ──1. verify HMAC on raw body (401 if bad)
                                    ──2. validate payload (422 if bad)
                                    ──3. INSERT row (status RECEIVED); UNIQUE(event_id) rejects duplicates
                                    ──4. publish task id ──▶ Redis ──▶ Celery worker
                                    ◀─ 202 Accepted (or 200 duplicate)            │
                                                                                  ▼
                                   claim row atomically (RECEIVED/RETRYING → PROCESSING)
                                   call downstream (simulated)
                                   ├─ ok ─────────────▶ PROCESSED
                                   ├─ transient error ─▶ RETRYING, retry_count+1, delayed re-run (exp. backoff + jitter)
                                   └─ permanent error / retries exhausted ─▶ FAILED (dead letter, error_message kept)

 Celery beat (every 60s): sweeper re-enqueues events stuck in RECEIVED / overdue RETRYING / PROCESSING with an expired lease
```

Status lifecycle: `RECEIVED → PROCESSING → PROCESSED | RETRYING → PROCESSING … → FAILED`

## API

### `POST /webhooks/order`

Headers:
- `Content-Type: application/json`
- `X-Webhook-Signature: sha256=<hex>`, where `<hex>` is `HMAC_SHA256(WEBHOOK_SECRET, raw_request_body)` in hex. The `sha256=` prefix is optional.

| Response | When |
|---|---|
| `202 Accepted` `{"event_id", "status": "RECEIVED", "duplicate": false}` | New event stored and queued |
| `200 OK` `{"duplicate": true, "status": "<current status>"}` | `event_id` seen before; nothing is re-processed |
| `401` | Missing or invalid signature (checked **before** the payload is parsed) |
| `400` / `422` | Body is not JSON / payload fails validation |
| `413` | Body larger than `MAX_BODY_BYTES` |

Duplicates return a 2xx status on purpose. A 4xx would make the sender keep retrying an event we already hold.

Other endpoints:
- `GET /webhooks/events/{event_id}`: processing status, retry count and last error. The payload isn't returned. In production this belongs behind internal auth.
- `GET /health`: liveness plus a DB check.
- `GET /docs`: OpenAPI UI.
- `POST /webhook/orders`: the original route, kept as a hidden alias for backwards compatibility.

Validation rules: identifiers are 1–100 chars from `[A-Za-z0-9_.:-]`, and `event_type` must match `order.<name>`. `timestamp` must be ISO-8601 **with a timezone**. `amount` is a `Decimal` ≥ 0, never a float, because it's money. Extra fields inside `data` are kept in the stored payload.

## Running the sweeper (optional)

The worker handles all normal processing. The **sweeper**, which re-queues events whose queue message was lost, runs on a schedule through Celery beat. To run it locally, use a fourth terminal:

```bash
celery -A app.worker beat --loglevel=INFO
```

**Broker note:** `.env.example` defaults to Redis (`redis://localhost:6379/0`), which is what production should use. The `sqla+sqlite` broker used in the guide above is for local testing only.

## Signing a request manually (without the helper script)

The signature is the HMAC-SHA256 of the exact raw request body, using `WEBHOOK_SECRET` as the key, written in hex. In bash (Git Bash, Mac, Linux):

```bash
BODY='{"event_id":"evt_1","event_type":"order.created","shop_id":"shop_123","timestamp":"2026-08-18T10:30:00Z","data":{"order_id":"ORD-1","customer_id":"CUS-1","amount":2500}}'
SIG=$(printf '%s' "$BODY" | openssl dgst -sha256 -hmac "$WEBHOOK_SECRET" -hex | sed 's/^.* //')
curl -i -X POST localhost:8000/webhooks/order -H "Content-Type: application/json" -H "X-Webhook-Signature: sha256=$SIG" -d "$BODY"
```

### With Docker (PostgreSQL + Redis)

```bash
cp .env.example .env     # set WEBHOOK_SECRET
docker compose up --build
```

This runs Postgres, Redis, a one-off `migrate` job, the API on :8000, a worker and beat. Compose overrides `DATABASE_URL` and `CELERY_BROKER_URL` to point at the containers.

## Test files

The tests use a throwaway SQLite file and need no broker: publishing is stubbed, or Celery runs in eager mode.

| File | Covers |
|---|---|
| [tests/test_api.py](tests/test_api.py) | valid webhook, `sha256=` prefix, invalid HMAC (missing, garbage, wrong secret, tampered body, checked before validation), 422/400/413, duplicates, broker outage, legacy route, status endpoint |
| [tests/test_processing.py](tests/test_processing.py) | success, transient → `RETRYING` → `PROCESSED`, retries exhausted → `FAILED`, permanent error, idempotent re-run, expired-lease reclaim and fencing, sweeper query, backoff bounds |
| [tests/test_concurrency.py](tests/test_concurrency.py) | 8 threads delivering the same event → 1 row; 8 workers claiming → 1 winner; 8 duplicate task runs → downstream called once |
| [tests/test_worker.py](tests/test_worker.py) | full Celery task path (eager) through the API, including retries and giving up; sweeper task |

## Configuration

All settings are environment variables (see [.env.example](.env.example) and [app/config.py](app/config.py)). `WEBHOOK_SECRET` has **no default**: the service refuses to start without one (minimum 16 chars). `.env` is git-ignored.

## Project layout

```
app/
  main.py           FastAPI app: auth → validate → store → enqueue
  security.py       HMAC-SHA256 sign/verify (constant-time compare)
  schemas.py        Pydantic request/response models
  ingest.py         insert-once logic (UNIQUE constraint + IntegrityError)
  processing.py     claim / process / finalize, retry policy, sweeper query
  worker.py         Celery app, process + sweeper tasks, safe enqueue
  downstream.py     simulated external order service
  models.py         SQLAlchemy model, statuses, UTC datetime type
  databases.py      engine/session setup (SQLite WAL + busy timeout)
  config.py         typed settings (pydantic-settings)
  logging_config.py JSON logs
migrations/         Alembic migrations
tests/              pytest suite
scripts/send_webhook.py
```

## Decisions and known limitations

- **SQLite vs PostgreSQL.** The code and migrations are portable. Tests and quick local runs use SQLite with WAL mode and a busy timeout. Production should use PostgreSQL: row-level locking, `JSONB`, `TIMESTAMPTZ` and real concurrent writers. `docker-compose.yml` uses Postgres.
- **At-least-once delivery to downstream.** If a worker crashes *after* the downstream call succeeds but *before* it records `PROCESSED`, the event is retried after the lease expires. Exactly-once across a network boundary isn't possible, so the downstream call must be idempotent: pass `event_id` as its idempotency key.
- **Replay protection** comes from `event_id` uniqueness. Replaying a captured request is a no-op duplicate. A signed timestamp header with a ±5 min window would also stop replays with new IDs, but it needs sender support. It's listed under next steps.
- **Same `event_id` with a different payload** is treated as a duplicate: the first version wins and a warning is logged.
- **Dead letter = `FAILED` rows.** They are queryable through the `(status, updated_at)` index and keep `error_message` and `retry_count`. A replay command or admin endpoint is not implemented. It would set `status='RECEIVED', retry_count=0` and enqueue.
- **Not verified here:** the Docker Compose stack, because Docker wasn't available on the development machine. The full flow was verified end to end with uvicorn, a real Celery worker and the SQLAlchemy broker, with 30% simulated downstream failures: every event reached `PROCESSED` after retries. The PostgreSQL DDL was checked through Alembic's offline SQL output.

**Next steps, in order:** signed-timestamp replay window, secret rotation (accept two secrets during rollover), a replay command for `FAILED` events, Prometheus metrics (see the design doc), and running the test suite against Postgres in CI.
