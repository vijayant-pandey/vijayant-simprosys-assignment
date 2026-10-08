# AI Usage Log

I used **Claude Code** (inside VS Code) throughout this assignment. I treated it like a fast pair programmer: it drafted code and pointed out problems, and I decided what to keep. This log covers where I used it, what came out, and what changed along the way.

---

### 1. Reviewing my starting code

I already had a basic version: a FastAPI endpoint, an SQLAlchemy model and an HMAC helper. I gave the AI the assignment brief and asked it to compare my code against the requirements.

That review found several real problems:
- **Wrong route.** I had `/webhook/orders`; the spec says `/webhooks/order`. I switched to the spec's path but kept the old one as a hidden alias so nothing that already called it breaks.
- **Hardcoded secret.** `WEBHOOK_SECRET = "my-secret-key"` was sitting in the code. It now comes from `.env`, and the app refuses to start without one.
- **Signature checked too late.** FastAPI was validating the body *before* my signature check, so anyone could send junk and read our validation errors. Now the HMAC is checked on the raw bytes first, and only authenticated requests get parsed.
- **Race in duplicate detection.** I was doing "look up the `event_id`, insert if missing." Two identical requests arriving together could both pass the lookup. More on this below.
- **Money as a float.** I changed `amount` to `Decimal`.

### 2. Duplicate events

When I asked how to block duplicates, the obvious answer was the check-then-insert I already had. That's not safe under concurrency, so I went with letting the database decide: `event_id` has a UNIQUE constraint, the API just inserts, and if the insert fails on that constraint we read the existing row and return `200 duplicate`.

One thing I made sure of: if the insert fails for some *other* reason, the error is re-raised instead of being treated as a duplicate.

### 3. Making sure two workers don't process the same event

The AI suggested a few options: `SELECT ... FOR UPDATE`, a Redis lock, or a conditional UPDATE. I picked the **conditional UPDATE** ("set to PROCESSING only if it's currently RECEIVED or RETRYING"):
- It's a single atomic statement, so only one worker can win.
- No database lock is held while we wait on the downstream service.
- It works the same on SQLite and Postgres.

I then asked it to look for edge cases, and it found one I hadn't thought about. If a worker hangs past its timeout and another worker takes over, the first one could wake up and overwrite the result. To stop that, each claim gets a random token, and a worker can only save its result while it still holds that token.

### 4. Retries — the first version didn't work

The first draft used Celery's built-in `self.retry()`. When I ran the tests, two of them failed: in Celery's test (eager) mode, `self.retry()` raises an exception instead of running the task again.

Instead of bending the tests around it, I changed the approach. The worker now records the failure in the database (`retry_count`, `error_message`, `next_retry_at`) and schedules a delayed re-run itself. That turned out to be better anyway: the retry limit lives in the database, so it survives worker restarts.

### 5. Problems found while testing

- **Timezones on SQLite.** SQLite returns datetimes without a timezone, which would have broken the "has this lease expired?" comparisons. I added a small custom column type that always stores and returns UTC.
- **Table name clash.** My old table was `Webhook_events` and the new one is `webhook_events`. SQLite treats table names as case-insensitive, so the migration would have failed. The old table was empty, so I backed up the old DB file and started fresh.
- **Broker down.** The AI pointed out that if Redis is down right after we save an event, the event would sit in RECEIVED forever. The API now still returns 202 (the event is safely stored), and a small scheduled "sweeper" job re-queues anything stuck.

### 6. Tests

The AI generated most of the test code. When I reviewed it, I made sure the tests check what actually happened in the database and how many times downstream was called, not just HTTP status codes. For the concurrency tests, the threads use a barrier so they really start at the same moment; otherwise the race might never actually happen during the test.

### 7. Checking it for real

Passing tests weren't enough for me, so I ran the API and a real Celery worker and set the fake downstream service to fail 30% of the time. Every event still ended up `PROCESSED`; some needed one or two retries. Sending the same event twice returned 200, and a bad signature returned 401.

I couldn't test the Docker Compose setup because Docker isn't installed on my machine, and the README says so.

### 8. Docs

The AI drafted the README and the design note. I went through them to make sure they match the code and don't claim anything I didn't actually verify.

---

**Overall:** the AI saved a lot of typing and was especially good at spotting edge cases: the stale-worker overwrite, the broker-down gap, and the SQLite timezone issue. Its first answers weren't always right, though. Check-then-insert and `self.retry()` both looked fine until I tested them. Running the tests and the real worker is what caught those.
