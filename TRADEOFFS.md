# Leaderboard: Design Tradeoffs

## 1. Architecture: Why structure it this way?

The system is split into a hot path (Redis) and a durable path (Postgres), connected by an async batch writer, with Kafka decoupling writes from real-time push.

- **Redis owns the live leaderboard, Postgres does not.** Every read endpoint (`GET /v1/scores`, `GET /v1/scores/:userId`) queries Redis directly, never Postgres. This means the leaderboard's correctness and speed depend entirely on Redis being up, a deliberate choice, since sorted sets give O(log N) ranked reads that Postgres can't match at this query pattern without specialized indexing and significantly more complexity.

- **Postgres is for durability and history, not serving traffic.** `score_events` and `user_scores` exist so the system has a permanent record and a recovery source, not because anything reads from them in the hot path. This keeps the write-amplification cost of Postgres off the critical path entirely.

- **Kafka sits between the write and the push, not between the write and the read.** A score update is applied to Redis synchronously (the leaderboard must reflect it immediately), but the WebSocket fan-out is driven by a Kafka consumer reading `score_update` events asynchronously. This means a slow or disconnected WebSocket client can never block a score write, the write path and the broadcast path fail independently.

- **Interfaces at the handler boundary, not throughout.** `internal/api` depends on a `LeaderboardService` interface rather than the concrete service, specifically so handler tests can run against a mock with no real Redis/Postgres/Kafka. The rest of the codebase doesn't interface everything, only the boundary that needed to be testable in isolation.

## 2. Consistency Model: How and why?

The leaderboard is **eventually consistent by design**, not strongly consistent, this was stated explicitly in the original requirements gathering, not discovered as a limitation later.

Score writes happen in this order:

1. `ZINCRBY` on Redis: synchronous, this is what reads see immediately
2. Enqueue for Postgres: asynchronous, may lag by up to `flushInterval` (250ms) or queue depth
3. Publish to Kafka: asynchronous, drives the WebSocket push to clients

This means there's a small window where Redis has the new score but Postgres doesn't yet, and a separate small window where Redis has the new score but connected WebSocket clients haven't been notified yet. For a leaderboard, this is an acceptable tradeoff, a rank that's stale by a few hundred milliseconds doesn't meaningfully harm the product, and demanding strict consistency across all three systems would mean blocking every write on the slowest one (Postgres, by a wide margin).

**Scores are additive, not absolute.** `POST /v1/scores` adds `points` to the existing score via `ZINCRBY` rather than replacing it. This was a deliberate early decision, not a default, game servers send "points earned this round," and the gateway accumulates. Replacing on each call was considered and rejected because it would require every caller to know and send the user's full current score, pushing state management onto every game server instead of centralizing it here.

## 3. Failure Handling: Backpressure and Partial Recovery

**Backpressure under load: drop the durability write, never the live response.** The Postgres write queue is a buffered channel (capacity 5,000). If `UpdateScore` can't enqueue because the channel is full, the event is dropped, logged as a warning, not retried. This was a direct response to a real failure found during load testing (see below): under sustained 90+ writes/sec, a naive "block until the queue has room" approach would have made the live API response wait on Postgres, defeating the entire point of keeping Postgres out of the hot path. The accepted cost is some score events may never reach `score_events`/`user_scores` if Postgres falls far enough behind, Redis remains correct regardless, since it was never waiting on the queue in the first place.

**Batched writes instead of one transaction per event.** Originally, `RecordScoreEvent` opened a fresh `Begin → Exec ×3 → Commit` transaction per score update. Under simulated load (90-100 writes/sec), this exhausted Neon's connection pool, `pool.Begin(ctx)` started timing out with `context deadline exceeded` because each write was independently competing for a connection. The fix batches pending writes into groups (flushed every 100 events or 250ms, whichever comes first) and inserts them in one transaction using `UNNEST`, cutting the transaction rate by roughly 25x. This is the single biggest correctness bug load testing surfaced, it worked perfectly in every manual test, because manual tests never generated enough concurrent transactions to exhaust the pool.

**Redis recovery has a known gap: it only triggers on a fully empty leaderboard.** On startup, `RebuildLeaderboard` checks `ZCARD` and only rebuilds from Postgres if the count is exactly zero. This correctly handles a full Redis data loss (fresh container, wiped volume, etc.), but **does not** handle a partial recovery, if Redis crashes mid-write and comes back via an incomplete AOF replay with, say, 3,000 of 10,000 expected entries, `ZCARD > 0` and the rebuild is skipped entirely, silently leaving the leaderboard incomplete. For the scope of this project, that gap is accepted. In production, this would need either a `LASTSAVE` timestamp check against a known-good watermark, or a separate "leaderboard version" key written atomically alongside each batch, so a partial rebuild can be distinguished from a complete one rather than inferred from a non-zero count.

## 4. Real ID Formats vs. Schema Assumptions

The original Postgres schema defined `user_id` as `UUID`, on the assumption that game servers would always send proper UUIDs. This passed every manual test, because manual tests used a single hand-picked UUID (`550e8400-...`).

Load testing with the simulator which generates realistic, human-readable IDs like `sim-user-07992`, immediately broke every batch write with `invalid input syntax for type uuid`. The schema was migrated to `TEXT` for all `user_id` columns. This is kept in here deliberately as a lesson: **the gap between "works in a manual test with curated input" and "works under realistic generated load" is exactly the kind of thing that only shows up once you actually load test**, not before. A schema decision that looked reasonable on paper (use UUIDs, they're a Postgres best practice) didn't match how the system would actually be used.

## 5. What I'd Do Differently in Production

- **Detect partial Redis rebuilds, not just empty ones.** As described above, a version/watermark key written alongside each Redis batch, checked on startup instead of a blunt `ZCARD == 0` check.

- **Shard the Redis sorted set.** A single global `leaderboard` key works at this project's scale but becomes a write hotspot well before 50M DAU. Sharding by region or game mode, with a fan-in step for global top-N queries, would be the next step.

- **Make the dropped-write tradeoff observable, not just logged.** Right now a full write queue silently drops events with a log line. In production this needs a metric (counter + alert), since silent data loss that only shows up in logs under load is easy to miss until someone asks why an audit trail has gaps.

- **Reconsider Kafka consumer groups for the push service at scale.** Currently one consumer group (`leaderboard-push-service`) reads all score updates. At higher WebSocket connection counts, partitioning by user ID range and running multiple consumer instances would avoid a single consumer becoming a fan-out bottleneck.

- **Add integration tests against real Redis/Postgres**, not just unit tests against extracted pure functions and mocked services. The current test suite intentionally skips this (a scoping decision, not an oversight) `miniredis` or testcontainers would close that gap without needing a live database for every CI run.
