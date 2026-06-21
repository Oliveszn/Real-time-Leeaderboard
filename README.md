# Real-Time Leaderboard

A backend system for a real-time game leaderboard, built to handle frequent score writes and push live rank changes to connected clients over WebSocket.

---

## What it does

- Players' scores update in real time as games complete
- Clients see the top 10 and their own rank update live, pushed over WebSocket
- Any user can look up their rank and see the 4 players ranked above and below them
- Designed for scale: Redis sorted sets for fast reads, Kafka to decouple writes from real-time push, Postgres as the durable source of truth

---

## Architecture

```
Game client ──(WebSocket)──┐                    ┌──(WebSocket push)── Game client
                           ↓                    ↑
                     Load balancer
                           ↓
Game server ──(POST /v1/scores)──→ Leaderboard service
                                        │
                          ┌─────────────┴──────────────┐
                          ↓                            ↓
                   Redis cluster               PostgreSQL (Neon)
                   (sorted set)               (source of truth)
                          │
                          ↓ publishes score_update event
                    Kafka (message broker)
                          ↓
                   WS push service
                          │
              ┌───────────┴───────────┐
              ↓                       ↓
         Game client            Game client
         (leaderboard updated)  (rank updated)
```

**Why this shape:**

- **Redis sorted sets** are the primary read/write path. `ZINCRBY`, `ZREVRANGE`, and `ZREVRANK` give O(log N) score updates and rank lookups, fast enough to stay in the hot path under load, and purpose-built for exactly this kind of "always sorted" data.
- **Postgres (Neon)** is the durable backing store, not the hot path. Every score update is asynchronously batched into Postgres so the live leaderboard never waits on a slower database, while still keeping a permanent, queryable history (`score_events`) and a recovery source if Redis is ever lost.
- **Kafka** decouples the score write path from the WebSocket fan-out path. A score update publishes an event; a separate consumer reads it and decides who to push to. If the push service goes down temporarily, events queue in Kafka and replay on recovery, writes never block on broadcast.
- **WebSocket push, not polling.** Clients open one persistent connection and receive `leaderboard_update` (top 10 changed) or `rank_update` (their specific rank changed) messages as they happen.

Full design rationale, API contracts, schema, and failure-mode analysis are in [`ARCHITECTURE.md`](./ARCHITECTURE.md).

For the reasoning behind specific design tradeoffs and what actually broke under load testing see [`TRADEOFFS.md`](./TRADEOFFS.md).

---

## Tech stack

| Layer               | Choice                                   |
| ------------------- | ---------------------------------------- |
| Language            | Go                                       |
| Hot-path store      | Redis (sorted sets)                      |
| Durable store       | PostgreSQL via [Neon](https://neon.tech) |
| Message broker      | Kafka                                    |
| Real-time transport | WebSocket (`gorilla/websocket`)          |
| HTTP routing        | `gorilla/mux`                            |
| Postgres driver     | `pgx/v5`                                 |
| Containerization    | Docker Compose (Redis, Kafka, Zookeeper) |

---

## Project structure

```
backend/
├── cmd/
│   ├── leaderboard/   → main service entrypoint
│   ├── seed/          → one-shot script to populate Redis with test users
│   └── simulator/      → load generator: sustained writes + WS connections
├── internal/
│   ├── api/            → HTTP handlers, API key middleware, service interface
│   ├── broker/          → Kafka producer + consumer
│   ├── config/           → env var loading
│   ├── leaderboard/      → core service: orchestrates Redis/Postgres/Kafka
│   ├── store/             → Redis sorted set ops, Postgres queries, rebuild logic
│   └── ws/                 → WebSocket hub, client, connection handler
├── migrations/              → SQL schema
└── pkg/models/                → shared request/response types

frontend/
└── index.html                   → standalone live-demo dashboard (no build step)
```

---

## API

### `POST /v1/scores`

Adds points to a user's score. **Additive, not absolute**: `points` is added to the user's existing total via `ZINCRBY`. Restricted to trusted game servers via API key.

```bash
curl -X POST http://localhost:8080/v1/scores \
  -H "Content-Type: application/json" \
  -H "X-API-Key: <your_api_key>" \
  -d '{"user_id":"user-001","points":450}'
```

### `GET /v1/scores`

Returns the top 10 players.

### `GET /v1/scores/:userId`

Returns a user's rank, score, and the 4 players immediately above and below them.

### `GET /ws?userId=<id>`

WebSocket upgrade. Pushes:

- `leaderboard_update`: sent to **all** connected clients when the top 10 changes
- `rank_update`: sent to the **specific user** when their rank changes outside the top 10

---

## Running it locally

**1. Start infrastructure**

```bash
docker-compose up redis kafka zookeeper
```

**2. Set up environment**

Copy `.env.example` to `.env` inside `backend/` and fill in:

```properties
NEON_DSN=postgresql://user:password@ep-xxxxx.neon.tech/leaderboard?sslmode=require
API_KEY=<generate one>
KAFKA_BROKER=localhost:9092
PORT=8080
REDIS_URL=localhost:6379
```

**3. Run the database migration**

Paste `migrations/002_reset_text_ids.sql` into Neon's SQL editor (creates `users`, `user_scores`, `score_events`).

**4. Seed test data**

```bash
go run ./cmd/seed
```

**5. Start the service**

```bash
go run ./cmd/leaderboard
```

**6. (Optional) Run the load simulator**

```bash
go run ./cmd/simulator
```

Fires 100 score writes/sec while holding 250 concurrent WebSocket connections open, printing live throughput/latency/connection metrics every 5 seconds.

**7. View the live demo**

Open `frontend/index.html` directly in a browser, no build step. Point it at your running service, watch a specific user, and see the leaderboard update in real time.

---

## Testing

```bash
go test ./...
```

Covers:

- **Ranking math**: neighbour-window clamping, 0-based→1-based rank conversion, self-exclusion logic (`internal/store`)
- **HTTP handlers**: success paths, validation errors, service failures, and a regression guard ensuring score updates stay additive (`internal/api`)
- **API key middleware**: auth rejection paths
- **WebSocket hub**: broadcast fan-out, targeted user pushes, and connection cleanup (`internal/ws`)

Integration tests against real Redis/Postgres are intentionally out of scope for now, handler and ranking logic are tested in isolation via mocks and pure-function extraction.

---

## Things this project deliberately handles

- **Redis recovery**: if Redis restarts and loses its sorted set, the service rebuilds it from Postgres on startup (`internal/store/rebuild.go`)
- **Batched durability writes**: score events are buffered and flushed to Postgres in batches (not one transaction per request) to avoid exhausting the connection pool under sustained load, found and fixed during load testing
- **Graceful shutdown**: `SIGINT`/`SIGTERM` drain in-flight requests before exiting
- **Backpressure under load**: if Postgres falls behind, durability writes are dropped rather than blocking the live API response; Redis (the source of truth for the live leaderboard) stays correct and fast either way

---

## Live demo

🎥 You can see a live demo here: **[link]**
