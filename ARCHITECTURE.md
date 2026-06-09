# Real-time Gaming Leaderboard — Architecture

## Overview

A real-time leaderboard system designed to handle 50 million DAU, with support for top-10 display, individual rank lookup, and a contextual neighbourhood view (4 players above and below a given user). Score updates are pushed to connected clients in real-time without polling.

---

## Functional Requirements

- Display the top 10 players on the global leaderboard
- Show a specific user's rank on demand
- Display the 4 players above and below a given user (contextual rank view)
- Score updates are **additive** i.e a user's new points are added to their existing score, never replaced

---

## Non-Functional Requirements

- Real-time score propagation: a score update must be reflected on the leaderboard within ~1–2 seconds
- **Eventual consistency is acceptable**: brief lag between a score update and leaderboard propagation is fine for this use case
- High availability: the leaderboard must remain readable even under write pressure
- Fault tolerance: Redis data loss must be recoverable from the backing database

---

## Scale Estimates

| Metric                     | Value                                    |
| -------------------------- | ---------------------------------------- |
| Daily active users         | 50 million                               |
| Average concurrent users   | 50 / second                              |
| Peak concurrent users      | 250 / second                             |
| Score write frequency      | Per game completion                      |
| Leaderboard read frequency | Continuous (WebSocket push, not polling) |

---

## API Design

### Authentication

Score write endpoints (`POST /v1/scores`) are **internal only** — accessible exclusively by trusted game servers. Authentication is enforced via **API key** passed in the request header:

```
X-API-Key: <server_api_key>
```

Client-facing read endpoints and WebSocket connections use standard user-level auth (JWT/session token). No client can write scores directly.

---

### POST /v1/scores

Updates a user's score. Points are **added** to the existing score, not replacing it.

**Request**

```http
POST /v1/scores
X-API-Key: <server_api_key>
Content-Type: application/json

{
  "user_id": "user_id1",
  "points": 450
}
```

**Behaviour**

Internally calls `ZINCRBY leaderboard <points> <user_id>` on Redis. If the user does not exist in the sorted set, Redis initialises their score to 0 and adds the points. The operation is O(log N).

**Response**

```json
{
  "user_id": "user_id1",
  "new_score": 12993
}
```

---

### GET /v1/scores

Returns the top 10 players on the leaderboard, ordered by rank descending.

**Response**

```json
{
  "data": [
    {
      "user_id": "user_id1",
      "user_name": "Olive",
      "rank": 1,
      "score": 12543
    },
    {
      "user_id": "user_id2",
      "user_name": "Alex",
      "rank": 2,
      "score": 11500
    }
  ],
  "total": 10
}
```

Internally calls `ZREVRANGE leaderboard 0 9 WITHSCORES` — O(log N + 10).

---

### GET /v1/scores/:userId

Returns a specific user's score, rank, and the 4 players immediately above and below them.

**Response**

```json
{
  "user_info": {
    "user_id": "user5",
    "user_name": "charlie",
    "score": 1000,
    "rank": 6
  },
  "neighbours": [
    { "user_id": "user3", "rank": 2, "score": 1400 },
    { "user_id": "user4", "rank": 5, "score": 1100 },
    { "user_id": "user6", "rank": 7, "score": 950 },
    { "user_id": "user7", "rank": 8, "score": 900 }
  ]
}
```

**Implementation**

```
rank  = ZREVRANK leaderboard <user_id>          → O(log N)
range = ZREVRANGE leaderboard (rank-4) (rank+4) → O(log N + 9)
```

Both operations complete in a single Redis round-trip (pipeline).

---

## Data Model

### Redis — Primary Leaderboard Store

Redis is chosen as the primary data store for leaderboard reads and writes. It is an in-memory data store, meaning operations complete in microseconds, and it ships a native **sorted set** data structure that maps directly to leaderboard semantics.

A sorted set maintains a mapping of member to score, kept sorted at all times. Internally it is implemented as a hash map (O(1) member lookup) plus a skip list (O(log N) sorted access). This gives us a data structure that is always sorted at the price of O(log N) for inserts and lookups — far cheaper than `ORDER BY` on a relational table at 50M rows.

**Key:** `leaderboard` (single global key; can be sharded by region or game mode as scale demands)

**Operations used:**

| Operation                              | Purpose                      | Complexity    |
| -------------------------------------- | ---------------------------- | ------------- |
| `ZINCRBY leaderboard <pts> <user_id>`  | Add points to a user's score | O(log N)      |
| `ZREVRANGE leaderboard 0 9 WITHSCORES` | Fetch top 10                 | O(log N + 10) |
| `ZREVRANK leaderboard <user_id>`       | Get a user's rank            | O(log N)      |
| `ZREVRANGE leaderboard <start> <end>`  | Fetch neighbourhood          | O(log N + M)  |

`ZINCRBY` is idempotent to missing users, if a user doesn't exist yet, their score starts at 0 and points are added. No separate insert step is needed.

---

### PostgreSQL (Neon) — Source of Truth

Redis is fast but exists/lasts for only a short time. A crash or restart wipes the sorted set. Neon Postgres is the durable backing store, every score update is asynchronously written to Postgres after Redis is updated.

**Schema:**

```sql
-- Users table
CREATE TABLE users (
  user_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_name TEXT NOT NULL UNIQUE,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Scores table — current cumulative score per user
CREATE TABLE user_scores (
  user_id    UUID PRIMARY KEY REFERENCES users(user_id),
  score      BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Score history — audit trail of each game result
CREATE TABLE score_events (
  id         BIGSERIAL PRIMARY KEY,
  user_id    UUID NOT NULL REFERENCES users(user_id),
  points     INT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW()
);
```

**Redis rebuild on restart:**

If Redis is restarted and the sorted set is empty, the leaderboard service runs a rebuild query on startup:

```sql
SELECT user_id, score FROM user_scores ORDER BY score DESC;
```

Then bulk-loads using `ZADD leaderboard <score> <user_id>` for each row. Redis replicas and AOF persistence reduce how often this is needed.

---

## High-Level Architecture

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

---

## Component Breakdown

### Game Server

The only entity permitted to call `POST /v1/scores`. Clients never write scores directly. The game server authenticates with an API key and submits the `user_id` + `points` earned from a completed game. This prevents score tampering from clients.

### Leaderboard Service

Orchestrates the score write path:

1. Receives `POST /v1/scores` from a game server
2. Calls `ZINCRBY` on Redis the primary update, synchronous
3. Asynchronously writes the delta to Postgres (`score_events`) and upserts `user_scores`
4. Publishes a `score_update` event to Kafka with the affected `user_id`, new score, and new rank

Also handles read requests (`GET /v1/scores`, `GET /v1/scores/:userId`) directly against Redis.

### Redis Cluster

The hot path for all leaderboard reads and writes. Sorted set operations are O(log N) and run entirely in memory. A replica is maintained for fault tolerance; AOF persistence is enabled so the dataset survives restarts without a full Postgres rebuild.

### PostgreSQL on Neon

The durable record of all scores. Writes are asynchronous and Redis is updated first, Postgres shortly after. Acts as the rebuild source if Redis is ever fully lost. `score_events` provides a full audit trail of every points grant, which is also useful for anti-cheat analysis.

### Kafka (Message Broker)

Decouples score writes from WebSocket fan-out. When the leaderboard service publishes a `score_update` event, Kafka holds it durably. The WebSocket push service consumes from Kafka at its own pace. If the push service restarts, it replays from its last committed offset, no updates are lost. This also prevents a slow or disconnected client from blocking the write path.

### WebSocket Push Service

Maintains persistent WebSocket connections to all active clients. Consumes `score_update` events from Kafka and fans out updates to relevant subscribers:

- If the update affects the top 10: broadcast to all connected clients
- If the update changes a user's rank: send a targeted update to that user's connection

This eliminates client polling entirely. At 250 peak connections, the push service handles 250 open sockets, horizontally scalable behind the load balancer with sticky sessions or a shared Redis pub/sub layer for cross-node fan-out.

---

## Score Write Flow (Step by Step)

```
1. User completes a game
2. Game server calls POST /v1/scores { user_id, points }
3. Leaderboard service authenticates the API key
4. ZINCRBY leaderboard <points> <user_id>  →  Redis (sync)
5. INSERT INTO score_events ...            →  Postgres (async)
6. UPSERT INTO user_scores ...             →  Postgres (async)
7. Publish { user_id, new_score, new_rank } → Kafka
8. WS push service consumes event
9. If top-10 affected: broadcast to all clients
   Else: push rank update to user's socket
```

---

## Failure Scenarios

| Failure                   | Impact                                              | Recovery                                           |
| ------------------------- | --------------------------------------------------- | -------------------------------------------------- |
| Redis crash               | Leaderboard reads fail; writes queue                | Rebuild from Postgres on restart                   |
| Postgres down             | Score writes still succeed (Redis); history delayed | Replay from Kafka on restore                       |
| Kafka down                | WS push stops; scores still update in Redis         | Clients see stale leaderboard until Kafka recovers |
| Push service crash        | No WebSocket updates                                | Reconnects and replays from last Kafka offset      |
| Leaderboard service crash | Reads and writes fail                               | Stateless — restart is immediate                   |

---

## Future Considerations

- **Shard Redis by game/region** if a single sorted set becomes a write bottleneck at 50M+ users
- **Batch `ZINCRBY` calls** via Kafka consumer grouping if score write spikes at game-end become a problem
- **Leaderboard segments**: weekly/monthly leaderboards using separate Redis keys with TTLs
- **Anti-cheat hooks**: `score_events` audit trail can feed an anomaly detection service
