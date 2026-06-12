CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- users: identity record for each player
CREATE TABLE IF NOT EXISTS users (
    user_id    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_name  TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- user_scores: current cumulative score per user
-- This mirrors what's in the Redis sorted set and is used to rebuild Redis if it's ever lost.
CREATE TABLE IF NOT EXISTS user_scores (
    user_id    UUID PRIMARY KEY REFERENCES users(user_id) ON DELETE CASCADE,
    score      BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- score_events: audit trail of every point grant
-- Useful for rebuilding user_scores, anti-cheat analysis, and debugging score history.
CREATE TABLE IF NOT EXISTS score_events (
    id         BIGSERIAL PRIMARY KEY,
    user_id    UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    points     INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index to speed up "fetch all events for a user" queries
CREATE INDEX IF NOT EXISTS idx_score_events_user_id ON score_events(user_id);

-- Index to speed up rebuilding the Redis leaderboard sorted by score
CREATE INDEX IF NOT EXISTS idx_user_scores_score ON user_scores(score DESC);