# ---- Build stage ----
FROM golang:1.25.4-alpine AS builder

WORKDIR /app

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source
COPY . .

# Build the leaderboard service binary
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/leaderboard ./cmd/leaderboard

# ---- Run stage ----
FROM alpine:3.19

WORKDIR /app

# Certs needed for HTTPS calls (e.g. Neon Postgres over TLS)
RUN apk add --no-cache ca-certificates

COPY --from=builder /bin/leaderboard /app/leaderboard

EXPOSE 8080

ENTRYPOINT ["/app/leaderboard"]