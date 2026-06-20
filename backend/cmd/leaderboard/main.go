package main

import (
	"context"
	"leaderboard/internal/api"
	"leaderboard/internal/broker"
	"leaderboard/internal/config"
	"leaderboard/internal/leaderboard"
	"leaderboard/internal/store"
	"leaderboard/internal/ws"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/mux"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := config.Load()

	//Redis
	redisClient, err := store.NewRedisClient(cfg.RedisURL)
	if err != nil {
		log.Fatalf("redis connection failed: %v", err)
	}
	log.Println("connected to redis")
	defer redisClient.Close()

	//Postgres
	pgPool, err := store.NewPostgresPool(cfg.NeonDSN)
	if err != nil {
		log.Fatalf("postgres connection failed: %v", err)
	}
	log.Println("connected to neon postgres")
	defer pgPool.Close()

	//Kafka
	if err := broker.CheckKafkaConnection(cfg.KafkaBroker); err != nil {
		log.Fatalf("kafka connection failed: %v", err)
	}
	log.Println("connected to kafka")

	producer := broker.NewProducer(cfg.KafkaBroker)
	defer producer.Close()

	consumer := broker.NewConsumer(cfg.KafkaBroker)
	defer consumer.Close()

	// Rebuild Redis leaderboard from Postgres if empty
	if err := store.RebuildLeaderboard(ctx, pgPool, redisClient); err != nil {
		log.Fatalf("leaderboard rebuild failed: %v", err)
	}

	//ws hub
	hub := ws.NewHub()
	go hub.Run()

	// Kafka consumer (drives WebSocket push)
	go broker.ConsumeScoreUpdates(ctx, consumer, hub, redisClient)

	//services
	lbService := leaderboard.NewService(redisClient, pgPool, producer)
	scoresHandler := api.NewScoresHandler(lbService)

	//Router
	r := mux.NewRouter()
	r.HandleFunc("/health", api.HealthHandler).Methods(http.MethodGet)

	r.Handle("/v1/scores",
		api.APIKeyMiddleware(cfg.APIKey)(http.HandlerFunc(scoresHandler.PostScore)),
	).Methods(http.MethodPost)

	r.HandleFunc("/v1/scores", scoresHandler.GetTopScores).Methods(http.MethodGet)
	r.HandleFunc("/v1/scores/{userId}", scoresHandler.GetUserScore).Methods(http.MethodGet)

	// ws clients connect here for real-time push
	r.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		ws.ServeWs(hub, w, r)
	})

	server := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      r,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// graceful shutdown
	go func() {
		log.Printf("leaderboard service listening on :%s", cfg.Port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server failed: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutdown signal received, draining connections...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("server shutdown error: %v", err)
	}

	log.Println("server stopped cleanly")
}
