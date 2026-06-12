package main

import (
	"leaderboard/internal/api"
	"leaderboard/internal/broker"
	"leaderboard/internal/config"
	"leaderboard/internal/store"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

func main() {
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

	//Router
	r := mux.NewRouter()
	r.HandleFunc("/health", api.HealthHandler).Methods(http.MethodGet)

	log.Printf("leaderboard service listening on :%s", cfg.Port)
	if err := http.ListenAndServe(":"+cfg.Port, r); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}
