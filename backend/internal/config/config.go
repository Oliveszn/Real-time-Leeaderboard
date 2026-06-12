package config

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	Port        string
	NeonDSN     string
	RedisURL    string
	KafkaBroker string
	APIKey      string
}

func Load() *Config {
	// .env is optional in Docker, env vars are injected directly via env_file/environment
	if err := godotenv.Load(); err != nil {
		log.Println("no .env file found, relying on environment variables")
	}

	cfg := &Config{
		Port:        getEnv("PORT", "8080"),
		NeonDSN:     mustGetEnv("NEON_DSN"),
		RedisURL:    getEnv("REDIS_URL", "localhost:6379"),
		KafkaBroker: getEnv("KAFKA_BROKER", "localhost:9092"),
		APIKey:      mustGetEnv("API_KEY"),
	}

	return cfg
}

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

func mustGetEnv(key string) string {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		log.Fatalf("required environment variable %s is not set", key)
	}
	return v
}
