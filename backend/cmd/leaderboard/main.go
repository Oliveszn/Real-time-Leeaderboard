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
	// Usage: GET /ws?userId=<user_id>
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

	// Start server in background so we can listen for shutdown signal
	go func() {
		log.Printf("leaderboard service listening on :%s", cfg.Port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server failed: %v", err)
		}
	}()

	// Block until Ctrl+C or SIGTERM
	<-ctx.Done()
	log.Println("shutdown signal received, draining connections...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("server shutdown error: %v", err)
	}

	log.Println("server stopped cleanly")
}

// package main

// import (
// 	"bytes"
// 	"encoding/json"
// 	"fmt"
// 	"log"
// 	"math/rand"
// 	"net/http"
// 	"net/url"
// 	"os"
// 	"os/signal"
// 	"sync"
// 	"sync/atomic"
// 	"syscall"
// 	"time"

// 	"github.com/gorilla/websocket"
// 	"github.com/joho/godotenv"
// )

// // Config

// const (
// 	numUsers       = 1000 // total user pool to draw from
// 	writesPerSec   = 100  // score updates to fire per second
// 	wsConnections  = 250  // concurrent WebSocket connections to hold open
// 	reportInterval = 5 * time.Second
// )

// var (
// 	baseURL string
// 	apiKey  string
// )

// // Metrics

// var (
// 	totalWrites    atomic.Int64
// 	successWrites  atomic.Int64
// 	failedWrites   atomic.Int64
// 	totalPushes    atomic.Int64
// 	activeWS       atomic.Int64
// 	totalLatencyMs atomic.Int64
// )

// // Entry point

// func main() {
// 	godotenv.Load()

// 	baseURL = getEnv("BASE_URL", "http://localhost:8080")
// 	apiKey = mustGetEnv("API_KEY")

// 	log.Printf("simulator starting")
// 	log.Printf("  target:      %s", baseURL)
// 	log.Printf("  users:       %d", numUsers)
// 	log.Printf("  writes/sec:  %d", writesPerSec)
// 	log.Printf("  ws conns:    %d", wsConnections)

// 	stop := make(chan struct{})

// 	// Catch Ctrl+C for a clean shutdown
// 	sig := make(chan os.Signal, 1)
// 	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
// 	go func() {
// 		<-sig
// 		log.Println("\nsimulator: shutting down...")
// 		close(stop)
// 	}()

// 	var wg sync.WaitGroup

// 	// 1. WebSocket clients connect and hold open
// 	wg.Add(1)
// 	go func() {
// 		defer wg.Done()
// 		runWSClients(stop)
// 	}()

// 	// 2. Score writer fires HTTP POSTs at writesPerSec
// 	wg.Add(1)
// 	go func() {
// 		defer wg.Done()
// 		runScoreWriter(stop)
// 	}()

// 	// 3. Metrics reporter
// 	wg.Add(1)
// 	go func() {
// 		defer wg.Done()
// 		runReporter(stop)
// 	}()

// 	wg.Wait()
// 	printFinalReport()
// }

// // Score writer

// func runScoreWriter(stop chan struct{}) {
// 	ticker := time.NewTicker(time.Second / time.Duration(writesPerSec))
// 	defer ticker.Stop()

// 	client := &http.Client{Timeout: 5 * time.Second}
// 	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

// 	for {
// 		select {
// 		case <-stop:
// 			return
// 		case <-ticker.C:
// 			go fireScoreUpdate(client, rng)
// 		}
// 	}
// }

// func fireScoreUpdate(client *http.Client, rng *rand.Rand) {
// 	userID := fmt.Sprintf("sim-user-%05d", rng.Intn(numUsers)+1)
// 	points := rng.Intn(500) + 10

// 	body, _ := json.Marshal(map[string]interface{}{
// 		"user_id": userID,
// 		"points":  points,
// 	})

// 	start := time.Now()

// 	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/scores", bytes.NewReader(body))
// 	if err != nil {
// 		failedWrites.Add(1)
// 		return
// 	}
// 	req.Header.Set("Content-Type", "application/json")
// 	req.Header.Set("X-API-Key", apiKey)

// 	resp, err := client.Do(req)
// 	totalWrites.Add(1)

// 	latency := time.Since(start).Milliseconds()
// 	totalLatencyMs.Add(latency)

// 	if err != nil || resp.StatusCode != http.StatusOK {
// 		failedWrites.Add(1)
// 		if err == nil {
// 			resp.Body.Close()
// 		}
// 		return
// 	}

// 	resp.Body.Close()
// 	successWrites.Add(1)
// }

// // WebSocket clients

// func runWSClients(stop chan struct{}) {
// 	var wg sync.WaitGroup

// 	// Stagger connections so we don't slam the server all at once
// 	for i := 0; i < wsConnections; i++ {
// 		wg.Add(1)
// 		go func(id int) {
// 			defer wg.Done()
// 			userID := fmt.Sprintf("ws-watcher-%05d", id+1)
// 			connectAndListen(userID, stop)
// 		}(i)

// 		// Small stagger between each connection
// 		time.Sleep(10 * time.Millisecond)
// 	}

// 	wg.Wait()
// }

// func connectAndListen(userID string, stop chan struct{}) {
// 	wsURL := url.URL{
// 		Scheme:   "ws",
// 		Host:     stripScheme(baseURL),
// 		Path:     "/ws",
// 		RawQuery: "userId=" + userID,
// 	}

// 	for {
// 		select {
// 		case <-stop:
// 			return
// 		default:
// 		}

// 		conn, _, err := websocket.DefaultDialer.Dial(wsURL.String(), nil)
// 		if err != nil {
// 			log.Printf("ws connect failed for %s: %v — retrying in 2s", userID, err)
// 			time.Sleep(2 * time.Second)
// 			continue
// 		}

// 		activeWS.Add(1)
// 		readMessages(conn, userID, stop)
// 		activeWS.Add(-1)

// 		select {
// 		case <-stop:
// 			return
// 		default:
// 			// reconnect
// 			time.Sleep(500 * time.Millisecond)
// 		}
// 	}
// }

// func readMessages(conn *websocket.Conn, userID string, stop chan struct{}) {
// 	defer conn.Close()

// 	done := make(chan struct{})

// 	go func() {
// 		defer close(done)
// 		for {
// 			_, _, err := conn.ReadMessage()
// 			if err != nil {
// 				return
// 			}
// 			totalPushes.Add(1)
// 		}
// 	}()

// 	select {
// 	case <-stop:
// 		conn.WriteMessage(websocket.CloseMessage,
// 			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
// 		return
// 	case <-done:
// 		return
// 	}
// }

// // Reporter

// func runReporter(stop chan struct{}) {
// 	ticker := time.NewTicker(reportInterval)
// 	defer ticker.Stop()

// 	var lastWrites, lastPushes int64
// 	start := time.Now()

// 	for {
// 		select {
// 		case <-stop:
// 			return
// 		case <-ticker.C:
// 			writes := successWrites.Load()
// 			failed := failedWrites.Load()
// 			pushes := totalPushes.Load()
// 			ws := activeWS.Load()
// 			total := totalWrites.Load()

// 			writeDelta := writes - lastWrites
// 			pushDelta := pushes - lastPushes
// 			lastWrites = writes
// 			lastPushes = pushes

// 			avgLatency := int64(0)
// 			if total > 0 {
// 				avgLatency = totalLatencyMs.Load() / total
// 			}

// 			elapsed := time.Since(start).Seconds()
// 			overallRPS := float64(writes) / elapsed

// 			fmt.Printf("\n[%s]\n", time.Now().Format("15:04:05"))
// 			fmt.Printf("  writes:    %d total  |  %d ok  |  %d failed\n", total, writes, failed)
// 			fmt.Printf("  throughput: %.1f rps overall  |  %d writes last %s\n",
// 				overallRPS, writeDelta, reportInterval)
// 			fmt.Printf("  ws:        %d active connections  |  %d pushes last %s\n",
// 				ws, pushDelta, reportInterval)
// 			fmt.Printf("  latency:   %dms avg per write\n", avgLatency)
// 		}
// 	}
// }

// func printFinalReport() {
// 	writes := successWrites.Load()
// 	failed := failedWrites.Load()
// 	total := totalWrites.Load()
// 	pushes := totalPushes.Load()

// 	fmt.Println("\n========= FINAL REPORT =========")
// 	fmt.Printf("total writes attempted: %d\n", total)
// 	fmt.Printf("successful writes:      %d\n", writes)
// 	fmt.Printf("failed writes:          %d\n", failed)
// 	fmt.Printf("total WS pushes recv:   %d\n", pushes)
// 	if total > 0 {
// 		fmt.Printf("success rate:           %.2f%%\n", float64(writes)/float64(total)*100)
// 		fmt.Printf("avg write latency:      %dms\n", totalLatencyMs.Load()/total)
// 	}
// 	fmt.Println("=================================")
// }

// // --- Helpers ---

// func stripScheme(u string) string {
// 	for _, prefix := range []string{"http://", "https://"} {
// 		if len(u) > len(prefix) && u[:len(prefix)] == prefix {
// 			return u[len(prefix):]
// 		}
// 	}
// 	return u
// }

// func getEnv(key, fallback string) string {
// 	if v, ok := os.LookupEnv(key); ok && v != "" {
// 		return v
// 	}
// 	return fallback
// }

// func mustGetEnv(key string) string {
// 	v, ok := os.LookupEnv(key)
// 	if !ok || v == "" {
// 		log.Fatalf("required env var %s not set", key)
// 	}
// 	return v
// }
