// cmd/simulator is a long-running load generator
//
// It mimics real production traffic against an already-running leaderboard
// service: it fires concurrent score updates over HTTP at a sustained rate
// (100/sec by default) while simultaneously holding many WebSocket
// connections open (250 by default) to receive real-time push updates,
// like real connected game clients would.
//
// It runs indefinitely until manually stopped with Ctrl+C, printing rolling
// throughput/latency/connection metrics every 5 seconds, followed by a
// final summary report on shutdown. Its purpose is to validate the system
// holds up under sustained concurrent load (Phase 5 load testing), not to
// seed initial data.
//
// Run cmd/seed first if you want the leaderboard to start from a known
// baseline of users — the simulator assumes the service and its
// dependencies (Redis, Postgres, Kafka) are already running.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
)

const (
	numUsers       = 10000 // total user pool to draw from
	writesPerSec   = 100   // score updates to fire per second
	wsConnections  = 250   // concurrent WebSocket connections to hold open
	reportInterval = 5 * time.Second
)

var (
	baseURL string
	apiKey  string
)

//  Metrics

var (
	totalWrites    atomic.Int64
	successWrites  atomic.Int64
	failedWrites   atomic.Int64
	totalPushes    atomic.Int64
	activeWS       atomic.Int64
	totalLatencyMs atomic.Int64
)

//  Entry point

func main() {
	godotenv.Load()

	baseURL = getEnv("BASE_URL", "http://localhost:8080")
	apiKey = mustGetEnv("API_KEY")

	log.Printf("simulator starting")
	log.Printf("  target:      %s", baseURL)
	log.Printf("  users:       %d", numUsers)
	log.Printf("  writes/sec:  %d", writesPerSec)
	log.Printf("  ws conns:    %d", wsConnections)

	stop := make(chan struct{})

	// Catch Ctrl+C for a clean shutdown
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		log.Println("\nsimulator: shutting down...")
		close(stop)
	}()

	var wg sync.WaitGroup

	// 1. WebSocket clients — connect and hold open
	wg.Add(1)
	go func() {
		defer wg.Done()
		runWSClients(stop)
	}()

	// 2. Score writer — fires HTTP POSTs at writesPerSec
	wg.Add(1)
	go func() {
		defer wg.Done()
		runScoreWriter(stop)
	}()

	// 3. Metrics reporter
	wg.Add(1)
	go func() {
		defer wg.Done()
		runReporter(stop)
	}()

	wg.Wait()
	printFinalReport()
}

//  Score writer

func runScoreWriter(stop chan struct{}) {
	ticker := time.NewTicker(time.Second / time.Duration(writesPerSec))
	defer ticker.Stop()

	client := &http.Client{Timeout: 5 * time.Second}
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			go fireScoreUpdate(client, rng)
		}
	}
}

func fireScoreUpdate(client *http.Client, rng *rand.Rand) {
	userID := fmt.Sprintf("sim-user-%05d", rng.Intn(numUsers)+1)
	points := rng.Intn(500) + 10

	body, _ := json.Marshal(map[string]interface{}{
		"user_id": userID,
		"points":  points,
	})

	start := time.Now()

	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/scores", bytes.NewReader(body))
	if err != nil {
		failedWrites.Add(1)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", apiKey)

	resp, err := client.Do(req)
	totalWrites.Add(1)

	latency := time.Since(start).Milliseconds()
	totalLatencyMs.Add(latency)

	if err != nil || resp.StatusCode != http.StatusOK {
		failedWrites.Add(1)
		if err == nil {
			resp.Body.Close()
		}
		return
	}

	resp.Body.Close()
	successWrites.Add(1)
}

//  WebSocket clients

func runWSClients(stop chan struct{}) {
	var wg sync.WaitGroup

	// Stagger connections so we don't slam the server all at once
	for i := 0; i < wsConnections; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			userID := fmt.Sprintf("ws-watcher-%05d", id+1)
			connectAndListen(userID, stop)
		}(i)

		// Small stagger between each connection
		time.Sleep(10 * time.Millisecond)
	}

	wg.Wait()
}

func connectAndListen(userID string, stop chan struct{}) {
	wsURL := url.URL{
		Scheme:   "ws",
		Host:     stripScheme(baseURL),
		Path:     "/ws",
		RawQuery: "userId=" + userID,
	}

	for {
		select {
		case <-stop:
			return
		default:
		}

		conn, _, err := websocket.DefaultDialer.Dial(wsURL.String(), nil)
		if err != nil {
			log.Printf("ws connect failed for %s: %v — retrying in 2s", userID, err)
			time.Sleep(2 * time.Second)
			continue
		}

		activeWS.Add(1)
		readMessages(conn, userID, stop)
		activeWS.Add(-1)

		select {
		case <-stop:
			return
		default:
			// reconnect
			time.Sleep(500 * time.Millisecond)
		}
	}
}

func readMessages(conn *websocket.Conn, userID string, stop chan struct{}) {
	defer conn.Close()

	done := make(chan struct{})

	go func() {
		defer close(done)
		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				return
			}
			totalPushes.Add(1)
		}
	}()

	select {
	case <-stop:
		conn.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		return
	case <-done:
		return
	}
}

//  Reporter

func runReporter(stop chan struct{}) {
	ticker := time.NewTicker(reportInterval)
	defer ticker.Stop()

	var lastWrites, lastPushes int64
	start := time.Now()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			writes := successWrites.Load()
			failed := failedWrites.Load()
			pushes := totalPushes.Load()
			ws := activeWS.Load()
			total := totalWrites.Load()

			writeDelta := writes - lastWrites
			pushDelta := pushes - lastPushes
			lastWrites = writes
			lastPushes = pushes

			avgLatency := int64(0)
			if total > 0 {
				avgLatency = totalLatencyMs.Load() / total
			}

			elapsed := time.Since(start).Seconds()
			overallRPS := float64(writes) / elapsed

			fmt.Printf("\n[%s]\n", time.Now().Format("15:04:05"))
			fmt.Printf("  writes:    %d total  |  %d ok  |  %d failed\n", total, writes, failed)
			fmt.Printf("  throughput: %.1f rps overall  |  %d writes last %s\n",
				overallRPS, writeDelta, reportInterval)
			fmt.Printf("  ws:        %d active connections  |  %d pushes last %s\n",
				ws, pushDelta, reportInterval)
			fmt.Printf("  latency:   %dms avg per write\n", avgLatency)
		}
	}
}

func printFinalReport() {
	writes := successWrites.Load()
	failed := failedWrites.Load()
	total := totalWrites.Load()
	pushes := totalPushes.Load()

	fmt.Println("\n========= FINAL REPORT =========")
	fmt.Printf("total writes attempted: %d\n", total)
	fmt.Printf("successful writes:      %d\n", writes)
	fmt.Printf("failed writes:          %d\n", failed)
	fmt.Printf("total WS pushes recv:   %d\n", pushes)
	if total > 0 {
		fmt.Printf("success rate:           %.2f%%\n", float64(writes)/float64(total)*100)
		fmt.Printf("avg write latency:      %dms\n", totalLatencyMs.Load()/total)
	}
	fmt.Println("=================================")
}

//  Helpers

func stripScheme(u string) string {
	for _, prefix := range []string{"http://", "https://"} {
		if len(u) > len(prefix) && u[:len(prefix)] == prefix {
			return u[len(prefix):]
		}
	}
	return u
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
		log.Fatalf("required env var %s not set", key)
	}
	return v
}
