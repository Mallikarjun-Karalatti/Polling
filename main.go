package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// ---------- Domain Constants & Types ----------

const (
	DefaultPort       = "8082"
	DefaultTimeoutMs  = 25000
	MaxWaitMs         = 30000
	DefaultStepMs     = 3000
	ClientLongTimeout = 10000
)

var Lifecycle = []string{
	"PLACED",
	"ACCEPTED",
	"PREPARING",
	"OUT_FOR_DELIVERY",
	"DELIVERED",
}

// OrderSnapshot represents the JSON payload returned to polling clients.
type OrderSnapshot struct {
	OrderID   string `json:"orderId"`
	Status    string `json:"status"`
	Version   int    `json:"version"`
	ChangedAt int64  `json:"changedAt"`
}

// TrackedOrder represents a live order in the kitchen simulation.
// All reads and writes to step, version, and changedAt are protected by mu.
// Long-polling goroutines await on notify; advance() closes oldNotify to wake them all up (signalAll).
type TrackedOrder struct {
	mu        sync.RWMutex
	id        string
	step      int
	version   int
	changedAt int64
	stepMs    int64
	notify    chan struct{}
	done      chan struct{}
}

func newTrackedOrder(id string, stepMs int64) *TrackedOrder {
	return &TrackedOrder{
		id:        id,
		step:      0,
		version:   1,
		changedAt: time.Now().UnixMilli(),
		stepMs:    stepMs,
		notify:    make(chan struct{}),
		done:      make(chan struct{}),
	}
}

func (o *TrackedOrder) isDone() bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.step >= len(Lifecycle)-1
}

func (o *TrackedOrder) snapshot() OrderSnapshot {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.snapshotLocked()
}

func (o *TrackedOrder) snapshotLocked() OrderSnapshot {
	return OrderSnapshot{
		OrderID:   o.id,
		Status:    Lifecycle[o.step],
		Version:   o.version,
		ChangedAt: o.changedAt,
	}
}

// advance advances the order to the next lifecycle step and wakes up ALL waiting long-poll requests.
func (o *TrackedOrder) advance() {
	o.mu.Lock()
	if o.step < len(Lifecycle)-1 {
		o.step++
		o.version++
		o.changedAt = time.Now().UnixMilli()
	}
	oldNotify := o.notify
	o.notify = make(chan struct{})
	isDone := o.step >= len(Lifecycle)-1
	snap := o.snapshotLocked()
	o.mu.Unlock()

	// Signal all waiting long-poll clients by closing the current notification channel
	close(oldNotify)

	if isDone {
		select {
		case <-o.done:
		default:
			close(o.done)
		}
	}

	log.Printf("   [kitchen] %s -> %s (v%d)", snap.OrderID, snap.Status, snap.Version)
}

// ---------- Tracking Server ----------

type Server struct {
	mu              sync.RWMutex
	orders          map[string]*TrackedOrder
	seq             int64
	totalRequests   int64
	simulate503Rate int32 // for stretch backoff demo
}

func NewServer() *Server {
	return &Server{
		orders: make(map[string]*TrackedOrder),
		seq:    100,
	}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleRoot)
	mux.HandleFunc("/orders", s.handleOrders)
	mux.HandleFunc("/orders/", s.handleOrderSubpath)
	mux.HandleFunc("/debug/stats", s.handleStats)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&s.totalRequests, 1)
		mux.ServeHTTP(w, r)
	})
}

func (s *Server) handleRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.Error(w, `{"error":"Not found"}`, http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"service":"QuickBite Tracking Server","status":"UP"}`))
}

func (s *Server) handleOrders(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, `{"error":"Method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}
	s.createOrder(w, r)
}

func (s *Server) handleOrderSubpath(w http.ResponseWriter, r *http.Request) {
	// Path format: /orders/{id}/status
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) != 3 || parts[0] != "orders" || parts[2] != "status" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"Not found"}`))
		return
	}

	orderID := parts[1]
	s.mu.RLock()
	order, exists := s.orders[orderID]
	s.mu.RUnlock()

	if !exists {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(fmt.Sprintf(`{"error":"Unknown order %s"}`, orderID)))
		return
	}

	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, `{"error":"Method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	// Simulated 503 check for stretch demonstration
	if atomic.LoadInt32(&s.simulate503Rate) > 0 {
		rate := atomic.LoadInt32(&s.simulate503Rate)
		if rand.Int31n(100) < rate {
			w.Header().Set("Retry-After", "1")
			http.Error(w, `{"error":"Service temporarily overloaded (simulated 503)"}`, http.StatusServiceUnavailable)
			return
		}
	}

	query := r.URL.Query()
	if query.Has("sinceVersion") {
		s.longPoll(w, r, order)
	} else {
		s.shortPoll(w, r, order)
	}
}

func (s *Server) createOrder(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, `{"error":"Cannot read request body"}`, http.StatusBadRequest)
		return
	}

	stepMs := int64(DefaultStepMs)
	if len(bytes.TrimSpace(body)) > 0 {
		var req struct {
			StepMs *int64 `json:"stepMs"`
		}
		if err := json.Unmarshal(body, &req); err == nil && req.StepMs != nil && *req.StepMs > 0 {
			stepMs = *req.StepMs
		}
	}

	newID := fmt.Sprintf("ord_%d", atomic.AddInt64(&s.seq, 1))
	order := newTrackedOrder(newID, stepMs)

	s.mu.Lock()
	s.orders[newID] = order
	s.mu.Unlock()

	// Start kitchen simulation for this order
	s.scheduleKitchen(order, stepMs)

	snap := order.snapshot()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(snap)
}

func (s *Server) scheduleKitchen(order *TrackedOrder, stepMs int64) {
	go func() {
		for !order.isDone() {
			// Randomize step duration between 0.5x and 1.5x of stepMs
			factor := 0.5 + rand.Float64()*1.0
			delay := time.Duration(float64(stepMs)*factor) * time.Millisecond
			time.Sleep(delay)
			order.advance()
		}
	}()
}

// shortPoll immediately returns the current order status.
func (s *Server) shortPoll(w http.ResponseWriter, r *http.Request, order *TrackedOrder) {
	snap := order.snapshot()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(snap)
}

// longPoll implements TODO 1: holds request until version > sinceVersion or timeout passes.
func (s *Server) longPoll(w http.ResponseWriter, r *http.Request, order *TrackedOrder) {
	q := r.URL.Query()
	sinceVersionStr := q.Get("sinceVersion")
	sinceVersion, err := strconv.Atoi(sinceVersionStr)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"Invalid or missing sinceVersion"}`))
		return
	}

	timeoutMs := int64(DefaultTimeoutMs)
	if q.Has("timeoutMs") {
		val, err := strconv.ParseInt(q.Get("timeoutMs"), 10, 64)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"Invalid timeoutMs"}`))
			return
		}
		timeoutMs = val
	}

	if timeoutMs > MaxWaitMs {
		timeoutMs = MaxWaitMs
	} else if timeoutMs < 0 {
		timeoutMs = 0
	}

	deadline := time.Now().Add(time.Duration(timeoutMs) * time.Millisecond)

	for {
		order.mu.RLock()
		currentVersion := order.version
		if currentVersion > sinceVersion {
			snap := order.snapshotLocked()
			order.mu.RUnlock()
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(snap)
			return
		}
		notifyCh := order.notify
		order.mu.RUnlock()

		remaining := time.Until(deadline)
		if remaining <= 0 {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		select {
		case <-notifyCh:
			// kitchen advanced! Loop around and re-check version under lock
		case <-time.After(remaining):
			w.WriteHeader(http.StatusNoContent)
			return
		case <-r.Context().Done():
			// Client disconnected
			return
		}
	}
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	count := len(s.orders)
	s.mu.RUnlock()
	res := map[string]interface{}{
		"activeOrders":  count,
		"totalRequests": atomic.LoadInt64(&s.totalRequests),
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(res)
}

// ---------- Client Scorecard & Polling Clients ----------

type Scorecard struct {
	Mode         string  `json:"mode"`
	IntervalMs   int64   `json:"intervalMs,omitempty"`
	TotalTimeSec float64 `json:"totalTimeSec"`
	Requests     int     `json:"requests"`
	Wasted       int     `json:"wasted"`
	AvgLagMs     int64   `json:"avgLagMs"`
	MissedSteps  int     `json:"missedSteps"`
}

type ClientLogger func(format string, args ...interface{})

func defaultLogger(format string, args ...interface{}) {
	fmt.Printf(format+"\n", args...)
}

func silentLogger(format string, args ...interface{}) {}

// ShortPoll executes TODO 2: poll every intervalMs, record wasted calls, lag, missed steps.
func ShortPoll(ctx context.Context, client *http.Client, baseURL, orderID string, intervalMs int64, logFn ClientLogger) (*Scorecard, error) {
	if logFn == nil {
		logFn = defaultLogger
	}
	start := time.Now()
	requests := 0
	wasted := 0
	missed := 0
	var lags []int64
	lastVersion := 0

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		requests++
		reqURL := fmt.Sprintf("%s/orders/%s/status", baseURL, orderID)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
		if err != nil {
			return nil, err
		}

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
		}

		var snap OrderSnapshot
		if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
			_ = resp.Body.Close()
			return nil, err
		}
		_ = resp.Body.Close()

		if snap.Version == lastVersion {
			wasted++
		} else {
			lag := time.Now().UnixMilli() - snap.ChangedAt
			if lag < 0 {
				lag = 0
			}
			if lastVersion > 0 && snap.Version > lastVersion+1 {
				skipped := snap.Version - lastVersion - 1
				missed += skipped
				logFn("   !! skipped %d status change(s) between polls", skipped)
			}
			lags = append(lags, lag)
			logFn("%s  %-17s v%d   (noticed %4d ms late)",
				time.Now().Format("15:04:05"), snap.Status, snap.Version, lag)
			lastVersion = snap.Version
		}

		if snap.Status == "DELIVERED" {
			break
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Duration(intervalMs) * time.Millisecond):
		}
	}

	var avgLag int64
	if len(lags) > 0 {
		var sum int64
		for _, l := range lags {
			sum += l
		}
		avgLag = sum / int64(len(lags))
	}

	return &Scorecard{
		Mode:         fmt.Sprintf("short %d ms", intervalMs),
		IntervalMs:   intervalMs,
		TotalTimeSec: time.Since(start).Seconds(),
		Requests:     requests,
		Wasted:       wasted,
		AvgLagMs:     avgLag,
		MissedSteps:  missed,
	}, nil
}

// LongPoll executes TODO 3: long poll with sinceVersion, no sleep, handles 204.
func LongPoll(ctx context.Context, client *http.Client, baseURL, orderID string, timeoutMs int64, logFn ClientLogger) (*Scorecard, error) {
	if logFn == nil {
		logFn = defaultLogger
	}
	start := time.Now()
	requests := 0
	wasted := 0
	missed := 0
	var lags []int64
	lastVersion := 0

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		requests++
		reqURL := fmt.Sprintf("%s/orders/%s/status?sinceVersion=%d&timeoutMs=%d", baseURL, orderID, lastVersion, timeoutMs)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
		if err != nil {
			return nil, err
		}

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}

		if resp.StatusCode == http.StatusNoContent {
			wasted++
			_ = resp.Body.Close()
			continue
		}

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
		}

		var snap OrderSnapshot
		if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
			_ = resp.Body.Close()
			return nil, err
		}
		_ = resp.Body.Close()

		lag := time.Now().UnixMilli() - snap.ChangedAt
		if lag < 0 {
			lag = 0
		}
		if lastVersion > 0 && snap.Version > lastVersion+1 {
			skipped := snap.Version - lastVersion - 1
			missed += skipped
			logFn("   !! skipped %d status change(s) between polls", skipped)
		}
		lags = append(lags, lag)
		logFn("%s  %-17s v%d   (noticed %4d ms late)",
			time.Now().Format("15:04:05"), snap.Status, snap.Version, lag)
		lastVersion = snap.Version

		if snap.Status == "DELIVERED" {
			break
		}
	}

	var avgLag int64
	if len(lags) > 0 {
		var sum int64
		for _, l := range lags {
			sum += l
		}
		avgLag = sum / int64(len(lags))
	}

	return &Scorecard{
		Mode:         "long",
		TotalTimeSec: time.Since(start).Seconds(),
		Requests:     requests,
		Wasted:       wasted,
		AvgLagMs:     avgLag,
		MissedSteps:  missed,
	}, nil
}

// CreateOrderHelper helper to place an order via POST /orders
func CreateOrderHelper(client *http.Client, baseURL string, stepMs int64) (string, error) {
	reqBody := fmt.Sprintf(`{"stepMs":%d}`, stepMs)
	resp, err := client.Post(baseURL+"/orders", "application/json", strings.NewReader(reqBody))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("failed to create order: HTTP %d: %s", resp.StatusCode, string(b))
	}

	var snap OrderSnapshot
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		return "", err
	}
	return snap.OrderID, nil
}

// ---------- Stretch: Short Polling with Exponential Backoff on 503 ----------

func ShortPollWithBackoff(ctx context.Context, client *http.Client, baseURL, orderID string, baseIntervalMs, maxIntervalMs int64, logFn ClientLogger) (*Scorecard, error) {
	if logFn == nil {
		logFn = defaultLogger
	}
	start := time.Now()
	requests := 0
	wasted := 0
	missed := 0
	var lags []int64
	lastVersion := 0
	currentInterval := baseIntervalMs
	backoffCount := 0

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		requests++
		reqURL := fmt.Sprintf("%s/orders/%s/status", baseURL, orderID)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
		if err != nil {
			return nil, err
		}

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}

		if resp.StatusCode == http.StatusServiceUnavailable {
			_ = resp.Body.Close()
			backoffCount++
			// Exponential backoff: base * 2^backoffCount + jitter
			currentInterval = baseIntervalMs * (1 << backoffCount)
			if currentInterval > maxIntervalMs {
				currentInterval = maxIntervalMs
			}
			jitter := rand.Int63n(currentInterval / 4)
			sleepDur := time.Duration(currentInterval+jitter) * time.Millisecond
			logFn("   [503 Received] Backing off for %v (backoff level %d)", sleepDur, backoffCount)
			time.Sleep(sleepDur)
			continue
		}

		// Reset backoff on successful response
		backoffCount = 0
		currentInterval = baseIntervalMs

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
		}

		var snap OrderSnapshot
		if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
			_ = resp.Body.Close()
			return nil, err
		}
		_ = resp.Body.Close()

		if snap.Version == lastVersion {
			wasted++
		} else {
			lag := time.Now().UnixMilli() - snap.ChangedAt
			if lag < 0 {
				lag = 0
			}
			if lastVersion > 0 && snap.Version > lastVersion+1 {
				skipped := snap.Version - lastVersion - 1
				missed += skipped
				logFn("   !! skipped %d status change(s) between polls", skipped)
			}
			lags = append(lags, lag)
			logFn("%s  %-17s v%d   (noticed %4d ms late)",
				time.Now().Format("15:04:05"), snap.Status, snap.Version, lag)
			lastVersion = snap.Version
		}

		if snap.Status == "DELIVERED" {
			break
		}

		time.Sleep(time.Duration(baseIntervalMs) * time.Millisecond)
	}

	var avgLag int64
	if len(lags) > 0 {
		var sum int64
		for _, l := range lags {
			sum += l
		}
		avgLag = sum / int64(len(lags))
	}

	return &Scorecard{
		Mode:         fmt.Sprintf("backoff %d ms", baseIntervalMs),
		IntervalMs:   baseIntervalMs,
		TotalTimeSec: time.Since(start).Seconds(),
		Requests:     requests,
		Wasted:       wasted,
		AvgLagMs:     avgLag,
		MissedSteps:  missed,
	}, nil
}

// ---------- Main & CLI Handlers ----------

func printScorecard(sc *Scorecard) {
	fmt.Printf("\n================ SCORECARD (%s) ================\n", sc.Mode)
	fmt.Printf("Total time          : %.1f s\n", sc.TotalTimeSec)
	fmt.Printf("HTTP requests       : %d\n", sc.Requests)
	fmt.Printf("Wasted requests     : %d  (no news)\n", sc.Wasted)
	fmt.Printf("Avg notice delay    : %d ms  (how stale the UI was)\n", sc.AvgLagMs)
	fmt.Printf("Missed status steps : %d\n", sc.MissedSteps)
}

func runExperiment(baseURL string, stepMs int64) {
	fmt.Println("=====================================================================")
	fmt.Println("RUNNING THE EXPERIMENT: SHORT POLLING VS LONG POLLING")
	fmt.Println("=====================================================================")
	client := &http.Client{Timeout: 45 * time.Second}
	ctx := context.Background()

	modes := []struct {
		isLong bool
		ms     int64
	}{
		{isLong: false, ms: 500},
		{isLong: false, ms: 2000},
		{isLong: false, ms: 5000},
		{isLong: true, ms: 10000},
	}

	var results []*Scorecard

	for _, m := range modes {
		var modeName string
		if m.isLong {
			modeName = "long"
		} else {
			modeName = fmt.Sprintf("short %d ms", m.ms)
		}
		fmt.Printf("\n>>> Starting experiment: %s\n", modeName)
		orderID, err := CreateOrderHelper(client, baseURL, stepMs)
		if err != nil {
			log.Fatalf("Failed to create order for %s: %v", modeName, err)
		}
		fmt.Printf("Placed order %s -- tracking with %s\n", orderID, modeName)

		var sc *Scorecard
		if m.isLong {
			sc, err = LongPoll(ctx, client, baseURL, orderID, m.ms, defaultLogger)
		} else {
			sc, err = ShortPoll(ctx, client, baseURL, orderID, m.ms, defaultLogger)
		}
		if err != nil {
			log.Fatalf("Experiment failed for %s: %v", modeName, err)
		}
		printScorecard(sc)
		results = append(results, sc)
	}

	fmt.Println("\n\n================ FINAL COMPARISON TABLE ================")
	fmt.Println("| Mode | HTTP requests | Wasted | Avg notice delay | Missed steps |")
	fmt.Println("|---|---|---|---|---|")
	for _, r := range results {
		fmt.Printf("| %-13s | %-13d | %-6d | %-16s | %-12d |\n",
			r.Mode, r.Requests, r.Wasted, fmt.Sprintf("%d ms", r.AvgLagMs), r.MissedSteps)
	}
	fmt.Println("========================================================")
}

func runConcurrencyTest(baseURL string, clientCount int) {
	fmt.Printf("\nStarting Stretch Concurrency Test: %d parallel long-poll clients...\n", clientCount)
	client := &http.Client{Timeout: 45 * time.Second}
	orderID, err := CreateOrderHelper(client, baseURL, 8000)
	if err != nil {
		log.Fatalf("Failed to create order: %v", err)
	}

	var wg sync.WaitGroup
	var successCount int64
	start := time.Now()

	for i := 0; i < clientCount; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			reqURL := fmt.Sprintf("%s/orders/%s/status?sinceVersion=1&timeoutMs=15000", baseURL, orderID)
			resp, err := client.Get(reqURL)
			if err != nil {
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				atomic.AddInt64(&successCount, 1)
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)
	fmt.Printf("Concurrency Test Complete: %d/%d clients successfully received real-time push in %.2f s\n",
		successCount, clientCount, elapsed.Seconds())
}

func main() {
	if len(os.Args) < 2 || os.Args[1] == "server" {
		port := DefaultPort
		if p := os.Getenv("PORT"); p != "" {
			port = p
		}
		if len(os.Args) >= 3 {
			port = os.Args[2]
		}
		srv := NewServer()
		httpSrv := &http.Server{
			Addr:    ":" + port,
			Handler: srv.Routes(),
		}

		go func() {
			log.Printf("Tracking server on http://localhost:%s", port)
			if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Fatalf("Server error: %v", err)
			}
		}()

		stop := make(chan os.Signal, 1)
		signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
		<-stop
		log.Println("Shutting down server...")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(ctx)
		return
	}

	command := os.Args[1]
	baseURL := "http://localhost:" + DefaultPort

	switch command {
	case "client":
		if len(os.Args) < 3 {
			fmt.Println("Usage: go run main.go client <short [intervalMs]|long>")
			return
		}
		mode := os.Args[2]
		client := &http.Client{Timeout: 45 * time.Second}
		orderID, err := CreateOrderHelper(client, baseURL, DefaultStepMs)
		if err != nil {
			log.Fatalf("Failed to create order: %v", err)
		}
		fmt.Printf("Placed order %s -- tracking with %s polling\n\n", orderID, strings.ToUpper(mode))

		var sc *Scorecard
		if mode == "long" {
			sc, err = LongPoll(context.Background(), client, baseURL, orderID, ClientLongTimeout, defaultLogger)
		} else {
			interval := int64(2000)
			if len(os.Args) >= 4 {
				if parsed, err := strconv.ParseInt(os.Args[3], 10, 64); err == nil {
					interval = parsed
				}
			}
			sc, err = ShortPoll(context.Background(), client, baseURL, orderID, interval, defaultLogger)
		}
		if err != nil {
			log.Fatalf("Polling error: %v", err)
		}
		printScorecard(sc)

	case "run-experiment":
		stepMs := int64(DefaultStepMs)
		if len(os.Args) >= 3 {
			if parsed, err := strconv.ParseInt(os.Args[2], 10, 64); err == nil {
				stepMs = parsed
			}
		}
		runExperiment(baseURL, stepMs)

	case "stretch-concurrency":
		clients := 200
		if len(os.Args) >= 3 {
			if parsed, err := strconv.Atoi(os.Args[2]); err == nil {
				clients = parsed
			}
		}
		runConcurrencyTest(baseURL, clients)

	case "stretch-backoff":
		client := &http.Client{Timeout: 45 * time.Second}
		orderID, err := CreateOrderHelper(client, baseURL, DefaultStepMs)
		if err != nil {
			log.Fatalf("Failed to create order: %v", err)
		}
		fmt.Printf("Tracking %s with exponential backoff on simulated 503...\n", orderID)
		sc, err := ShortPollWithBackoff(context.Background(), client, baseURL, orderID, 1000, 8000, defaultLogger)
		if err != nil {
			log.Fatalf("Error: %v", err)
		}
		printScorecard(sc)

	default:
		fmt.Printf("Unknown command %q. Available: server, client, run-experiment, stretch-concurrency, stretch-backoff\n", command)
	}
}
