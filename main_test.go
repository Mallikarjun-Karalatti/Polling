package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTestServer() (*Server, *httptest.Server) {
	srv := NewServer()
	ts := httptest.NewServer(srv.Routes())
	return srv, ts
}

func TestServerHealthAndCreate(t *testing.T) {
	_, ts := newTestServer()
	defer ts.Close()

	// Root endpoint check
	resp, err := http.Get(ts.URL + "/")
	if err != nil {
		t.Fatalf("failed to GET /: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 on /, got %d", resp.StatusCode)
	}

	// Create order
	orderID, err := CreateOrderHelper(ts.Client(), ts.URL, 500)
	if err != nil {
		t.Fatalf("failed to create order: %v", err)
	}
	if orderID == "" {
		t.Fatalf("expected non-empty order ID")
	}

	// Short poll immediate status
	statusURL := fmt.Sprintf("%s/orders/%s/status", ts.URL, orderID)
	resp, err = http.Get(statusURL)
	if err != nil {
		t.Fatalf("failed to get status: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 on status, got %d", resp.StatusCode)
	}

	var snap OrderSnapshot
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		t.Fatalf("failed to decode snap: %v", err)
	}

	if snap.OrderID != orderID {
		t.Errorf("expected orderId %s, got %s", orderID, snap.OrderID)
	}
	if snap.Version < 1 {
		t.Errorf("expected version >= 1, got %d", snap.Version)
	}
}

func TestLongPoll_ImmediateWhenNewsAvailable(t *testing.T) {
	_, ts := newTestServer()
	defer ts.Close()

	orderID, err := CreateOrderHelper(ts.Client(), ts.URL, 5000)
	if err != nil {
		t.Fatalf("failed to create order: %v", err)
	}

	start := time.Now()
	url := fmt.Sprintf("%s/orders/%s/status?sinceVersion=0&timeoutMs=5000", ts.URL, orderID)
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("failed long poll: %v", err)
	}
	defer resp.Body.Close()

	elapsed := time.Since(start)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if elapsed > 300*time.Millisecond {
		t.Errorf("sinceVersion=0 should return immediately, took %v", elapsed)
	}

	var snap OrderSnapshot
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		t.Fatalf("failed to decode snap: %v", err)
	}
	if snap.Version != 1 {
		t.Errorf("expected version 1, got %d", snap.Version)
	}
	if snap.Status != "PLACED" {
		t.Errorf("expected PLACED, got %s", snap.Status)
	}
}

func TestLongPoll_WaitsForNextVersion(t *testing.T) {
	_, ts := newTestServer()
	defer ts.Close()

	// stepMs = 300ms
	orderID, err := CreateOrderHelper(ts.Client(), ts.URL, 300)
	if err != nil {
		t.Fatalf("failed to create order: %v", err)
	}

	// Fetch current version
	statusURL := fmt.Sprintf("%s/orders/%s/status", ts.URL, orderID)
	resp, err := http.Get(statusURL)
	if err != nil {
		t.Fatalf("failed to get status: %v", err)
	}
	var initialSnap OrderSnapshot
	_ = json.NewDecoder(resp.Body).Decode(&initialSnap)
	_ = resp.Body.Close()

	// Long poll asking for updates beyond current version
	start := time.Now()
	pollURL := fmt.Sprintf("%s/orders/%s/status?sinceVersion=%d&timeoutMs=5000", ts.URL, orderID, initialSnap.Version)
	resp, err = http.Get(pollURL)
	if err != nil {
		t.Fatalf("failed to long poll: %v", err)
	}
	defer resp.Body.Close()
	elapsed := time.Since(start)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var newSnap OrderSnapshot
	if err := json.NewDecoder(resp.Body).Decode(&newSnap); err != nil {
		t.Fatalf("failed to decode snap: %v", err)
	}

	if newSnap.Version <= initialSnap.Version {
		t.Errorf("expected version > %d, got %d", initialSnap.Version, newSnap.Version)
	}
	if elapsed < 80*time.Millisecond {
		t.Errorf("long poll should have waited for kitchen update, but returned after %v", elapsed)
	}
}

func TestLongPoll_TimeoutReturns204(t *testing.T) {
	_, ts := newTestServer()
	defer ts.Close()

	// Very slow order (60s stepMs)
	orderID, err := CreateOrderHelper(ts.Client(), ts.URL, 60000)
	if err != nil {
		t.Fatalf("failed to create order: %v", err)
	}

	start := time.Now()
	pollURL := fmt.Sprintf("%s/orders/%s/status?sinceVersion=1&timeoutMs=500", ts.URL, orderID)
	resp, err := http.Get(pollURL)
	if err != nil {
		t.Fatalf("failed to long poll: %v", err)
	}
	defer resp.Body.Close()
	elapsed := time.Since(start)

	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("expected 204 No Content, got %d", resp.StatusCode)
	}

	if elapsed < 400*time.Millisecond || elapsed > 1500*time.Millisecond {
		t.Errorf("expected timeout of ~500ms, took %v", elapsed)
	}
}

func TestLongPoll_MultipleClientsWakeUpSimultaneously(t *testing.T) {
	_, ts := newTestServer()
	defer ts.Close()

	orderID, err := CreateOrderHelper(ts.Client(), ts.URL, 400)
	if err != nil {
		t.Fatalf("failed to create order: %v", err)
	}

	clientCount := 5
	var wg sync.WaitGroup
	var successCount int64

	for i := 0; i < clientCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pollURL := fmt.Sprintf("%s/orders/%s/status?sinceVersion=1&timeoutMs=4000", ts.URL, orderID)
			resp, err := http.Get(pollURL)
			if err != nil {
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				var snap OrderSnapshot
				if err := json.NewDecoder(resp.Body).Decode(&snap); err == nil && snap.Version > 1 {
					atomic.AddInt64(&successCount, 1)
				}
			}
		}()
	}

	wg.Wait()
	if int(successCount) != clientCount {
		t.Fatalf("expected all %d waiting clients to receive update, but only %d did", clientCount, successCount)
	}
}

func TestErrorHandling(t *testing.T) {
	_, ts := newTestServer()
	defer ts.Close()

	orderID, err := CreateOrderHelper(ts.Client(), ts.URL, 1000)
	if err != nil {
		t.Fatalf("failed to create order: %v", err)
	}

	// Bad sinceVersion -> 400
	resp, err := http.Get(fmt.Sprintf("%s/orders/%s/status?sinceVersion=invalid", ts.URL, orderID))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 for bad sinceVersion, got %d", resp.StatusCode)
	}

	// Bad timeoutMs -> 400
	resp, err = http.Get(fmt.Sprintf("%s/orders/%s/status?sinceVersion=1&timeoutMs=bad", ts.URL, orderID))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 for bad timeoutMs, got %d", resp.StatusCode)
	}

	// Unknown order -> 404
	resp, err = http.Get(fmt.Sprintf("%s/orders/ord_nonexistent/status", ts.URL))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404 for nonexistent order, got %d", resp.StatusCode)
	}

	// Invalid method -> 405
	req, _ := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/orders/%s/status", ts.URL, orderID), nil)
	resp, err = ts.Client().Do(req)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 for PUT status, got %d", resp.StatusCode)
	}
}

func TestShortPollClientSimulation(t *testing.T) {
	_, ts := newTestServer()
	defer ts.Close()

	orderID, err := CreateOrderHelper(ts.Client(), ts.URL, 150)
	if err != nil {
		t.Fatalf("failed to create order: %v", err)
	}

	sc, err := ShortPoll(context.Background(), ts.Client(), ts.URL, orderID, 50, silentLogger)
	if err != nil {
		t.Fatalf("short poll failed: %v", err)
	}

	if sc.Requests < 4 {
		t.Errorf("expected at least 4 requests, got %d", sc.Requests)
	}
	if sc.Wasted < 1 {
		t.Errorf("expected wasted calls at 50ms interval, got %d", sc.Wasted)
	}
}

func TestLongPollClientSimulation(t *testing.T) {
	_, ts := newTestServer()
	defer ts.Close()

	orderID, err := CreateOrderHelper(ts.Client(), ts.URL, 150)
	if err != nil {
		t.Fatalf("failed to create order: %v", err)
	}

	sc, err := LongPoll(context.Background(), ts.Client(), ts.URL, orderID, 2000, silentLogger)
	if err != nil {
		t.Fatalf("long poll failed: %v", err)
	}

	// Lifecycle has 5 statuses: PLACED (v1), ACCEPTED (v2), PREPARING (v3), OUT_FOR_DELIVERY (v4), DELIVERED (v5)
	// Long poll receives exactly 5 updates, 0 wasted calls (since timeout is 2000ms > 150ms step), 0 missed
	if sc.Requests != 5 {
		t.Errorf("expected 5 requests for 5 lifecycle steps, got %d", sc.Requests)
	}
	if sc.Wasted != 0 {
		t.Errorf("expected 0 wasted requests in ideal long poll, got %d", sc.Wasted)
	}
	if sc.MissedSteps != 0 {
		t.Errorf("expected 0 missed steps in long poll, got %d", sc.MissedSteps)
	}
}

func TestStretchExponentialBackoff(t *testing.T) {
	srv, ts := newTestServer()
	defer ts.Close()

	// Simulate 30% 503 error rate
	atomic.StoreInt32(&srv.simulate503Rate, 30)

	orderID, err := CreateOrderHelper(ts.Client(), ts.URL, 150)
	if err != nil {
		t.Fatalf("failed to create order: %v", err)
	}

	sc, err := ShortPollWithBackoff(context.Background(), ts.Client(), ts.URL, orderID, 50, 500, silentLogger)
	if err != nil {
		t.Fatalf("backoff poll failed: %v", err)
	}

	if sc.Requests < 4 {
		t.Errorf("expected requests >= 4, got %d", sc.Requests)
	}
}

