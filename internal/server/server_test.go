package server

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eko/pihole-exporter/config"
	"github.com/eko/pihole-exporter/internal/metrics"
	"github.com/eko/pihole-exporter/internal/pihole"
)

// metricsInit registers the Prometheus metrics exactly once. metrics.Init calls
// prometheus.MustRegister, which panics on a second registration.
var metricsInit sync.Once

// newTestClient builds a Pi-hole client pointed at baseURL with a per-call
// timeout long enough not to cut off the deliberately slow backend below.
func newTestClient(t *testing.T, baseURL string) *pihole.Client {
	t.Helper()

	u, err := url.Parse(baseURL)
	if err != nil {
		t.Fatalf("parse backend URL %q: %v", baseURL, err)
	}
	port, err := strconv.ParseUint(u.Port(), 10, 16)
	if err != nil {
		t.Fatalf("parse backend port %q: %v", u.Port(), err)
	}

	clientConfig := &config.Config{
		PIHoleProtocol: "http",
		PIHoleHostname: u.Hostname(),
		PIHolePort:     uint16(port),
		PIHolePassword: "secret",
	}
	envConfig := &config.EnvConfig{
		// Well above the slow-collection delay so the API client does not abort
		// the request itself; the point of the test is a slow-but-successful
		// collection, not a failed one.
		Timeout: 60 * time.Second,
	}
	return pihole.NewClient(clientConfig, envConfig)
}

// TestMetricsHandler_SlowCollectionStillServes reproduces the handler deadlock
// and proves the fix. Metrics collection is made slower than the 10s cap the
// old handler wrapped it in. The old handler, on that timeout, spawned a second
// goroutine that received from the client's single-value Status channel to
// "discard" the late result, racing the handler's own receive; when the discard
// goroutine won, the handler blocked forever and never wrote a response, so
// every scrape and the /metrics health check hung until a container restart.
//
// Two slow clients make that failure deterministic against the old code: the
// handler's receive loop is already parked on the first client from the start,
// so it wins that client's value, but the second client's discard goroutine is
// enqueued at the 10s timeout — before the loop reaches the second client near
// t=11s — so it steals that value and the loop blocks on the second client
// forever.
//
// The fixed handler has a single receiver and no timeout, so a slow collection
// still yields a complete /metrics response. The test fails (rather than hangs)
// if the handler does not return within a generous budget.
func TestMetricsHandler_SlowCollectionStillServes(t *testing.T) {
	metricsInit.Do(metrics.Init)

	const slowDelay = 11 * time.Second // exceeds the old 10s handler cap

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/auth" {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"session":{"valid":true,"sid":"sid","validity":1800}}`)
			return
		}
		// Delay the first statistics call so the whole collection runs past the
		// old 10s cap while every upstream call still succeeds.
		if r.URL.Path == "/api/stats/summary" {
			time.Sleep(slowDelay)
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{}`)
	}))
	defer backend.Close()

	clients := []*pihole.Client{
		newTestClient(t, backend.URL),
		newTestClient(t, backend.URL),
	}

	srv := NewServer("127.0.0.1", 0, clients)
	handler := srv.httpServer.Handler

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)

	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(rec, req)
		close(done)
	}()

	// Comfortably above slowDelay; only reached if the handler deadlocks.
	select {
	case <-done:
	case <-time.After(slowDelay + 30*time.Second):
		t.Fatal("/metrics handler did not return: it deadlocked on the Status channel")
	}

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if body := rec.Body.String(); !strings.Contains(body, "pihole_") {
		t.Fatalf("response body does not contain a pihole_ series:\n%s", body)
	}
}
