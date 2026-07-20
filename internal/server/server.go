package server

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/eko/pihole-exporter/internal/pihole"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
)

// metricsIdleTimeout bounds how long an idle keep-alive connection to /metrics
// is kept open. It MUST stay below the 15s Prometheus scrape interval so an
// idle connection is always closed between scrapes: left open, a connection
// over the port-publishing NAT path goes stale and the next inbound scrape
// hangs until its timeout (up==0) until the container is restarted.
const metricsIdleTimeout = 10 * time.Second

// Server is the struct for the HTTP server.
type Server struct {
	httpServer *http.Server
}

// NewServer method initializes a new HTTP server instance and associates
// the different routes that will be used by Prometheus (metrics) or for monitoring (readiness, liveness).
func NewServer(addr string, port uint16, clients []*pihole.Client) *Server {
	mux := http.NewServeMux()
	httpServer := &http.Server{
		Addr:    fmt.Sprintf("%s:%d", addr, port),
		Handler: mux,
		// Close idle keep-alive connections below the 15s Prometheus scrape
		// interval so a connection is never left idle long enough for the
		// intermediary NAT path (docker-proxy publishing the port) to go stale
		// on it. A stale keep-alive hangs the next inbound scrape until the
		// scrape timeout and shows up as up==0 until the container is
		// restarted; expiring the connection between scrapes makes Prometheus
		// redial fresh each time. This is the server-side mirror of the
		// client-side DisableKeepAlives on the upstream Pi-hole calls.
		IdleTimeout: metricsIdleTimeout,
		// Bound the time spent reading request headers (slowloris hardening).
		ReadHeaderTimeout: 5 * time.Second,
		// WriteTimeout is intentionally unset: the /metrics response time is
		// already bounded by the per-call API-client timeout applied to each
		// upstream Pi-hole request, so the handler cannot hang indefinitely
		// writing a response, and a fixed write deadline would risk cutting off
		// a slow-but-successful collection.
	}

	s := &Server{
		httpServer: httpServer,
	}

	mux.HandleFunc("/metrics", func(writer http.ResponseWriter, request *http.Request) {
		log.Debugf("request.Header: %+v\n", request.Header)

		// Each client's CollectMetricsAsync sends exactly one status on its own
		// buffered (cap-1) Status channel. Run one collector goroutine per client
		// and let the loop below be the single consumer, reading exactly one
		// status per client.
		var wg sync.WaitGroup
		for _, client := range clients {
			wg.Add(1)
			go func(c *pihole.Client) {
				defer wg.Done()
				c.CollectMetricsAsync()
			}(client)
		}

		// This loop is the ONLY receiver of Status. The previous version wrapped
		// collection in a 10s context and, on timeout, started a second goroutine
		// that also received from the same channel to "prevent blocking". That
		// left two receivers racing for the single value: when the discard
		// goroutine won, this loop blocked forever, the handler never reached
		// ServeHTTP, and no response was written — so every Prometheus scrape and
		// the in-container /metrics health check hung until the container was
		// restarted, with no log line because the collection itself had
		// succeeded. Per-request latency is already bounded by the API client
		// timeout applied to each upstream call, so no separate handler timeout
		// is needed.
		for _, client := range clients {
			status := <-client.Status
			if status.Status != pihole.MetricsCollectionSuccess {
				log.Warnf("An error occurred while contacting %s: %+v\n", client.GetHostname(), status.Err)
			}
		}
		wg.Wait()

		promhttp.Handler().ServeHTTP(writer, request)
	})

	mux.Handle("/readiness", s.readinessHandler())
	mux.Handle("/liveness", s.livenessHandler())

	return s
}

// ListenAndServe method serves HTTP requests.
func (s *Server) ListenAndServe() error {
	return s.httpServer.ListenAndServe()
}

// Stop method stops the HTTP server (so the exporter become unavailable).
func (s *Server) Stop() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	s.httpServer.Shutdown(ctx)
}

func (s *Server) readinessHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		status := http.StatusNotFound
		if s.isReady() {
			status = http.StatusOK
		}
		w.WriteHeader(status)
	}
}

func (s *Server) livenessHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusOK)
	}
}

func (s *Server) isReady() bool {
	return s.httpServer != nil
}
