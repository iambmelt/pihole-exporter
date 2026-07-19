package server

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/eko/pihole-exporter/internal/pihole"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

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
				c.CollectMetricsAsync(writer, request)
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
