package pihole_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/eko/pihole-exporter/internal/pihole"
)

// transport returns the transport of the client
func transport(t *testing.T, c *pihole.APIClient) *http.Transport {
	t.Helper()
	tr, ok := c.Client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("Transport not *http.Transport")
	}
	if tr.TLSClientConfig == nil {
		t.Fatalf("nil TLSClientConfig")
	}
	return tr
}

// TestNewAPIClient_TLSVerification tests the TLS verification of the client
func TestNewAPIClient_TLSVerification(t *testing.T) {
	tests := []struct {
		name             string
		skipVerify       bool
		wantInsecureSkip bool
	}{
		{"disabled", true, true},
		{"enabled", false, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := pihole.NewAPIClient("https://cloudflare.com", "", time.Second, tc.skipVerify)
			if got := transport(t, c).TLSClientConfig.InsecureSkipVerify; got != tc.wantInsecureSkip {
				t.Errorf("InsecureSkipVerify = %v, want %v", got, tc.wantInsecureSkip)
			}
		})
	}
}

// TestNewAPIClient_DisablesKeepAlives verifies the client opens a fresh
// connection per request. Pooling a keep-alive connection that FTL has closed
// server-side between scrapes makes each request block until the client
// timeout, so the exporter must not reuse connections.
func TestNewAPIClient_DisablesKeepAlives(t *testing.T) {
	c := pihole.NewAPIClient("https://cloudflare.com", "", time.Second, false)
	if !transport(t, c).DisableKeepAlives {
		t.Error("DisableKeepAlives = false, want true")
	}
}

// TestFetchData_ReauthAfterSessionRejected verifies that a session rejected
// server-side (as happens when Pi-hole restarts) is dropped so the next request
// re-authenticates, rather than resending the dead SID until the local validity
// window expires.
func TestFetchData_ReauthAfterSessionRejected(t *testing.T) {
	var mu sync.Mutex
	authCount := 0
	validSID := ""

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/auth" {
			mu.Lock()
			authCount++
			validSID = fmt.Sprintf("sid-%d", authCount)
			sid := validSID
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"session":{"valid":true,"sid":%q,"validity":1800}}`, sid)
			return
		}

		mu.Lock()
		accepted := r.Header.Get("X-FTL-SID") == validSID
		mu.Unlock()
		if !accepted {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{}`)
	}))
	defer server.Close()

	c := pihole.NewAPIClient(server.URL, "secret", 5*time.Second, false)
	var out map[string]any

	if err := c.FetchData("/api/stats/summary", &out); err != nil {
		t.Fatalf("initial fetch: %v", err)
	}

	// Simulate a Pi-hole restart: the cached SID is no longer accepted.
	mu.Lock()
	validSID = "gone"
	mu.Unlock()

	// This request sends the stale SID and is rejected.
	if err := c.FetchData("/api/stats/summary", &out); err == nil {
		t.Fatal("expected an error when the session is rejected")
	}

	// The following request must re-authenticate and succeed.
	if err := c.FetchData("/api/stats/summary", &out); err != nil {
		t.Fatalf("fetch after re-authentication: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if authCount != 2 {
		t.Fatalf("expected 2 authentications, got %d", authCount)
	}
}
