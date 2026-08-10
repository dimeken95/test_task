package httpapi

import (
	"crypto/sha256"
	"crypto/subtle"
	"log/slog"
	"net/http"
	"strings"

	"github.com/dimeken95/test_task/internal/domain"
)

// apiKeyAuth guards the business endpoints with a shared-secret key supplied
// as `Authorization: Bearer <key>` or `X-API-Key: <key>`.
//
// This is deliberately the simplest thing that closes the hole: a real
// deployment would terminate OAuth/JWT at the gateway and pass identity down.
// Probes and /metrics stay open because kubelet and Prometheus scrape them,
// and neither exposes payload data.
//
// With no keys configured the middleware is a pass-through, which keeps the
// demo stack usable; the server logs a warning at startup so that is a
// conscious choice rather than an oversight.
func apiKeyAuth(keys []string, logger *slog.Logger) func(http.Handler) http.Handler {
	// Compare digests, not raw strings: every comparison then runs over the
	// same length regardless of how much of the key was guessed.
	digests := make([][32]byte, 0, len(keys))
	for _, k := range keys {
		if k = strings.TrimSpace(k); k != "" {
			digests = append(digests, sha256.Sum256([]byte(k)))
		}
	}

	return func(next http.Handler) http.Handler {
		if len(digests) == 0 {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !authorised(digests, presentedKey(r)) {
				w.Header().Set("WWW-Authenticate", `Bearer realm="payload-service"`)
				writeError(w, logger, r, domain.ErrUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func presentedKey(r *http.Request) string {
	if h := r.Header.Get("Authorization"); h != "" {
		if key, ok := strings.CutPrefix(h, "Bearer "); ok {
			return strings.TrimSpace(key)
		}
		return ""
	}
	return strings.TrimSpace(r.Header.Get("X-API-Key"))
}

func authorised(digests [][32]byte, presented string) bool {
	if presented == "" {
		return false
	}
	got := sha256.Sum256([]byte(presented))
	// Check every key rather than returning early, so the time taken does not
	// reveal which position matched.
	var ok int
	for _, want := range digests {
		ok |= subtle.ConstantTimeCompare(got[:], want[:])
	}
	return ok == 1
}
