//go:build go1.20
// +build go1.20

package scs

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestFlusher(t *testing.T) {
	t.Parallel()

	sessionManager := New()
	sessionManager.Lifetime = 500 * time.Millisecond

	mux := http.NewServeMux()

	mux.HandleFunc("/get", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, ok := w.(http.Flusher)

		fmt.Fprint(w, ok)
	}))

	ts := newTestServer(t, sessionManager.LoadAndSave(mux))
	defer ts.Close()

	ts.execute(t, "/put")

	_, body := ts.execute(t, "/get")
	if body != "true" {
		t.Errorf("want %q; got %q", "true", body)
	}
}

func TestHijacker(t *testing.T) {
	t.Parallel()

	sessionManager := New()
	sessionManager.Lifetime = 500 * time.Millisecond

	mux := http.NewServeMux()

	mux.HandleFunc("/get", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, ok := w.(http.Hijacker)

		fmt.Fprint(w, ok)
	}))

	ts := newTestServer(t, sessionManager.LoadAndSave(mux))
	defer ts.Close()

	ts.execute(t, "/put")

	_, body := ts.execute(t, "/get")
	if body != "true" {
		t.Errorf("want %q; got %q", "true", body)
	}
}

func TestFlushCommitsSession(t *testing.T) {
	t.Parallel()

	sessionManager := New()
	sessionManager.Lifetime = 500 * time.Millisecond

	mux := http.NewServeMux()

	mux.HandleFunc("/login", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		err := sessionManager.RenewToken(r.Context())
		if err != nil {
			t.Error(err)
		}
		sessionManager.Put(r.Context(), "authenticated", true)

		// Simulate a streaming/SSE handler which flushes the response headers
		// before writing any body data.
		http.NewResponseController(w).Flush()

		fmt.Fprint(w, "streaming")
	}))

	mux.HandleFunc("/check", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ok := sessionManager.GetBool(r.Context(), "authenticated")
		fmt.Fprint(w, ok)
	}))

	ts := newTestServer(t, sessionManager.LoadAndSave(mux))
	defer ts.Close()

	header, body := ts.execute(t, "/login")
	if body != "streaming" {
		t.Errorf("want body %q; got %q", "streaming", body)
	}
	if cookies := header.Values("Set-Cookie"); len(cookies) == 0 {
		t.Fatal("no Set-Cookie header received after flushing a modified session")
	}

	_, body = ts.execute(t, "/check")
	if body != "true" {
		t.Errorf("want authenticated %q; got %q", "true", body)
	}
}

func TestHijackCommitsSession(t *testing.T) {
	t.Parallel()

	sessionManager := New()
	sessionManager.Lifetime = 500 * time.Millisecond

	var committedToken string
	var statusAtHijack Status

	mux := http.NewServeMux()

	mux.HandleFunc("/hijack", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sessionManager.Put(r.Context(), "authenticated", true)

		conn, bufrw, err := http.NewResponseController(w).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()

		statusAtHijack = sessionManager.Status(r.Context())
		committedToken = sessionManager.Token(r.Context())
		if cookies := w.Header().Values("Set-Cookie"); len(cookies) == 0 {
			t.Error("no Set-Cookie header added to the response writer before hijack")
		}

		bufrw.WriteString("HTTP/1.0 200 OK\r\nContent-Length: 2\r\n\r\nok")
		bufrw.Flush()
	}))

	ts := httptest.NewServer(sessionManager.LoadAndSave(mux))
	defer ts.Close()

	conn, err := net.Dial("tcp", ts.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	fmt.Fprint(conn, "GET /hijack HTTP/1.0\r\n\r\n")

	if _, err := io.Copy(io.Discard, conn); err != nil {
		t.Fatal(err)
	}

	if statusAtHijack != Committed {
		t.Errorf("want session status %v at hijack; got %v", Committed, statusAtHijack)
	}
	if committedToken == "" {
		t.Fatal("session was not committed before the connection was hijacked")
	}

	// The store must contain the committed session, so the same token remains
	// usable even though the hijacked handler wrote the response itself.
	b, found, err := sessionManager.Store.Find(committedToken)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("committed session token not found in the store")
	}
	if len(b) == 0 {
		t.Fatal("committed session data is empty")
	}
}

func TestFlushCommitErrorInvokesErrorFunc(t *testing.T) {
	t.Parallel()

	sessionManager := New()
	sessionManager.Lifetime = 500 * time.Millisecond

	sessionManager.Store = &errorCommitStore{}

	var errorFuncCalled bool
	sessionManager.ErrorFunc = func(w http.ResponseWriter, r *http.Request, err error) {
		errorFuncCalled = true
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/flush", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sessionManager.Put(r.Context(), "foo", "bar")
		http.NewResponseController(w).Flush()
	}))

	ts := newTestServer(t, sessionManager.LoadAndSave(mux))
	defer ts.Close()

	ts.execute(t, "/flush")

	if !errorFuncCalled {
		t.Fatal("want ErrorFunc to be called when the commit during Flush fails")
	}
}

type errorCommitStore struct{}

func (s *errorCommitStore) Commit(token string, b []byte, expiry time.Time) error {
	return errors.New("store unavailable")
}

func (s *errorCommitStore) Delete(token string) error {
	return nil
}

func (s *errorCommitStore) Find(token string) (b []byte, found bool, err error) {
	return nil, false, nil
}
