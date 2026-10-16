package main

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type seenReq struct{ method, path, client, run, id, ctype string }

// recStand — заглушки и Orders, которые запоминают заголовки X-Stand-*.
func recStand(t *testing.T) (orders, mocks *httptest.Server, got func() []seenReq) {
	t.Helper()
	var mu sync.Mutex
	var reqs []seenReq
	note := func(r *http.Request) {
		mu.Lock()
		reqs = append(reqs, seenReq{r.Method, r.URL.Path, r.Header.Get("X-Stand-Client"), r.Header.Get("X-Stand-Run"),
			r.Header.Get("X-Stand-Run-Id"), r.Header.Get("Content-Type")})
		mu.Unlock()
	}
	var n int
	orders = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		note(r)
		switch {
		case r.URL.Path == "/healthz":
		case r.Method == "POST" && r.URL.Path == "/orders":
			mu.Lock()
			n++
			id := n
			mu.Unlock()
			w.WriteHeader(201)
			fmt.Fprintf(w, `{"id":"o-%d","status":"rejected"}`, id)
		default:
			fmt.Fprint(w, `{"status":"rejected","amount_cents":0}`)
		}
	}))
	t.Cleanup(orders.Close)
	mocks = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		note(r)
		switch r.URL.Path {
		case "/admin/facts":
			fmt.Fprint(w, `{"payment":{"requests":{"by_order":{},"by_status":{}},"charges":{"by_order":{},"amount_by_order":{}},"declines":{"by_order":{}}},"inventory":{"reservations":{"by_order":{}}}}`)
		case "/admin/runner/status":
			fmt.Fprint(w, `{"state":"idle"}`)
		case "/admin/run/start":
			w.WriteHeader(204)
		default:
			w.WriteHeader(200)
		}
	}))
	t.Cleanup(mocks.Close)
	return orders, mocks, func() []seenReq {
		mu.Lock()
		defer mu.Unlock()
		return append([]seenReq(nil), reqs...)
	}
}

// Все admin-запросы раннера помечены X-Stand-Client: runner и X-Stand-Run
// (cli — make scenarios), во время прогона идёт признак жизни; запросы в
// Orders не помечаются.
func TestAdminRequestsMarked(t *testing.T) {
	orders, mocks, got := recStand(t)
	p := testParams(t)
	p.WaitS = 0.3
	p.Scenarios.S1.Orders, p.Scenarios.S1.RatePerS = 2, 100
	r := newRunner(p, orders.URL, mocks.URL, "tok", newOutput(io.Discard))
	r.aliveEvery = 50 * time.Millisecond
	if r.dashboardBusy() {
		t.Fatal("idle → не busy")
	}
	r.run("1", []string{"S1"}, "")
	checkMarked(t, got(), "cli")
}

func checkMarked(t *testing.T, reqs []seenReq, run string) {
	t.Helper()
	admin, alive := 0, 0
	paths := map[string]bool{}
	for _, q := range reqs {
		if strings.HasPrefix(q.path, "/admin/") {
			admin++
			paths[q.method+" "+q.path] = true
			if q.client != "runner" || q.run != run || len(q.id) != 16 || q.id != reqs[0].id {
				t.Fatalf("%s %s: X-Stand-Client=%q X-Stand-Run=%q X-Stand-Run-Id=%q", q.method, q.path, q.client, q.run, q.id)
			}
			if q.method != "GET" && q.ctype != "application/json" {
				t.Fatalf("%s %s: Content-Type %q", q.method, q.path, q.ctype)
			}
			if q.method == "GET" && q.path == "/admin/modes" {
				alive++
			}
		} else if q.client != "" || q.run != "" {
			t.Fatalf("%s %s помечен: %q %q", q.method, q.path, q.client, q.run)
		}
	}
	for _, want := range []string{"POST /admin/run/start", "POST /admin/reset", "PUT /admin/modes", "GET /admin/facts"} {
		if !paths[want] {
			t.Fatalf("нет %s среди %v", want, paths)
		}
	}
	if alive == 0 {
		t.Fatal("нет признака жизни GET /admin/modes во время прогона")
	}
}

// Прогон из дашборда (runner -serve) — X-Stand-Run: dashboard.
func TestServeRequestsMarked(t *testing.T) {
	orders, mocks, got := recStand(t)
	dir := t.TempDir()
	params := filepath.Join(dir, "p.json")
	_ = os.WriteFile(params, []byte(`{"scenarios":{"S1":{"orders":2,"rate_per_s":100}}}`), 0o644)
	s := &server{state: "idle", cfg: serveConfig{task: "1", paramsFile: params, orders: orders.URL, mocks: mocks.URL, token: "tok"}}
	h := s.routes()
	if c, b := apiCall(t, h, "POST", "/run", "tok", `{"only":"S1","wait":0}`); c != 202 {
		t.Fatalf("run: %d %s", c, b)
	}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		done := s.state == "done"
		s.mu.Unlock()
		if done {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	reqs := got()
	n := 0
	for _, q := range reqs {
		if strings.HasPrefix(q.path, "/admin/") {
			n++
			if q.client != "runner" || q.run != "dashboard" {
				t.Fatalf("%s %s: %q %q", q.method, q.path, q.client, q.run)
			}
		}
	}
	if n == 0 {
		t.Fatal("нет admin-запросов")
	}
}
