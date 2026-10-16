package main

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// В сервис прогонов из параметров запроса уходит только task=1|2.
func TestRunnerProxyTaskQuery(t *testing.T) {
	var mu sync.Mutex
	var got []string
	runner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		got = append(got, r.URL.Path+"?"+r.URL.RawQuery)
		mu.Unlock()
		w.WriteHeader(200)
	}))
	t.Cleanup(runner.Close)
	p, err := newRunnerProxy(runner.URL)
	if err != nil {
		t.Fatal(err)
	}
	st := newState(Modes{Seed: 1})
	st.adminToken = "tok"
	st.runnerUI = p
	srv := httptest.NewServer(st.routes())
	t.Cleanup(srv.Close)

	cases := map[string]string{
		"/admin/runner/params?task=2":            "/params?task=2",
		"/admin/runner/report?task=1":            "/report?task=1",
		"/admin/runner/report?task=2&x=1":        "/report?task=2",
		"/admin/runner/report?x=1&task=1&task=2": "/report?task=1",
		"/admin/runner/params?task=3":            "/params?",
		"/admin/runner/params?task=1%26x%3D1":    "/params?",
		"/admin/runner/status?foo=bar":           "/status?",
		"/admin/runner/status":                   "/status?",
	}
	for in, want := range cases {
		mu.Lock()
		got = nil
		mu.Unlock()
		if c, b := standDo(t, http.MethodGet, srv.URL+in, "", "", ""); c != 200 {
			t.Fatalf("%s: %d %s", in, c, b)
		}
		mu.Lock()
		if len(got) != 1 || got[0] != want {
			t.Fatalf("%s: в раннер ушло %v, ожидалось %s", in, got, want)
		}
		mu.Unlock()
	}
}
