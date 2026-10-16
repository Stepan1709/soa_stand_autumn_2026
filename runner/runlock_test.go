package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// lockMocks — заглушки, которые отвечают на метку начала кодом startCode и
// запоминают изменяющие admin-запросы с телами.
func lockMocks(t *testing.T, startCode int) (*httptest.Server, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var got []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			b, _ := io.ReadAll(r.Body)
			mu.Lock()
			got = append(got, r.Method+" "+r.URL.Path+" "+string(b))
			mu.Unlock()
		}
		switch r.URL.Path {
		case "/healthz":
		case "/admin/run/start":
			w.WriteHeader(startCode)
		case "/admin/run":
			fmt.Fprint(w, `{"runner_active":true,"runner_source":"cli"}`)
		default:
			w.WriteHeader(404)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), got...)
	}
}

// Идёт чужой прогон: раннер не трогает заглушки и метку конца не ставит.
func TestRunOtherRunActive(t *testing.T) {
	mocks, got := lockMocks(t, http.StatusConflict)
	out := newOutput(io.Discard)
	r := newRunner(testParams(t), "http://127.0.0.1:1", mocks.URL, "tok", out)
	if code := r.run("1", []string{"S1"}, ""); code != 2 {
		t.Fatalf("код %d", code)
	}
	if g := got(); len(g) != 1 || !strings.HasPrefix(g[0], "POST /admin/run/start") {
		t.Fatalf("запросы %v", g)
	}
	if !strings.Contains(out.text(), "другой прогон") {
		t.Fatalf("вывод %q", out.text())
	}
	if !r.dashboardBusy() {
		t.Fatal("runner_active у заглушек → busy")
	}
}

// Метка конца: подпись текущего сценария плюс причина, finished, один раз.
func TestEndRunOnce(t *testing.T) {
	mocks, got := lockMocks(t, http.StatusNoContent)
	r := newRunner(testParams(t), "http://127.0.0.1:1", mocks.URL, "tok", newOutput(io.Discard))
	r.endRun("прогон прерван") // начала не было — ничего
	if err := r.startRun(); err != nil || !r.started.Load() {
		t.Fatalf("start: %v", err)
	}
	r.setLabel("S4 · Payment не отвечает")
	r.endRun("прогон прерван")
	r.endRun("прогон прерван")
	g := got()
	if len(g) != 2 || !strings.HasPrefix(g[1], "PUT /admin/modes") {
		t.Fatalf("запросы %v", g)
	}
	var m map[string]any
	_ = json.Unmarshal([]byte(strings.SplitN(g[1], " ", 3)[2]), &m)
	if m["finished"] != true || m["label"] != "S4 · Payment не отвечает · прогон прерван" {
		t.Fatalf("метка конца %v", m)
	}
}

// Orders не отвечает после метки начала — standErr ставит метку конца сразу.
func TestStandErrAfterStart(t *testing.T) {
	mocks, got := lockMocks(t, http.StatusNoContent)
	orders := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(500) }))
	defer orders.Close()
	r := newRunner(testParams(t), orders.URL, mocks.URL, "tok", newOutput(io.Discard))
	r.ordersWait = 200 * time.Millisecond
	if code := r.run("1", []string{"S1"}, ""); code != 2 {
		t.Fatalf("код %d", code)
	}
	g := got()
	if len(g) != 2 || !strings.HasPrefix(g[0], "POST /admin/run/start") ||
		!strings.Contains(g[1], "прогон прерван: стенд не готов") || !strings.Contains(g[1], `"finished":true`) {
		t.Fatalf("запросы %v", g)
	}
}

// Ответ Orders с редиректом раннер не выполняет.
func TestRunnerNoRedirect(t *testing.T) {
	var hit atomic.Int64
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { hit.Add(1) }))
	defer other.Close()
	orders := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL+"/admin/reset", http.StatusFound)
	}))
	defer orders.Close()
	r := newRunner(testParams(t), orders.URL, other.URL+"/x", "tok", newOutput(io.Discard))
	code, _, err := r.do(http.MethodGet, orders.URL+"/orders/o-1", nil, 2*time.Second)
	if err != nil || code != http.StatusFound || hit.Load() != 0 {
		t.Fatalf("code %d err %v hit %d", code, err, hit.Load())
	}
}

// allowList: DNS вне лока (медленный резолвер не держит проверку известного
// адреса), неудача кэшируется примерно на секунду.
func TestAllowListDNS(t *testing.T) {
	var calls atomic.Int64
	fail := atomic.Bool{}
	release := make(chan struct{})
	a := &allowList{name: "mocks-admin", lookup: func(ctx context.Context, _ string) ([]string, error) {
		n := calls.Add(1)
		if fail.Load() {
			return nil, errors.New("нет DNS")
		}
		if n == 2 {
			<-release // второй резолв — медленный
		}
		return []string{"10.0.0.5"}, nil
	}}
	if !a.allowed("10.0.0.5:1234") || calls.Load() != 1 {
		t.Fatalf("первый: calls %d", calls.Load())
	}
	// Кэш истёк: один запрос резолвит (медленно), другой в это время
	// отвечает по старому кэшу сразу.
	a.mu.Lock()
	a.until = time.Now().Add(-time.Second)
	a.mu.Unlock()
	done := make(chan bool)
	go func() { done <- a.allowed("10.0.0.5:1") }()
	for calls.Load() < 2 {
		time.Sleep(time.Millisecond)
	}
	start := time.Now()
	if !a.allowed("10.0.0.5:2") || time.Since(start) > 100*time.Millisecond {
		t.Fatal("проверка ждала резолвер")
	}
	close(release)
	if !<-done {
		t.Fatal("медленный резолв")
	}
	// Неудача: чужой адрес — резолв не чаще раза в секунду.
	fail.Store(true)
	a.mu.Lock()
	a.until, a.resolved = time.Now().Add(-time.Second), time.Time{}
	a.mu.Unlock()
	before := calls.Load()
	for i := 0; i < 20; i++ {
		if a.allowed("10.9.9.9:1") {
			t.Fatal("чужой адрес пущен")
		}
	}
	if calls.Load()-before != 1 {
		t.Fatalf("резолвов при неудаче: %d", calls.Load()-before)
	}
	if !a.allowed("127.0.0.1:5") {
		t.Fatal("loopback")
	}
}
