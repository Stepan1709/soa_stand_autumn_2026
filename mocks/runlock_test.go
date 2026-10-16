package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeClock — подменяемые часы для state.now.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *fakeClock) add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func standDo(t *testing.T, method, url, client, run, body string) (int, []byte) {
	t.Helper()
	req, _ := http.NewRequest(method, url, strings.NewReader(body))
	req.Header.Set("X-Admin-Token", "tok")
	if method != http.MethodGet {
		req.Header.Set("Content-Type", "application/json")
	}
	if client != "" {
		req.Header.Set("X-Stand-Client", client)
	}
	// run — «источник» или «источник:id прогона».
	if src, id, ok := strings.Cut(run, ":"); ok {
		req.Header.Set("X-Stand-Run", src)
		req.Header.Set("X-Stand-Run-Id", id)
	} else if run != "" {
		req.Header.Set("X-Stand-Run", run)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

func lockStand(t *testing.T) (*httptest.Server, *fakeClock) {
	t.Helper()
	orders := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(201)
		fmt.Fprint(w, `{"id":"o-1","status":"pending"}`)
	}))
	t.Cleanup(orders.Close)
	runnerUI := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"state":"running"}`)
	}))
	t.Cleanup(runnerUI.Close)
	clk := &fakeClock{t: time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)}
	st := newState(Modes{Seed: 1})
	st.adminToken = "tok"
	st.ordersURL = orders.URL
	st.now = clk.now
	p, err := newRunnerProxy(runnerUI.URL)
	if err != nil {
		t.Fatal(err)
	}
	st.runnerUI = p
	srv := httptest.NewServer(st.routes())
	t.Cleanup(srv.Close)
	return srv, clk
}

type manualReq struct{ method, path, body string }

// Ручные действия страницы; POST /admin/orders — последним: отправка идёт в
// фоне, повторная дала бы 409 «предыдущая отправка».
var manualReqs = []manualReq{
	{http.MethodPut, "/admin/modes", `{"label":"ручной режим","payment":{}}`},
	{http.MethodPost, "/admin/reset", ""},
	{http.MethodPost, "/admin/orders/check", ""},
	{http.MethodPost, "/admin/runner/run", `{"only":"S1"}`},
}

func runnerState(t *testing.T, url string) eventsResp {
	t.Helper()
	c, b := standDo(t, http.MethodGet, url+"/admin/events?since=0", "ui", "", "")
	var ev eventsResp
	if c != 200 || json.Unmarshal(b, &ev) != nil {
		t.Fatalf("events %d %s", c, b)
	}
	return ev
}

func expectManual(t *testing.T, url string, want int) {
	t.Helper()
	for _, m := range manualReqs {
		c, b := standDo(t, m.method, url+m.path, "ui", "", m.body)
		if want == http.StatusConflict {
			if c != 409 || !strings.Contains(string(b), "Идёт прогон сценариев") || !strings.Contains(string(b), "runner_active") {
				t.Fatalf("%s %s во время прогона: %d %s", m.method, m.path, c, b)
			}
		} else if c == 409 {
			t.Fatalf("%s %s вне прогона: %d %s", m.method, m.path, c, b)
		}
	}
}

func TestRunnerLockUI(t *testing.T) {
	srv, clk := lockStand(t)
	u := srv.URL

	// Раннер только проверил стенд (GET) — прогон не начат.
	standDo(t, http.MethodGet, u+"/admin/runner/status", "runner", "cli", "")
	if ev := runnerState(t, u); ev.RunnerActive {
		t.Fatal("GET раннера не начинает прогон")
	}
	expectManual(t, u, 200)

	// Начало сценария: reset и режимы от раннера.
	if c, _ := standDo(t, http.MethodPost, u+"/admin/reset", "runner", "cli", ""); c != 204 {
		t.Fatalf("reset раннера: %d", c)
	}
	if c, _ := standDo(t, http.MethodPut, u+"/admin/modes", "runner", "cli", `{"label":"S1 · x","payment":{"decline_rate":0.1}}`); c != 200 {
		t.Fatalf("modes раннера: %d", c)
	}
	ev := runnerState(t, u)
	if !ev.RunnerActive || ev.RunnerSource != "cli" {
		t.Fatalf("runner_active=%v source=%q", ev.RunnerActive, ev.RunnerSource)
	}
	expectManual(t, u, http.StatusConflict)
	if c, b := standDo(t, http.MethodPost, u+"/admin/orders", "ui", "", `{"count":1,"rate_per_s":1}`); c != 409 {
		t.Fatalf("orders со страницы: %d %s", c, b)
	}
	// Режимы и журнал остались от раннера.
	if ev := runnerState(t, u); ev.Modes.Label != "S1 · x" {
		t.Fatalf("режимы тронуты: %+v", ev.Modes)
	}
	// Ручной curl без X-Stand-Client — осознанное действие, проходит.
	if c, _ := standDo(t, http.MethodPost, u+"/admin/orders/check", "", "", ""); c != 200 {
		t.Fatalf("curl без заголовка: %d", c)
	}
	// Раннер сам не блокируется.
	if c, _ := standDo(t, http.MethodPost, u+"/admin/reset", "runner", "cli", ""); c != 204 {
		t.Fatalf("reset раннера во время прогона: %d", c)
	}
	if c, _ := standDo(t, http.MethodPut, u+"/admin/modes", "runner", "cli", `{"label":"S1 · x","payment":{}}`); c != 200 {
		t.Fatalf("modes раннера во время прогона: %d", c)
	}

	// Долгая пауза с признаком жизни: 10 × 20 с — прогон всё ещё идёт.
	for i := 0; i < 10; i++ {
		clk.add(20 * time.Second)
		standDo(t, http.MethodGet, u+"/admin/modes", "runner", "cli", "")
	}
	if !runnerState(t, u).RunnerActive {
		t.Fatal("признак жизни не продлил прогон")
	}

	// Метка «прогон завершён» — разблокировка сразу.
	if c, _ := standDo(t, http.MethodPut, u+"/admin/modes", "runner", "cli",
		`{"label":"S1 · x · прогон завершён","finished":true,"payment":{}}`); c != 200 {
		t.Fatalf("finished: %d", c)
	}
	// Признак жизни после метки конца прогон не возобновляет.
	standDo(t, http.MethodGet, u+"/admin/modes", "runner", "cli", "")
	if ev := runnerState(t, u); ev.RunnerActive {
		t.Fatal("после finished прогон всё ещё идёт")
	}
	expectManual(t, u, 200)
}

// Раннер убит посреди прогона: метки конца нет — через runnerIdle без его
// запросов кнопки снова доступны.
func TestRunnerLockExpires(t *testing.T) {
	srv, clk := lockStand(t)
	u := srv.URL
	standDo(t, http.MethodPost, u+"/admin/reset", "runner", "dashboard", "")
	ev := runnerState(t, u)
	if !ev.RunnerActive || ev.RunnerSource != "dashboard" {
		t.Fatalf("runner_active=%v source=%q", ev.RunnerActive, ev.RunnerSource)
	}
	expectManual(t, u, http.StatusConflict)
	// Запросы страницы (лента, 409) прогон не продлевают.
	clk.add(runnerIdle)
	if !runnerState(t, u).RunnerActive {
		t.Fatal("ровно runnerIdle — ещё идёт")
	}
	clk.add(time.Second)
	if runnerState(t, u).RunnerActive {
		t.Fatal("после runnerIdle без запросов раннера прогон не кончился")
	}
	expectManual(t, u, 200)
	// Новый прогон — снова блокировка.
	standDo(t, http.MethodPost, u+"/admin/reset", "runner", "cli", "")
	if ev := runnerState(t, u); !ev.RunnerActive || ev.RunnerSource != "cli" {
		t.Fatalf("новый прогон: %v %q", ev.RunnerActive, ev.RunnerSource)
	}
}

// Текст 409 и признак в ленте — без слов-подсказок.
func TestRunnerLockNoHints(t *testing.T) {
	low := strings.ToLower(runnerBusyMsg)
	for _, w := range []string{"ключ", "key", "lookup", "seed", "таймаут", "повтор", "идемпот", "размыкат"} {
		if strings.Contains(low, w) {
			t.Fatalf("%q в %q", w, runnerBusyMsg)
		}
	}
}

// Владелец прогона: второй раннер не начинает прогон и не трогает заглушки,
// его метка конца не снимает чужую блокировку; источник — у владельца.
func TestRunnerOwner(t *testing.T) {
	srv, clk := lockStand(t)
	u := srv.URL
	if c, _ := standDo(t, http.MethodPost, u+"/admin/run/start", "", "", ""); c != 400 {
		t.Fatalf("start не от раннера: %d", c)
	}
	if c, _ := standDo(t, http.MethodPost, u+"/admin/run/start", "runner", "dashboard:A", ""); c != 204 {
		t.Fatalf("start A: %d", c)
	}
	// Метка начала сразу блокирует страницу (до первого сброса).
	expectManual(t, u, http.StatusConflict)
	c, b := standDo(t, http.MethodGet, u+"/admin/run", "", "", "")
	if c != 200 || string(b) != "{\"runner_active\":true,\"runner_source\":\"dashboard\"}\n" {
		t.Fatalf("GET /admin/run: %d %s", c, b)
	}
	for _, q := range []manualReq{
		{http.MethodPost, "/admin/run/start", ""},
		{http.MethodPost, "/admin/reset", ""},
		{http.MethodPut, "/admin/modes", `{"label":"S2 · чужой","payment":{"fail_rate":1}}`},
		{http.MethodPut, "/admin/modes", `{"label":"чужой · прогон завершён","finished":true,"payment":{}}`},
	} {
		c, b := standDo(t, q.method, u+q.path, "runner", "cli:B", q.body)
		if c != 409 || !strings.Contains(string(b), "другой прогон") {
			t.Fatalf("B %s %s: %d %s", q.method, q.path, c, b)
		}
	}
	// Запросы B прогон не продлевают, источник остаётся у A.
	clk.add(runnerIdle - time.Second)
	standDo(t, http.MethodGet, u+"/admin/modes", "runner", "cli:B", "")
	ev := runnerState(t, u)
	if !ev.RunnerActive || ev.RunnerSource != "dashboard" || ev.Modes.Label == "S2 · чужой" {
		t.Fatalf("после B: %v %q %q", ev.RunnerActive, ev.RunnerSource, ev.Modes.Label)
	}
	clk.add(2 * time.Second)
	if runnerState(t, u).RunnerActive {
		t.Fatal("запросы B продлили прогон A")
	}
	// A брошен — B становится владельцем; конец B снимает блокировку сразу.
	if c, _ := standDo(t, http.MethodPost, u+"/admin/run/start", "runner", "cli:B", ""); c != 204 {
		t.Fatalf("start B: %d", c)
	}
	if ev := runnerState(t, u); !ev.RunnerActive || ev.RunnerSource != "cli" {
		t.Fatalf("B: %v %q", ev.RunnerActive, ev.RunnerSource)
	}
	if c, _ := standDo(t, http.MethodPut, u+"/admin/modes", "runner", "dashboard:A", `{"finished":true,"payment":{}}`); c != 409 {
		t.Fatalf("конец A во время B: %d", c)
	}
	if c, _ := standDo(t, http.MethodPut, u+"/admin/modes", "runner", "cli:B", `{"label":"S1 · прогон прерван","finished":true,"payment":{}}`); c != 200 {
		t.Fatalf("конец B: %d", c)
	}
	if runnerState(t, u).RunnerActive {
		t.Fatal("после конца B прогон идёт")
	}
	expectManual(t, u, 200)
}

// Защита от чужих страниц: Host — только имена стенда (порт любой),
// изменяющие запросы — только JSON.
func TestAdminHostAndContentType(t *testing.T) {
	st := newState(Modes{Seed: 1})
	h := st.routes()
	do := func(method, host, ct string) int {
		r := httptest.NewRequest(method, "/admin/modes", strings.NewReader(`{"payment":{}}`))
		r.Host = host
		if ct != "" {
			r.Header.Set("Content-Type", ct)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, r)
		return rec.Code
	}
	for _, host := range []string{"localhost:8090", "127.0.0.1:18090", "[::1]:28090", "mocks:8080", "mocks-admin:8080", "LOCALHOST", "localhost"} {
		if c := do(http.MethodGet, host, ""); c != 200 {
			t.Fatalf("GET Host %s: %d", host, c)
		}
		if c := do(http.MethodPut, host, "application/json; charset=utf-8"); c != 200 {
			t.Fatalf("PUT Host %s: %d", host, c)
		}
	}
	for _, host := range []string{"evil.example:8090", "127.0.0.1.nip.io:8090", "orders:8080", ""} {
		if c := do(http.MethodGet, host, ""); c != 403 {
			t.Fatalf("Host %q: %d", host, c)
		}
	}
	for _, ct := range []string{"", "text/plain", "application/x-www-form-urlencoded", "multipart/form-data; boundary=x"} {
		if c := do(http.MethodPut, "localhost:8090", ct); c != 415 {
			t.Fatalf("Content-Type %q: %d", ct, c)
		}
	}
	// Страница не требует токена и Host не проверяет: она сама только
	// читает /admin/*.
	r := httptest.NewRequest(http.MethodGet, "/ui", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	if rec.Code != 200 {
		t.Fatalf("/ui: %d", rec.Code)
	}
}
