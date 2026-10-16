package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func publish(t *testing.T, url, id, orderID string) int {
	t.Helper()
	b, _ := json.Marshal(map[string]string{"id": id, "type": "order.paid", "order_id": orderID})
	c := &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	resp, err := c.Post(url+"/events", "application/json", bytes.NewReader(b))
	if err != nil {
		return 0
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode
}

func TestPublishCopiesAndBadEvents(t *testing.T) {
	srv, _ := setup(t, Modes{Seed: 1})
	for _, id := range []string{"e1", "e1", "e2"} {
		if c := publish(t, srv.URL, id, "o1"); c != 202 {
			t.Fatalf("publish %s: %d", id, c)
		}
	}
	if c := publish(t, srv.URL, "", "o1"); c != 400 {
		t.Fatalf("без id: %d", c)
	}
	resp, _ := http.Post(srv.URL+"/events", "application/json", bytes.NewReader([]byte(`{"id":"x","type":"other","order_id":"o2"}`)))
	resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("чужой type: %d", resp.StatusCode)
	}
	f := facts(t, srv.URL).Bus
	if f.Accepted != 3 || len(f.EventsByOrder["o1"]) != 2 || f.EventsByOrder["o1"][0] != "e1" {
		t.Fatalf("accepted=%d by_order=%v", f.Accepted, f.EventsByOrder)
	}
	if f.Requests.ByStatus["400"] != 2 || !f.Log[1].Dup || f.Log[2].Dup {
		t.Fatalf("by_status=%v log=%+v", f.Requests.ByStatus, f.Log)
	}
}

func TestBusDownWindow(t *testing.T) {
	srv, _ := setup(t, Modes{Seed: 3, Bus: BusModes{DownS: 0.6, HangRate: 0.5}})
	codes := map[int]int{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := publish(t, srv.URL, "e", "o1")
			mu.Lock()
			codes[c]++
			mu.Unlock()
		}()
	}
	wg.Wait()
	// Висящие закрываются без ответа (код 0) по концу окна, остальные — 503.
	if codes[503] == 0 || codes[0] == 0 || codes[202] != 0 {
		t.Fatalf("коды во время отказа: %v", codes)
	}
	f := facts(t, srv.URL).Bus
	if f.Requests.InDown != 20 || f.Accepted != 0 || len(f.EventsByOrder) != 0 || f.Down == nil {
		t.Fatalf("факты: %+v", f.Requests)
	}
	for _, l := range f.Log {
		if l.Outcome == "hang" && l.DoneAt == nil {
			t.Fatalf("висящий запрос без done_at: %+v", l)
		}
	}
	time.Sleep(100 * time.Millisecond)
	if c := publish(t, srv.URL, "e", "o1"); c != 202 {
		t.Fatalf("после окна: %d", c)
	}
}

// fakeWebhook — Orders с /webhooks/delivery: первые fail ответов — 500.
func fakeWebhook(t *testing.T, fail int64) (*httptest.Server, *atomic.Int64, *sync.Map) {
	var calls atomic.Int64
	var seen sync.Map // id → число подтверждённых копий
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/webhooks/delivery" {
			http.NotFound(w, r)
			return
		}
		var ev struct {
			ID     string `json:"id"`
			Type   string `json:"type"`
			UserID string `json:"user_id"`
			Points int    `json:"bonus_points"`
		}
		_ = json.NewDecoder(r.Body).Decode(&ev)
		if ev.Type != "delivery.confirmed" || ev.UserID == "" || ev.Points <= 0 {
			w.WriteHeader(400)
			return
		}
		if calls.Add(1) <= fail {
			w.WriteHeader(500)
			return
		}
		v, _ := seen.LoadOrStore(ev.ID, new(atomic.Int64))
		v.(*atomic.Int64).Add(1)
		w.WriteHeader(204)
	}))
	t.Cleanup(srv.Close)
	return srv, &calls, &seen
}

func busStand(t *testing.T, ordersURL string) (*httptest.Server, *state) {
	t.Helper()
	st := newState(Modes{Seed: 5})
	st.ordersURL = ordersURL
	srv := httptest.NewServer(st.routes())
	t.Cleanup(srv.Close)
	// Доставка не переживает тест: иначе её попытки идут в закрытый сервер.
	t.Cleanup(func() {
		st.mu.Lock()
		st.resetLocked()
		st.mu.Unlock()
	})
	return srv, st
}

func waitDelivery(t *testing.T, url string, f func(*DeliveryFacts) bool) DeliveryFacts {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if d := facts(t, url).Bus.Delivery; d != nil && f(d) {
			return *d
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("доставка не дошла до ожидаемого состояния")
	return DeliveryFacts{}
}

func TestDeliverCopiesAndRetry(t *testing.T) {
	defer func(d time.Duration) { dlvRetry = d }(dlvRetry)
	dlvRetry = 50 * time.Millisecond
	orders, calls, seen := fakeWebhook(t, 3)
	srv, _ := busStand(t, orders.URL)
	if c := call(t, http.MethodPost, srv.URL+"/admin/bus/deliver",
		`{"events":12,"users":4,"max_copies":3,"parallel_rate":0.5,"rate_per_s":200}`); c != 202 {
		t.Fatalf("deliver: %d", c)
	}
	if c := call(t, http.MethodPost, srv.URL+"/admin/bus/deliver",
		`{"events":1,"users":1,"max_copies":1,"parallel_rate":0,"rate_per_s":1}`); c != 409 {
		t.Fatalf("вторая партия до подтверждения первой: %d", c)
	}
	d := waitDelivery(t, srv.URL, func(d *DeliveryFacts) bool { return d.Pending == 0 })
	copies, sum, multi := 0, 0, 0
	for _, e := range d.Events {
		copies += e.Copies
		sum += e.Points
		if e.Copies > 1 {
			multi++
		}
		if e.Acked != e.Copies {
			t.Fatalf("событие %s: подтверждено %d из %d", e.ID, e.Acked, e.Copies)
		}
	}
	if multi == 0 || d.AckedEvents != 12 || len(d.ExpectedByUser) != 4 {
		t.Fatalf("копий больше одной у %d событий, подтверждено %d, покупателей %d", multi, d.AckedEvents, len(d.ExpectedByUser))
	}
	total := 0
	for _, v := range d.ExpectedByUser {
		total += v
	}
	if total != sum {
		t.Fatalf("expected_by_user %d, сумма %d", total, sum)
	}
	if got := calls.Load(); got != int64(copies)+3 || d.Attempts != copies+3 {
		t.Fatalf("попыток %d (в фактах %d), копий %d + 3 ошибки", got, d.Attempts, copies)
	}

	// Ещё по одной копии каждого события.
	if c := call(t, http.MethodPost, srv.URL+"/admin/bus/redeliver", ``); c != 202 {
		t.Fatalf("redeliver: %d", c)
	}
	d = waitDelivery(t, srv.URL, func(d *DeliveryFacts) bool { return d.Pending == 0 })
	n := 0
	seen.Range(func(_, v any) bool { n += int(v.(*atomic.Int64).Load()); return true })
	if n != copies+12 {
		t.Fatalf("подтверждено копий %d, ожидалось %d", n, copies+12)
	}
}

func TestDeliverDeterministicAndReset(t *testing.T) {
	defer func(d time.Duration) { dlvRetry = d }(dlvRetry)
	dlvRetry = 50 * time.Millisecond
	ids := func() []string {
		orders, _, _ := fakeWebhook(t, 0)
		srv, _ := busStand(t, orders.URL)
		call(t, http.MethodPost, srv.URL+"/admin/bus/deliver",
			`{"events":5,"users":2,"max_copies":3,"parallel_rate":0,"rate_per_s":100}`)
		d := waitDelivery(t, srv.URL, func(d *DeliveryFacts) bool { return d.Pending == 0 })
		var out []string
		for _, e := range d.Events {
			out = append(out, e.ID+e.UserID)
		}
		return out
	}
	a, b := ids(), ids()
	if len(a) != 5 || a[0] != b[0] || a[4] != b[4] {
		t.Fatalf("при одном seed разные события: %v %v", a, b)
	}

	// Orders не отвечает 2xx: после reset доставка прекращается.
	orders, calls, _ := fakeWebhook(t, 1<<40)
	srv, _ := busStand(t, orders.URL)
	call(t, http.MethodPost, srv.URL+"/admin/bus/deliver",
		`{"events":2,"users":1,"max_copies":1,"parallel_rate":0,"rate_per_s":100}`)
	time.Sleep(200 * time.Millisecond)
	if c := call(t, http.MethodPost, srv.URL+"/admin/reset", ``); c != 204 {
		t.Fatalf("reset: %d", c)
	}
	time.Sleep(100 * time.Millisecond)
	before := calls.Load()
	time.Sleep(300 * time.Millisecond)
	if after := calls.Load(); after != before || before == 0 {
		t.Fatalf("после reset попытки продолжаются: %d → %d", before, after)
	}
	if facts(t, srv.URL).Bus.Delivery != nil {
		t.Fatal("после reset в фактах осталась доставка")
	}
}

func TestBusModesValidate(t *testing.T) {
	srv, _ := setup(t, Modes{Seed: 1})
	for _, body := range []string{`{"bus":{"hang_rate":2}}`, `{"bus":{"down_s":-1}}`, `{"bus":{"x":1}}`} {
		if c := call(t, http.MethodPut, srv.URL+"/admin/modes", body); c != 400 {
			t.Fatalf("%s: %d", body, c)
		}
	}
	if c := call(t, http.MethodPut, srv.URL+"/admin/modes", `{"bus":{"down_s":5,"hang_rate":0.5}}`); c != 200 {
		t.Fatalf("корректный режим: %d", c)
	}
}
