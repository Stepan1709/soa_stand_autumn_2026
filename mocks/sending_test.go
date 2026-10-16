package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// slowOrders — Orders, который отвечает на POST через delay и считает
// запросы; GET /orders/{id} — через getDelay.
func slowOrders(t *testing.T, delay, getDelay time.Duration) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var n atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			select {
			case <-time.After(getDelay):
			case <-r.Context().Done():
				return
			}
			fmt.Fprint(w, `{"status":"paid"}`)
			return
		}
		k := n.Add(1)
		time.Sleep(delay)
		w.WriteHeader(201)
		fmt.Fprintf(w, `{"id":"o-%d","status":"pending"}`, k)
	}))
	t.Cleanup(srv.Close)
	return srv, &n
}

func sendingStand(t *testing.T, ordersURL string) (*httptest.Server, *state) {
	t.Helper()
	st := newState(Modes{Seed: 1})
	st.adminToken = "tok"
	st.ordersURL = ordersURL
	srv := httptest.NewServer(st.routes())
	t.Cleanup(srv.Close)
	return srv, st
}

func waitSending(t *testing.T, url string, f func(*sendingView) bool) eventsResp {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		_, ev := getEvents(t, url, "tok", 0)
		if ev.Sending != nil && f(ev.Sending) {
			return ev
		}
		if time.Now().After(deadline) {
			t.Fatalf("sending %+v", ev.Sending)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// «Очистить журнал» прерывает отправку: новых заказов нет, ответы на
// заказы, ушедшие до сброса, в новый журнал не попадают.
func TestResetStopsSending(t *testing.T) {
	orders, n := slowOrders(t, 150*time.Millisecond, 0)
	srv, _ := sendingStand(t, orders.URL)
	if c, b := standDo(t, http.MethodPost, srv.URL+"/admin/orders", "ui", "", `{"count":200,"rate_per_s":50}`); c != 202 {
		t.Fatalf("send: %d %s", c, b)
	}
	waitSending(t, srv.URL, func(s *sendingView) bool { return s.Sent >= 5 })
	if c, _ := standDo(t, http.MethodPost, srv.URL+"/admin/reset", "ui", "", ""); c != 204 {
		t.Fatalf("reset: %d", c)
	}
	ev := waitSending(t, srv.URL, func(s *sendingView) bool { return !s.Active })
	if ev.Sending.Stopped != "reset" || ev.Sending.Sent >= 200 {
		t.Fatalf("sending %+v", ev.Sending)
	}
	got := n.Load()
	time.Sleep(400 * time.Millisecond) // дольше ответа Orders и шага отправки
	if n.Load() != got {
		t.Fatalf("заказы идут после сброса: %d → %d", got, n.Load())
	}
	_, ev = getEvents(t, srv.URL, "tok", 0)
	if len(ev.Events) != 0 {
		t.Fatalf("в новом журнале события старой отправки: %+v", ev.Events)
	}
	c, b := standDo(t, http.MethodPost, srv.URL+"/admin/orders/check", "ui", "", "")
	if c != 200 || !strings.Contains(string(b), `"statuses":{}`) {
		t.Fatalf("check после сброса: %d %s", c, b)
	}
	// Новая отправка после сброса работает.
	if c, _ := standDo(t, http.MethodPost, srv.URL+"/admin/orders", "ui", "", `{"count":1,"rate_per_s":1}`); c != 202 {
		t.Fatalf("новая отправка: %d", c)
	}
	ev = waitSending(t, srv.URL, func(s *sendingView) bool { return !s.Active })
	if ev.Sending.Stopped != "" || ev.Sending.Done != 1 {
		t.Fatalf("новая отправка %+v", ev.Sending)
	}
}

// Начало прогона раннера (метка начала и прогон из дашборда) прерывает
// ручную отправку.
func TestRunStartStopsSending(t *testing.T) {
	orders, n := slowOrders(t, 0, 0)
	srv, _ := sendingStand(t, orders.URL)
	standDo(t, http.MethodPost, srv.URL+"/admin/orders", "ui", "", `{"count":200,"rate_per_s":50}`)
	waitSending(t, srv.URL, func(s *sendingView) bool { return s.Sent >= 3 })
	if c, _ := standDo(t, http.MethodPost, srv.URL+"/admin/run/start", "runner", "cli:R", ""); c != 204 {
		t.Fatalf("start: %d", c)
	}
	ev := waitSending(t, srv.URL, func(s *sendingView) bool { return !s.Active })
	if ev.Sending.Stopped != "runner" {
		t.Fatalf("sending %+v", ev.Sending)
	}
	got := n.Load()
	time.Sleep(200 * time.Millisecond)
	if n.Load() != got {
		t.Fatalf("заказы идут во время прогона: %d → %d", got, n.Load())
	}
	// Своё продолжение прогона отправку уже не трогает (её нет), а страница
	// новую не начнёт.
	if c, _ := standDo(t, http.MethodPost, srv.URL+"/admin/orders", "ui", "", `{"count":1,"rate_per_s":1}`); c != 409 {
		t.Fatalf("отправка во время прогона: %d", c)
	}
}

// Ответ Orders с редиректом не уводит заглушки на другой адрес.
func TestOrdersNoRedirect(t *testing.T) {
	var hit atomic.Int64
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hit.Add(1) }))
	defer other.Close()
	orders := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL+"/admin/reset", http.StatusTemporaryRedirect)
	}))
	defer orders.Close()
	srv, st := sendingStand(t, orders.URL)
	standDo(t, http.MethodPost, srv.URL+"/admin/orders", "ui", "", `{"count":1,"rate_per_s":1}`)
	ev := waitSending(t, srv.URL, func(s *sendingView) bool { return !s.Active })
	if hit.Load() != 0 || len(ev.Events) != 1 || ev.Events[0].Outcome != "307" {
		t.Fatalf("hit=%d events %+v", hit.Load(), ev.Events)
	}
	st.mu.Lock()
	st.known = []string{"o-1"}
	st.mu.Unlock()
	_, b := standDo(t, http.MethodPost, srv.URL+"/admin/orders/check", "ui", "", "")
	if hit.Load() != 0 || !strings.Contains(string(b), `"GET 307"`) {
		t.Fatalf("check: hit=%d %s", hit.Load(), b)
	}
}

// Проверка статусов: одна за раз (вторая — 409) и не дольше checkWait.
func TestCheckSingleFlightAndDeadline(t *testing.T) {
	old := checkWait
	checkWait = 300 * time.Millisecond
	defer func() { checkWait = old }()
	orders, _ := slowOrders(t, 0, 2*time.Second)
	srv, st := sendingStand(t, orders.URL)
	st.mu.Lock()
	for i := 0; i < 20; i++ {
		st.known = append(st.known, fmt.Sprintf("o-%d", i))
	}
	st.mu.Unlock()
	type res struct {
		code int
		body []byte
		dur  time.Duration
	}
	first := make(chan res, 1)
	go func() {
		start := time.Now()
		c, b := standDo(t, http.MethodPost, srv.URL+"/admin/orders/check", "ui", "", "")
		first <- res{c, b, time.Since(start)}
	}()
	time.Sleep(50 * time.Millisecond)
	if c, b := standDo(t, http.MethodPost, srv.URL+"/admin/orders/check", "ui", "", ""); c != 409 || !strings.Contains(string(b), "уже идёт") {
		t.Fatalf("вторая проверка: %d %s", c, b)
	}
	r := <-first
	var out struct {
		Statuses map[string]string `json:"statuses"`
	}
	if r.code != 200 || json.Unmarshal(r.body, &out) != nil || r.dur > 1500*time.Millisecond {
		t.Fatalf("первая: %d %s за %v", r.code, r.body, r.dur)
	}
	if c, _ := standDo(t, http.MethodPost, srv.URL+"/admin/orders/check", "ui", "", ""); c == 409 {
		t.Fatal("после конца проверки — снова 409")
	}
}
