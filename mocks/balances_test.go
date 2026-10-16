package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// eventsJSON — GET /admin/events и его summary как сырой JSON (видно null).
func eventsJSON(t *testing.T, url string) (eventsResp, map[string]json.RawMessage) {
	t.Helper()
	c, b := standDo(t, http.MethodGet, url+"/admin/events", "", "", "")
	if c != 200 {
		t.Fatalf("events: %d %s", c, b)
	}
	var out eventsResp
	var top struct {
		Summary map[string]json.RawMessage `json:"summary"`
	}
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &top); err != nil {
		t.Fatal(err)
	}
	return out, top.Summary
}

func setDelivery(st *state, events ...dlvEvent) {
	ctx, cancel := context.WithCancel(context.Background())
	d := &delivery{ctx: ctx, cancel: cancel}
	for i := range events {
		d.events = append(d.events, &events[i])
	}
	st.mu.Lock()
	st.dlv = d
	st.addExpectedLocked(d)
	st.mu.Unlock()
}

func TestEventsBusSummary(t *testing.T) {
	srv, st := busStand(t, "")
	ev, sum := eventsJSON(t, srv.URL)
	if ev.BusDown != nil || string(sum["delivery"]) != "null" || ev.Summary.Bus.Publish != 0 {
		t.Fatalf("без шины: bus_down %v, delivery %s, bus %+v", ev.BusDown, sum["delivery"], ev.Summary.Bus)
	}

	publish(t, srv.URL, "e1", "o1")
	publish(t, srv.URL, "e1", "o1") // копия
	publish(t, srv.URL, "e2", "o2")
	publish(t, srv.URL, "", "o3") // 400
	if c := call(t, http.MethodPut, srv.URL+"/admin/modes", `{"bus":{"down_s":30,"hang_rate":0}}`); c != 200 {
		t.Fatalf("modes: %d", c)
	}
	publish(t, srv.URL, "e3", "o3") // 503
	ev, _ = eventsJSON(t, srv.URL)
	var ids []string
	for _, e := range ev.Events {
		if e.Service == "bus" && e.Path == "/events" {
			ids = append(ids, e.OrderID+":"+e.EventID)
		}
	}
	if s := strings.Join(ids, ","); s != "o1:e1,o1:e1,o2:e2,o3:,o3:e3" {
		t.Fatalf("event_id в ленте: %s", s)
	}
	b := ev.Summary.Bus
	if b.Publish != 5 || b.Accepted != 3 || b.OrdersWithEvent != 2 || b.InDown != 1 ||
		b.ByOutcome["202"] != 3 || b.ByOutcome["400"] != 1 || b.ByOutcome["503"] != 1 {
		t.Fatalf("summary.bus %+v", b)
	}
	if ev.BusDown == nil || ev.BusDown.Until.Sub(ev.BusDown.From) != 30*time.Second {
		t.Fatalf("bus_down %+v", ev.BusDown)
	}

	setDelivery(st,
		dlvEvent{ID: "a", UserID: "u-1", Points: 10, Copies: 2, Acked: 2, Attempts: 3},
		dlvEvent{ID: "b", UserID: "u-2", Points: 20, Copies: 3, Acked: 1, Attempts: 1},
		dlvEvent{ID: "c", UserID: "u-1", Points: 30, Copies: 1, Acked: 0, Attempts: 2})
	ev, _ = eventsJSON(t, srv.URL)
	want := deliverySummary{Events: 3, Deliveries: 6, AckedEvents: 2, Pending: 3, Attempts: 6, Users: 2}
	if ev.Summary.Delivery == nil || *ev.Summary.Delivery != want {
		t.Fatalf("summary.delivery %+v, ожидалось %+v", ev.Summary.Delivery, want)
	}
}

// bonusOrders — Orders с /users/{id}/bonus: ответ по покупателю из answers
// (код и тело); hang — не отвечает, пока клиент не уйдёт.
func bonusOrders(t *testing.T, answers map[string][2]string) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, ok := strings.CutSuffix(strings.TrimPrefix(r.URL.Path, "/users/"), "/bonus")
		a, known := answers[u]
		if !ok || !known {
			http.NotFound(w, r)
			return
		}
		switch a[0] {
		case "hang":
			<-r.Context().Done()
			return
		case "302":
			w.Header().Set("Location", "http://127.0.0.1:1/users/u-1/bonus")
		}
		var code int
		fmt.Sscan(a[0], &code)
		w.WriteHeader(code)
		fmt.Fprint(w, a[1])
	}))
	t.Cleanup(srv.Close)
	return srv
}

func balances(t *testing.T, url string) []balanceView {
	t.Helper()
	c, b := standDo(t, http.MethodPost, url+"/admin/bus/balances", "", "", "")
	if c != 200 {
		t.Fatalf("balances: %d %s", c, b)
	}
	var out struct {
		Users []balanceView `json:"users"`
	}
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	return out.Users
}

func TestBalances(t *testing.T) {
	orders := bonusOrders(t, map[string][2]string{
		"u-0":  {"200", `{"points":0}`},
		"u-1":  {"200", `{"points":40}`},
		"u-2":  {"500", `{"error":"x"}`},
		"u-3":  {"200", `{"points":"1e400` + "\x07" + `"}`},
		"u-4":  {"302", ``},
		"u-5":  {"200", `не json`},
		"u-10": {"200", `{"balance":5}`},
		"u-11": {"200", `{"points":1e400}`},
	})
	srv, st := busStand(t, orders.URL)

	// Доставки не было — только u-0.
	got := balances(t, srv.URL)
	if len(got) != 1 || got[0].UserID != "u-0" || got[0].Expected != 0 || got[0].Points == nil || *got[0].Points != 0 || got[0].Error != "" {
		t.Fatalf("без доставки: %+v", got)
	}

	var evs []dlvEvent
	for i, u := range []string{"u-10", "u-1", "u-2", "u-11", "u-3", "u-4", "u-5", "u-1"} {
		evs = append(evs, dlvEvent{ID: fmt.Sprint(i), UserID: u, Points: 20})
	}
	setDelivery(st, evs...)
	got = balances(t, srv.URL)
	var order []string
	res := map[string]balanceView{}
	for _, b := range got {
		order = append(order, b.UserID)
		res[b.UserID] = b
	}
	if s := strings.Join(order, ","); s != "u-1,u-2,u-3,u-4,u-5,u-10,u-11,u-0" {
		t.Fatalf("порядок %s", s)
	}
	if b := res["u-1"]; b.Expected != 40 || b.Points == nil || *b.Points != 40 || b.Error != "" {
		t.Fatalf("u-1 %+v", b)
	}
	for u, want := range map[string]string{
		"u-2":  "GET 500",
		"u-3":  "ответ не JSON", // points — строка
		"u-4":  "GET 302",       // без перехода по Location
		"u-5":  "ответ не JSON",
		"u-10": "в ответе нет points",
		"u-11": "points не целое: 1e400",
	} {
		if b := res[u]; b.Points != nil || b.Error != want || b.Expected != 20 {
			t.Fatalf("%s: %+v, ожидалась ошибка %q", u, b, want)
		}
	}
}

func TestBalancesBusyAndLimit(t *testing.T) {
	defer func(d time.Duration) { balancesWait = d }(balancesWait)
	balancesWait = 300 * time.Millisecond
	orders := bonusOrders(t, map[string][2]string{"u-0": {"200", `{"points":0}`}, "u-1": {"hang", ""}})
	srv, st := busStand(t, orders.URL)
	setDelivery(st, dlvEvent{ID: "a", UserID: "u-1", Points: 10})

	st.mu.Lock()
	st.balancing = true
	st.mu.Unlock()
	c, b := standDo(t, http.MethodPost, srv.URL+"/admin/bus/balances", "", "", "")
	if c != 409 || !strings.Contains(string(b), `"busy"`) {
		t.Fatalf("вторая проверка: %d %s", c, b)
	}
	st.mu.Lock()
	st.balancing = false
	st.mu.Unlock()

	start := time.Now()
	got := balances(t, srv.URL)
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("проверка шла %v", d)
	}
	if len(got) != 2 || got[0].UserID != "u-1" || got[0].Points != nil || got[0].Error != "нет ответа" ||
		got[1].UserID != "u-0" || got[1].Error != "" {
		t.Fatalf("%+v", got)
	}

	// Без ORDERS_URL — 503.
	srv2, _ := busStand(t, "")
	if c, _ := standDo(t, http.MethodPost, srv2.URL+"/admin/bus/balances", "", "", ""); c != 503 {
		t.Fatalf("без Orders: %d", c)
	}
}

// bonusLedger — Orders, который правильно копит бонусы: по событию один раз,
// по всем партиям.
func bonusLedger(t *testing.T) *httptest.Server {
	var mu sync.Mutex
	seen := map[string]bool{}
	points := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/webhooks/delivery" {
			var ev struct {
				ID     string `json:"id"`
				UserID string `json:"user_id"`
				Points int    `json:"bonus_points"`
			}
			_ = json.NewDecoder(r.Body).Decode(&ev)
			mu.Lock()
			if !seen[ev.ID] {
				seen[ev.ID] = true
				points[ev.UserID] += ev.Points
			}
			mu.Unlock()
			w.WriteHeader(204)
			return
		}
		u, ok := strings.CutSuffix(strings.TrimPrefix(r.URL.Path, "/users/"), "/bonus")
		if !ok {
			http.NotFound(w, r)
			return
		}
		mu.Lock()
		fmt.Fprintf(w, `{"points":%d}`, points[u])
		mu.Unlock()
	}))
	t.Cleanup(srv.Close)
	return srv
}

// Ожидаемый баланс копится по всем партиям; redeliver его не меняет, reset
// обнуляет. delivery в фактах — про последнюю партию.
func TestBalancesAcrossBatches(t *testing.T) {
	defer func(d time.Duration) { dlvRetry = d }(dlvRetry)
	dlvRetry = 50 * time.Millisecond
	srv, _ := busStand(t, bonusLedger(t).URL)
	total := map[string]int{}
	for i, body := range []string{
		`{"events":6,"users":3,"max_copies":2,"parallel_rate":0.5,"rate_per_s":200}`,
		`{"events":4,"users":4,"max_copies":2,"parallel_rate":0.5,"rate_per_s":200}`,
	} {
		if c := call(t, http.MethodPost, srv.URL+"/admin/bus/deliver", body); c != 202 {
			t.Fatalf("партия %d: %d", i+1, c)
		}
		d := waitDelivery(t, srv.URL, func(d *DeliveryFacts) bool { return d.Pending == 0 })
		if c := call(t, http.MethodPost, srv.URL+"/admin/bus/redeliver", ``); c != 202 {
			t.Fatalf("redeliver: %d", c)
		}
		d = waitDelivery(t, srv.URL, func(d *DeliveryFacts) bool { return d.Pending == 0 })
		if len(d.Events) != []int{6, 4}[i] {
			t.Fatalf("в фактах не текущая партия: %d событий", len(d.Events))
		}
		for u, v := range d.ExpectedByUser {
			total[u] += v
		}
	}
	got := balances(t, srv.URL)
	if len(got) != 5 { // u-1…u-4 и u-0
		t.Fatalf("%+v", got)
	}
	for _, b := range got {
		if b.Expected != total[b.UserID] || b.Points == nil || *b.Points != int64(b.Expected) || b.Error != "" {
			t.Fatalf("%s: %+v, ожидалось %d", b.UserID, b, total[b.UserID])
		}
	}
	if total["u-1"] == 0 || total["u-4"] == 0 {
		t.Fatalf("total %v", total)
	}

	if c := call(t, http.MethodPost, srv.URL+"/admin/reset", ``); c != 204 {
		t.Fatalf("reset: %d", c)
	}
	if got := balances(t, srv.URL); len(got) != 1 || got[0].UserID != "u-0" || got[0].Expected != 0 {
		t.Fatalf("после reset: %+v", got)
	}
}
