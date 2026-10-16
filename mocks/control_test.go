package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func adminDo(t *testing.T, method, url, token, body string) (int, []byte, http.Header) {
	t.Helper()
	req, _ := http.NewRequest(method, url, strings.NewReader(body))
	if token != "" {
		req.Header.Set("X-Admin-Token", token)
	}
	if method != http.MethodGet {
		req.Header.Set("Content-Type", "application/json")
	}
	c := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b, resp.Header
}

// Ни в фактах, ни в ленте, ни в режимах нет слов-подсказок.
func TestAdminNoHints(t *testing.T) {
	srv, _ := setup(t, Modes{Seed: 9, Payment: PaymentModes{LostResponse: "first_per_order", LatencyMS: 1}})
	pay(t, srv.URL, "o1", "k1", 100)
	pay(t, srv.URL, "o1", "k1", 100)
	call(t, http.MethodGet, srv.URL+"/payments?order_id=o1", "")
	time.Sleep(20 * time.Millisecond)
	for _, path := range []string{"/admin/facts", "/admin/events?since=0", "/admin/modes"} {
		_, b, h := adminDo(t, http.MethodGet, srv.URL+path, "", "")
		low := strings.ToLower(string(b))
		for _, w := range []string{"ключ", "key", "lookup", "seed", "таймаут", "повтор", "идемпот", "размыкат"} {
			if strings.Contains(low, w) {
				t.Fatalf("%s: %q в %s", path, w, b)
			}
		}
		if h.Get("X-Content-Type-Options") != "nosniff" {
			t.Fatalf("%s: нет nosniff", path)
		}
	}
	c, b, _ := adminDo(t, http.MethodPut, srv.URL+"/admin/modes", "", `{"seed":5,"payment":{}}`)
	if c != 200 || bytes.Contains(b, []byte("seed")) {
		t.Fatalf("PUT: %d %s", c, b)
	}
	f := facts(t, srv.URL)
	if f.Payment.GetRequests.Total != 1 || len(f.Payment.GetRequests.Log) != 1 {
		t.Fatalf("get_requests %+v", f.Payment.GetRequests)
	}
}

// Задержка ответа, клиент ушёл раньше: списание есть, в ленте — client_closed,
// в фактах — client_closed; списание засчитывается в сводке после этого.
func TestLatencyClientClosed(t *testing.T) {
	srv, _ := setup(t, Modes{Seed: 1, Payment: PaymentModes{LatencyMS: 600}})
	b, _ := json.Marshal(chargeRequest{OrderID: "o1", AmountCents: 5, Currency: "RUB"})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/payments", bytes.NewReader(b))
	if _, err := http.DefaultClient.Do(req); err == nil {
		t.Fatal("ожидалось: клиент ушёл")
	}
	_, ev := getEvents(t, srv.URL, "", 0)
	if len(ev.Events) == 1 && ev.Summary.Charges.Total != 0 {
		t.Fatalf("списание засчитано до исхода: %+v", ev.Summary.Charges)
	}
	deadline := time.Now().Add(2 * time.Second)
	for len(ev.Events) < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
		_, ev = getEvents(t, srv.URL, "", 0)
	}
	if len(ev.Events) != 2 || ev.Events[1].Outcome != "client_closed" || !ev.Events[0].Charged || ev.Summary.Charges.Total != 1 {
		t.Fatalf("events %+v summary %+v", ev.Events, ev.Summary.Charges)
	}
	l := facts(t, srv.URL).Payment.Requests.Log[0]
	if !l.ClientClosed || l.DoneAt == nil || l.Outcome != "201" {
		t.Fatalf("log %+v", l)
	}
}

func TestEventsRing(t *testing.T) {
	old := maxEvents
	maxEvents = 100
	defer func() { maxEvents = old }()
	st := newState(Modes{Seed: 1})
	h := st.routes()
	for i := 0; i < 250; i++ {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/reservations?order_id=o", nil))
	}
	if len(st.events) > 111 {
		t.Fatalf("лента растёт: %d", len(st.events))
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, localReq(http.MethodGet, "/admin/events?since=5"))
	var out eventsResp
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	if !out.Truncated || len(out.Events) != 100 || out.Events[0].Seq != 151 || out.Seq != 250 {
		t.Fatalf("truncated=%v n=%d first=%d seq=%d", out.Truncated, len(out.Events), out.Events[0].Seq, out.Seq)
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, localReq(http.MethodGet, "/admin/events?since=200"))
	out = eventsResp{}
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	if out.Truncated || len(out.Events) != 50 {
		t.Fatalf("since=200: %v %d", out.Truncated, len(out.Events))
	}
}

func TestUIRedirect(t *testing.T) {
	srv, _ := setup(t, Modes{Seed: 1})
	c, _, h := adminDo(t, http.MethodGet, srv.URL+"/ui/", "", "")
	if c != http.StatusFound || h.Get("Location") != "/ui" {
		t.Fatalf("%d %v", c, h)
	}
}

func TestOrdersTarget(t *testing.T) {
	for _, bad := range []string{"ftp://x", "http://", "http://x/path", "http://u:p@x", "x"} {
		if _, err := ordersTarget(bad); err == nil {
			t.Fatalf("принят %q", bad)
		}
	}
	if u, err := ordersTarget("http://orders:8080/"); err != nil || u != "http://orders:8080" {
		t.Fatalf("%q %v", u, err)
	}
}

func TestSendOrders(t *testing.T) {
	var n atomic.Int64
	orders := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/orders":
			var body orderBody
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.UserID == "" || len(body.Items) == 0 || len(body.Items) > 3 {
				w.WriteHeader(400)
				return
			}
			k := n.Add(1)
			if k == 2 {
				w.WriteHeader(500)
				return
			}
			w.WriteHeader(201)
			fmt.Fprintf(w, `{"id":"o-%d","status":"pending"}`, k)
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/orders/o-"):
			fmt.Fprint(w, `{"status":"paid"}`)
		default:
			w.WriteHeader(404)
		}
	}))
	defer orders.Close()

	st := newState(Modes{Seed: 1})
	st.adminToken = "tok"
	srv := httptest.NewServer(st.routes())
	defer srv.Close()

	if c, _, _ := adminDo(t, http.MethodPost, srv.URL+"/admin/orders", "", `{"count":1,"rate_per_s":1}`); c != 403 {
		t.Fatalf("без токена: %d", c)
	}
	if c, _, _ := adminDo(t, http.MethodPost, srv.URL+"/admin/orders", "tok", `{"count":1,"rate_per_s":1}`); c != 503 {
		t.Fatalf("без ORDERS_URL: %d", c)
	}
	st.ordersURL = orders.URL
	for _, bad := range []string{`{"count":0,"rate_per_s":1}`, `{"count":501,"rate_per_s":1}`, `{"count":5,"rate_per_s":0}`,
		`{"count":500,"rate_per_s":0.5}`, `{"count":1,"rate_per_s":1,"url":"http://evil"}`} {
		if c, _, _ := adminDo(t, http.MethodPost, srv.URL+"/admin/orders", "tok", bad); c != 400 {
			t.Fatalf("%s: %d", bad, c)
		}
	}
	if c, b, _ := adminDo(t, http.MethodPost, srv.URL+"/admin/orders", "tok", `{"count":5,"rate_per_s":50}`); c != 202 {
		t.Fatalf("send: %d %s", c, b)
	}
	if c, _, _ := adminDo(t, http.MethodPost, srv.URL+"/admin/orders", "tok", `{"count":1,"rate_per_s":1}`); c != 409 {
		t.Fatalf("вторая отправка: %d", c)
	}
	var ev eventsResp
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		_, ev = getEvents(t, srv.URL, "tok", 0)
		if ev.Sending != nil && !ev.Sending.Active {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if ev.Sending == nil || ev.Sending.Active || ev.Sending.Done != 5 || len(ev.Events) != 5 || !ev.OrdersEnabled {
		t.Fatalf("sending %+v events %d", ev.Sending, len(ev.Events))
	}
	codes := map[string]int{}
	for _, e := range ev.Events {
		if e.Service != "orders" || e.Method != "POST" || e.Path != "/orders" {
			t.Fatalf("%+v", e)
		}
		codes[e.Outcome]++
		if e.Outcome == "201" && (e.OrderID == "" || e.Status != "pending") {
			t.Fatalf("%+v", e)
		}
	}
	if codes["201"] != 4 || codes["500"] != 1 {
		t.Fatalf("codes %v", codes)
	}
	c, b, _ := adminDo(t, http.MethodPost, srv.URL+"/admin/orders/check", "tok", "")
	var chk struct {
		Statuses map[string]string `json:"statuses"`
	}
	if c != 200 || json.Unmarshal(b, &chk) != nil || len(chk.Statuses) != 4 || chk.Statuses["o-1"] != "paid" {
		t.Fatalf("check %d %s", c, b)
	}
	// reset — id забываются.
	adminDo(t, http.MethodPost, srv.URL+"/admin/reset", "tok", "")
	_, b, _ = adminDo(t, http.MethodPost, srv.URL+"/admin/orders/check", "tok", "")
	if !bytes.Contains(b, []byte(`"statuses":{}`)) {
		t.Fatalf("после reset: %s", b)
	}
}

func TestRunnerProxy(t *testing.T) {
	var gotPath, gotToken atomic.Value
	runner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath.Store(r.Method + " " + r.URL.Path)
		gotToken.Store(r.Header.Get("X-Admin-Token"))
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"state":"idle"}`)
	}))
	st := newState(Modes{Seed: 1})
	st.adminToken = "tok"
	p, err := newRunnerProxy(runner.URL)
	if err != nil {
		t.Fatal(err)
	}
	st.runnerUI = p
	srv := httptest.NewServer(st.routes())
	defer srv.Close()

	if c, _, _ := adminDo(t, http.MethodGet, srv.URL+"/admin/runner/status", "", ""); c != 403 {
		t.Fatalf("без токена: %d", c)
	}
	c, b, _ := adminDo(t, http.MethodGet, srv.URL+"/admin/runner/status", "tok", "")
	if c != 200 || !bytes.Contains(b, []byte("idle")) || gotPath.Load() != "GET /status" || gotToken.Load() != "tok" {
		t.Fatalf("%d %s %v", c, b, gotPath.Load())
	}
	if c, _, _ := adminDo(t, http.MethodPost, srv.URL+"/admin/runner/run", "tok", `{"only":"S1"}`); c != 200 || gotPath.Load() != "POST /run" {
		t.Fatalf("run %d", c)
	}
	for _, bad := range []string{"/admin/runner/../facts", "/admin/runner/other", "/admin/runner/"} {
		if c, _, _ := adminDo(t, http.MethodGet, srv.URL+bad, "tok", ""); c != 404 && c != 301 && c != 307 {
			t.Fatalf("%s: %d", bad, c)
		}
	}
	if c, _, _ := adminDo(t, http.MethodGet, srv.URL+"/admin/runner/run", "tok", ""); c != 405 {
		t.Fatalf("GET run: %d", c)
	}
	runner.Close()
	c, b, _ = adminDo(t, http.MethodGet, srv.URL+"/admin/runner/status", "tok", "")
	if c != 502 || !bytes.Contains(b, []byte("make ui")) {
		t.Fatalf("без runner-ui: %d %s", c, b)
	}
	if _, err := newRunnerProxy("http://x/path"); err == nil {
		t.Fatal("путь в RUNNER_UI_URL принят")
	}
}

// localReq — запрос к стенду так, как его шлёт браузер на localhost.
func localReq(method, path string) *http.Request {
	r := httptest.NewRequest(method, path, nil)
	r.Host = "localhost:8090"
	return r
}
