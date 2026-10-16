package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func getEvents(t *testing.T, url, token string, since int64) (int, eventsResp) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/admin/events?since=%d", url, since), nil)
	if token != "" {
		req.Header.Set("X-Admin-Token", token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out eventsResp
	if resp.StatusCode == http.StatusOK {
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}
	}
	return resp.StatusCode, out
}

func call(t *testing.T, method, url, body string) int {
	t.Helper()
	req, _ := http.NewRequest(method, url, strings.NewReader(body))
	if method != http.MethodGet {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode
}

func TestUIWithoutTokenDataWithToken(t *testing.T) {
	st := newState(Modes{Seed: 1})
	st.adminToken = "secret"
	srv := httptest.NewServer(st.routes())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/ui")
	if err != nil {
		t.Fatal(err)
	}
	page, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/html") ||
		!bytes.Contains(page, []byte("/admin/events")) || resp.Header.Get("Content-Security-Policy") == "" {
		t.Fatalf("/ui: %d %v", resp.StatusCode, resp.Header)
	}
	if c, _ := getEvents(t, srv.URL, "", 0); c != http.StatusForbidden {
		t.Fatalf("events without token: %d", c)
	}
	if c, _ := getEvents(t, srv.URL, "wrong", 0); c != http.StatusForbidden {
		t.Fatalf("events with wrong token: %d", c)
	}
	if c, _ := getEvents(t, srv.URL, "secret", 0); c != http.StatusOK {
		t.Fatalf("events with token: %d", c)
	}
}

// Страница не подсказывает решение и не ведёт наружу.
func TestUIPageNoHints(t *testing.T) {
	low := strings.ToLower(string(uiPage))
	for _, w := range []string{"ключ", "lookup", "таймаут", "повтор", "идемпот", "размыкат", "seed", "has_key",
		"idempotency", "outbox", "inbox", "http://", "https://", "innerhtml", "eval("} {
		if strings.Contains(low, w) {
			t.Fatalf("в ui.html %q", w)
		}
	}
}

func TestLabelInModes(t *testing.T) {
	srv, _ := setup(t, Modes{Seed: 1})
	if c := call(t, http.MethodPut, srv.URL+"/admin/modes", `{"seed":3,"label":"S4 · проба","payment":{"fail_rate":0.5}}`); c != 200 {
		t.Fatalf("put: %d", c)
	}
	_, ev := getEvents(t, srv.URL, "", 0)
	if ev.Modes.Label != "S4 · проба" || ev.Modes.Payment.FailRate != 0.5 {
		t.Fatalf("modes %+v", ev.Modes)
	}
	if f := facts(t, srv.URL); f.Modes.Label != "S4 · проба" {
		t.Fatalf("facts modes %+v", f.Modes)
	}
	if c := call(t, http.MethodPut, srv.URL+"/admin/modes", `{"label":"`+strings.Repeat("я", maxLabel+1)+`"}`); c != 400 {
		t.Fatalf("long label: %d", c)
	}
	call(t, http.MethodPost, srv.URL+"/admin/reset", "")
	if _, ev := getEvents(t, srv.URL, "", 0); ev.Modes.Label != "" {
		t.Fatalf("label after reset: %q", ev.Modes.Label)
	}
	// Без label режимы по-прежнему принимаются.
	if c := call(t, http.MethodPut, srv.URL+"/admin/modes", `{"seed":1,"payment":{}}`); c != 200 {
		t.Fatalf("put без label: %d", c)
	}
}

func TestEventsAndNewFacts(t *testing.T) {
	srv, _ := setup(t, Modes{Seed: 1, Payment: PaymentModes{DeclineRate: 0, LostResponse: "first_per_order"}})
	resv := `{"order_id":"o1","items":[{"sku":"sku-1","qty":1}]}`
	if c := call(t, http.MethodPost, srv.URL+"/reservations", resv); c != 201 {
		t.Fatalf("reserve %d", c)
	}
	if _, err := pay(t, srv.URL, "o1", "", 150); err == nil {
		t.Fatal("ожидался потерянный ответ")
	}
	if c, _ := pay(t, srv.URL, "o1", "", 150); c != 201 {
		t.Fatalf("pay %d", c)
	}
	call(t, http.MethodGet, srv.URL+"/payments?order_id=o1", "")
	call(t, http.MethodGet, srv.URL+"/reservations?order_id=o1", "")
	call(t, http.MethodDelete, srv.URL+"/reservations/o1", "")

	_, ev := getEvents(t, srv.URL, "", 0)
	type row struct{ svc, method, path, outcome string }
	want := []row{
		{"inventory", "POST", "/reservations", "201"},
		{"payment", "POST", "/payments", "lost"},
		{"payment", "POST", "/payments", "201"},
		{"payment", "GET", "/payments", "200"},
		{"inventory", "GET", "/reservations", "200"},
		{"inventory", "DELETE", "/reservations/{order_id}", "204"},
	}
	if len(ev.Events) != len(want) {
		t.Fatalf("events %+v", ev.Events)
	}
	for i, w := range want {
		e := ev.Events[i]
		if (row{e.Service, e.Method, e.Path, e.Outcome}) != w || e.OrderID != "o1" || e.At.IsZero() {
			t.Fatalf("#%d: %+v, want %+v", i, e, w)
		}
		if i > 0 && e.Seq <= ev.Events[i-1].Seq {
			t.Fatalf("seq не растёт: %+v", ev.Events)
		}
	}
	if ev.Events[1].AmountCents != 150 || ev.Seq != ev.Events[5].Seq {
		t.Fatalf("%+v", ev)
	}
	s := ev.Summary
	if s.Payment.Requests != 2 || s.Payment.Gets != 1 || s.Charges.Total != 2 || s.Charges.Orders != 1 ||
		s.Charges.DoubleCharged != 1 || s.Reservations.Released != 1 || s.Payment.ByOutcome["lost"] != 1 {
		t.Fatalf("summary %+v", s)
	}
	b, _ := json.Marshal(ev)
	if bytes.Contains(b, []byte("seed")) || bytes.Contains(b, []byte("has_key")) {
		t.Fatalf("лишнее в /admin/events: %s", b)
	}

	// since: только новые.
	if _, ev2 := getEvents(t, srv.URL, "", ev.Seq); len(ev2.Events) != 0 || ev2.Seq != ev.Seq || ev2.Run != ev.Run {
		t.Fatalf("since: %+v", ev2)
	}

	f := facts(t, srv.URL)
	if l := f.Payment.Requests.Log; len(l) != 2 || l[0].Method != "POST" || l[0].AmountCents != 150 {
		t.Fatalf("payment log %+v", l)
	}
	if l := f.Payment.GetRequests.Log; len(l) != 1 || l[0].Method != "GET" {
		t.Fatalf("lookup log %+v", l)
	}
	il := f.Inventory.Log
	if len(il) != 3 || il[0].Op != "reserve" || il[0].Outcome != "201" || il[1].Op != "get" ||
		il[2].Op != "release" || il[2].Method != "DELETE" || il[2].OrderID != "o1" {
		t.Fatalf("inventory log %+v", il)
	}

	// reset: новый run, лента с начала.
	call(t, http.MethodPost, srv.URL+"/admin/reset", "")
	_, ev3 := getEvents(t, srv.URL, "", ev.Seq)
	if ev3.Run == ev.Run || len(ev3.Events) != 0 || len(facts(t, srv.URL).Inventory.Log) != 0 {
		t.Fatalf("после reset: %+v", ev3)
	}
	call(t, http.MethodPost, srv.URL+"/reservations", resv)
	if _, ev4 := getEvents(t, srv.URL, "", 0); len(ev4.Events) != 1 || ev4.Events[0].Seq <= ev.Seq {
		t.Fatalf("seq после reset: %+v", ev4.Events)
	}
}

func TestEventsDelayedResponse(t *testing.T) {
	srv, _ := setup(t, Modes{Seed: 1, Payment: PaymentModes{BlackholeS: 0.4}})
	c := &http.Client{Timeout: 3 * time.Second}
	b, _ := json.Marshal(chargeRequest{OrderID: "o1", AmountCents: 1, Currency: "RUB"})
	if _, err := c.Post(srv.URL+"/payments", "application/json", bytes.NewReader(b)); err == nil {
		t.Fatal("ожидалось: без ответа")
	}
	var ev eventsResp
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, ev = getEvents(t, srv.URL, "", 0)
		if len(ev.Events) == 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if len(ev.Events) != 2 || !ev.Events[0].Wait || ev.Events[0].Outcome != "blackhole" ||
		ev.Events[1].Kind != "done" || ev.Events[1].Ref != ev.Events[0].Seq || ev.Events[1].Outcome != "no_response" {
		t.Fatalf("events %+v", ev.Events)
	}
	if ev.Blackhole == nil || !ev.Blackhole.Until.After(ev.Blackhole.From) {
		t.Fatalf("blackhole %+v", ev.Blackhole)
	}
	f := facts(t, srv.URL)
	if d := f.Payment.Requests.Log[0].DoneAt; d == nil || d.Sub(f.Payment.Requests.Log[0].At) < 300*time.Millisecond {
		t.Fatalf("done_at %+v", f.Payment.Requests.Log[0])
	}

	// Задержка ответа: запрос сразу с исходом, done — после задержки.
	srv2, _ := setup(t, Modes{Seed: 1, Payment: PaymentModes{LatencyMS: 200}})
	if code, err := pay(t, srv2.URL, "o2", "", 5); err != nil || code != 201 {
		t.Fatalf("%d %v", code, err)
	}
	_, ev = getEvents(t, srv2.URL, "", 0)
	for i := 0; i < 50 && len(ev.Events) < 2; i++ {
		time.Sleep(10 * time.Millisecond)
		_, ev = getEvents(t, srv2.URL, "", 0)
	}
	if len(ev.Events) != 2 || ev.Events[0].Outcome != "201" || !ev.Events[0].Wait || ev.Events[1].Outcome != "201" {
		t.Fatalf("latency events %+v", ev.Events)
	}
}

func TestEventsPaging(t *testing.T) {
	st := newState(Modes{Seed: 1})
	h := st.routes()
	n := eventsPage + 50
	for i := 0; i < n; i++ {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/reservations?order_id=o", nil))
	}
	get := func(since int64) eventsResp {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, localReq(http.MethodGet, fmt.Sprintf("/admin/events?since=%d", since)))
		var out eventsResp
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	a := get(0)
	if len(a.Events) != eventsPage || !a.More {
		t.Fatalf("page 1: %d %v", len(a.Events), a.More)
	}
	b := get(a.Seq)
	if len(b.Events) != 50 || b.More || b.Events[0].Seq != a.Seq+1 {
		t.Fatalf("page 2: %d %v", len(b.Events), b.More)
	}
}

// Длинный order_id в ленте обрезается, в фактах — целиком.
func TestEventsLongOrderID(t *testing.T) {
	srv, _ := setup(t, Modes{Seed: 1})
	id := strings.Repeat("ы", 500)
	pay(t, srv.URL, id, "", 1)
	_, ev := getEvents(t, srv.URL, "", 0)
	if len([]rune(ev.Events[0].OrderID)) != maxEventOrderID {
		t.Fatalf("len %d", len([]rune(ev.Events[0].OrderID)))
	}
	if facts(t, srv.URL).Payment.Requests.Log[0].OrderID != id {
		t.Fatal("факты обрезаны")
	}
}

// Панель управления ходит только в известные /admin/* и не выводит чужой текст как HTML.
func TestUIControls(t *testing.T) {
	page := string(uiPage)
	for _, want := range []string{"/admin/modes", "/admin/reset", "/admin/orders", "/admin/orders/check",
		"/admin/runner/run", "/admin/runner/status", "/admin/runner/report", "/admin/runner/params",
		"Прогнать все (S1–S5)", "Отчёт HTML", "Итог (GET)", "make scenarios",
		"'X-Stand-Client': 'ui'", "runner_active", "Идёт прогон make scenarios",
		"Отправка прервана: журнал очищен", "Отправка прервана: начался прогон", "2,5 мин", "открыть отчёт",
		"headers['Content-Type'] = 'application/json'",
		// Задание 2: шина и доставка.
		"/admin/bus/deliver", "/admin/bus/redeliver", "/admin/bus/balances", "/admin/runner/params?task=",
		"/admin/runner/report?task=", "task: task, ids: ids, wait_s: wait", "Шина событий", "Доля висящих запросов",
		"Убрать отказ", "как в S6", "Доставка (задание 2)", "Доставить события", "Прислать копии ещё раз",
		"Проверить балансы", "должно быть 0", "Публикаций в шину", "Заказов с событием", "<th>Событие</th>",
		"paid без события", "разные id события", "bus_down", "Прогнать S1 и S6",
		"S7 и S8 — только <code>make scenarios TASK=2</code> (они перезапускают Orders)",
		"payment: payment, bus: bus", "curModes('bus')", "curModes('payment')"} {
		if !strings.Contains(page, want) {
			t.Fatalf("в ui.html нет %q", want)
		}
	}
	for _, bad := range []string{"insertAdjacentHTML", "outerHTML", "document.write"} {
		if strings.Contains(page, bad) {
			t.Fatalf("в ui.html %q", bad)
		}
	}
}
