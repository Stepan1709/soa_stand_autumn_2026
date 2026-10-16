package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestTask2Params(t *testing.T) {
	p, err := loadParams("2", "")
	if err != nil {
		t.Fatal(err)
	}
	if p.Scenarios.S6.Bus.DownS != 60 || p.Scenarios.S7.KillOrders == 0 || p.Scenarios.S8.Events == 0 {
		t.Fatalf("%+v", p.Scenarios)
	}
	bad := []func(p *Params){
		func(p *Params) { p.Scenarios.S6.Bus.DownS = 0 },
		func(p *Params) { p.Scenarios.S6.Orders = 1000 }, // 100 с ≥ 60 с отказа
		func(p *Params) { p.Scenarios.S6.Bus.HangRate = 2 },
		func(p *Params) { p.Scenarios.S6.Bus.DownAfterS = 3 },
		func(p *Params) { p.Scenarios.S7.KillOrders = 0 },
		func(p *Params) { p.Scenarios.S7.KillLatencyMS = 100 },
		func(p *Params) { p.Scenarios.S7.RestartS = 0 },
		func(p *Params) { p.Scenarios.S8.KillAfterEvents = p.Scenarios.S8.Events },
		func(p *Params) { p.Scenarios.S8.Users = 0 },
		func(p *Params) { p.Scenarios.S8.MaxCopies = 0 },
	}
	for i, f := range bad {
		q, _ := loadParams("2", "")
		f(q)
		if q.validate("2") == nil {
			t.Fatalf("#%d принят", i)
		}
	}
	// Задание 1 о параметрах задания 2 не знает и не проверяет их.
	if _, err := loadParams("1", ""); err != nil {
		t.Fatal(err)
	}
}

// busState — прогон S6/S7: заказы и события в шине по заказам.
func busState(t *testing.T, orders []tOrder, events map[string][]string) *runState {
	rs := mkState(t, orders)
	rs.facts.Bus.EventsByOrder = events
	rs.facts.Bus.Requests.Total = 1
	return rs
}

func TestChecksBus(t *testing.T) {
	clean := busState(t, []tOrder{paidOK("a"), declined("b")}, map[string][]string{"a": {"e1"}})
	if f := failed(checksBus(nil, clean)); len(f) != 0 {
		t.Fatalf("чистый прогон: %v", f)
	}
	rs := busState(t, []tOrder{paidOK("a"), paidOK("b"), declined("c"), {id: "d", status: "404", notSent: true}},
		map[string][]string{"b": {"e1", "e2"}, "c": {"e3"}, "d": {"e4"}})
	cs := checksBus(nil, rs)
	if c := findCheck(t, cs, "каждый paid заказ есть в шине"); c.OK || strings.Join(c.Orders, ",") != "a" {
		t.Fatalf("%+v", c)
	}
	if c := findCheck(t, cs, "события order.paid — только"); c.OK || strings.Join(c.Orders, ",") != "c,d" {
		t.Fatalf("%+v", c)
	}
	if c := findCheck(t, cs, "у всех копий"); c.OK || strings.Join(c.Orders, ",") != "b" {
		t.Fatalf("%+v", c)
	}
	// События по заказам прошлых сценариев не считаются.
	rs = busState(t, []tOrder{paidOK("a")}, map[string][]string{"a": {"e1"}, "old": {"x", "y"}})
	rs.prev["old"] = true
	if f := failed(checksBus(nil, rs)); len(f) != 0 {
		t.Fatalf("прошлый сценарий: %v", f)
	}
	j := strings.Join(journal(rs, scenarioByID("S6")), "\n")
	if !strings.Contains(j, "заказов с событием 1, paid без события 0") {
		t.Fatalf("журнал:\n%s", j)
	}
}

func TestKilledOrdersExcludedFromBudget(t *testing.T) {
	rs := mkState(t, []tOrder{paidOK("a"), {phase: "kill"}, {id: "k", status: "paid", charges: 1, resv: "held", phase: "kill"}})
	rs.orders[1].Err, rs.orders[1].Code = "соединение сброшено", 0
	for _, name := range []string{"ни одного 5xx", "каждый ответ на POST /orders", "ответы Orders по контракту"} {
		if c := findCheck(t, commonChecks(rs.params, rs), name); !c.OK {
			t.Fatalf("%s: %+v", name, c)
		}
	}
	// Деньги по убитому заказу проверяются как у всех.
	rs.final["k"] = "404"
	if c := findCheck(t, commonChecks(rs.params, rs), "каждый заказ, за который списаны"); c.OK {
		t.Fatal("списание без заказа прошло")
	}
}

func s8State(t *testing.T, bonus map[string]bonusResult) *runState {
	rs := mkState(t, nil)
	rs.facts.Bus.Delivery = &struct {
		Events         []dlvEvent     `json:"events"`
		Pending        int            `json:"pending"`
		AckedEvents    int            `json:"acked_events"`
		Attempts       int            `json:"attempts"`
		ExpectedByUser map[string]int `json:"expected_by_user"`
	}{
		Events: []dlvEvent{
			{ID: "e1", UserID: "u-1", Points: 30, Copies: 2, Acked: 2},
			{ID: "e2", UserID: "u-1", Points: 20, Copies: 1, Acked: 1},
			{ID: "e3", UserID: "u-2", Points: 10, Copies: 3, Acked: 3},
		},
		ExpectedByUser: map[string]int{"u-1": 50, "u-2": 10},
	}
	rs.bonus, rs.unknownUser = bonus, "u-0"
	return rs
}

func TestChecksS8(t *testing.T) {
	ok := map[string]bonusResult{"u-0": {}, "u-1": {Points: 50}, "u-2": {Points: 10}}
	if f := failed(checksS8(nil, s8State(t, ok))); len(f) != 0 {
		t.Fatalf("чистый прогон: %v", f)
	}
	rs := s8State(t, map[string]bonusResult{"u-0": {Err: "GET 404"}, "u-1": {Points: 80}, "u-2": {Points: 10}})
	rs.facts.Bus.Delivery.Events[2].Acked = 1
	rs.facts.Bus.Delivery.Pending = 2
	cs := checksS8(nil, rs)
	if c := findCheck(t, cs, "каждая доставка"); c.OK || c.Orders[0] != "e3" {
		t.Fatalf("%+v", c)
	}
	if c := findCheck(t, cs, "баланс покупателя"); c.OK || strings.Join(c.Orders, ",") != "u-1" {
		t.Fatalf("%+v", c)
	}
	if c := findCheck(t, cs, "у покупателя без начислений"); c.OK || !strings.Contains(c.Detail, "GET 404") {
		t.Fatalf("%+v", c)
	}
	var res scenarioResult
	evaluate(&res, scenarioByID("S8"), &rs.params.Scenarios.S8, rs)
	j := strings.Join(res.Journal, "\n")
	if res.Pass || !strings.Contains(j, "балансы (получено/ожидалось): u-1 80/50, u-2 10/10") ||
		!strings.Contains(j, "[баланс покупателя — сумма бонусов по разным событиям] u-1: баланс 80, ожидалось 50") {
		t.Fatalf("журнал S8:\n%s", j)
	}
	for _, c := range res.Checks {
		if strings.Contains(c.Name, "POST /orders") {
			t.Fatalf("в S8 общие проверки заказов: %s", c.Name)
		}
	}
}

func TestLevel2(t *testing.T) {
	mk := func(pass ...string) []scenarioResult {
		ok := map[string]bool{}
		for _, id := range pass {
			ok[id] = true
		}
		var out []scenarioResult
		for _, id := range tasks["2"] {
			out = append(out, scenarioResult{ID: id, Pass: ok[id]})
		}
		return out
	}
	for _, c := range []struct {
		res  []scenarioResult
		want string
	}{
		{mk("S1", "S6", "S7", "S8"), "полностью"},
		{mk("S1", "S6", "S7"), "засчитано"},
		{mk("S1", "S6", "S8"), "не засчитано"},
		{mk("S6", "S7", "S8"), "не засчитано"},
	} {
		if got, _ := level("2", tasks["2"], c.res); got != c.want {
			t.Fatalf("%v: %q", c.res, got)
		}
	}
}

func TestRestartOrders(t *testing.T) {
	orders := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	defer orders.Close()
	r := newRunner(testParams(t), orders.URL, "http://mocks", "", newOutput(&strings.Builder{}))
	if _, err := r.restartOrders(1); err != errNoChaos {
		t.Fatalf("без каталога: %v", err)
	}
	dir := t.TempDir()
	r.chaosDir = dir
	// Цикл chaos.sh: увидел kill — отвечает done.
	go func() {
		for i := 0; i < 100; i++ {
			if b, err := os.ReadFile(filepath.Join(dir, "kill")); err == nil {
				if string(b) != "5" {
					t.Errorf("kill: %q", b)
				}
				os.Remove(filepath.Join(dir, "kill"))
				os.WriteFile(filepath.Join(dir, "done"), nil, 0o644)
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()
	if _, err := r.restartOrders(5); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "done")); !os.IsNotExist(err) {
		t.Fatal("done не убран")
	}

	defer func(d time.Duration) { chaosWait = d }(chaosWait)
	chaosWait = 200 * time.Millisecond
	if _, err := r.restartOrders(0); err == nil || !strings.Contains(err.Error(), "chaos.sh") {
		t.Fatalf("без ответа: %v", err)
	}
}
