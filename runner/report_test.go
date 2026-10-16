package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// forbidden — слова, которые подсказывают решение: их нет ни в отчёте, ни в выводе.
var forbidden = []string{"ключ", "lookup", "тайм", "повтор", "идемпот", "размыкат", "seed", "has_key", "idempotency", "keyed"}

func hostileRun(t *testing.T) (*report, *output) {
	t.Helper()
	evil := `<script>alert(1)</script>"'&`
	rs := mkState(t, []tOrder{
		paidOK("ok-1"),
		{id: evil, status: "paid", charges: 2, resv: "held", latency: 1500 * time.Millisecond},
		{id: "rej", status: "rejected", resv: "held", declines: 1},
	})
	t0 := time.Now().Add(-10 * time.Second)
	done := t0.Add(2 * time.Second)
	f := rs.facts
	f.Payment.Requests.Log = []payLog{
		{At: t0.Add(100 * time.Millisecond), OrderID: evil, Outcome: "blackhole", Blackhole: true, Method: "POST", AmountCents: 3980, DoneAt: &done},
		{At: t0.Add(200 * time.Millisecond), OrderID: evil, Outcome: "lost", Method: "POST", AmountCents: 3980},
		{At: t0.Add(300 * time.Millisecond), OrderID: evil, Outcome: "201", Method: "POST", AmountCents: 3980},
		{At: t0.Add(50 * time.Millisecond), OrderID: "rej", Outcome: "402", Method: "POST", AmountCents: 3980},
	}
	f.Payment.GetRequests.Log = []payLog{{At: t0.Add(400 * time.Millisecond), OrderID: evil, Outcome: "200", Method: "GET"}}
	f.Payment.GetRequests.Total = 1
	f.Inventory.Log = []invLog{
		{At: t0, Method: "POST", Op: "reserve", OrderID: evil, Outcome: "201"},
		{At: t0, Method: "POST", Op: "reserve", OrderID: "rej", Outcome: "201"},
	}
	for _, s := range rs.orders {
		s.At = t0
		if s.ID == evil {
			s.Status = "<b>pending</b>"
		}
	}
	rs.orders = append(rs.orders, &sent{N: 4, At: t0, Err: "раннер не дождался ответа", Latency: 5 * time.Second})

	var res scenarioResult
	def := scenarioByID("S5")
	res.ID, res.Title = def.ID, def.Title
	evaluate(&res, def, &rs.params.Scenarios.S5, rs)
	if res.Pass {
		t.Fatal("прогон с двойным списанием прошёл")
	}
	o := newOutput(&strings.Builder{})
	printResult(o, res)
	return &report{Task: "1", RunAt: t0, Seconds: 42, Passed: 0, Total: 1, Scenarios: []scenarioResult{res}}, o
}

func TestReportHTML(t *testing.T) {
	rep, o := hostileRun(t)
	path := filepath.Join(t.TempDir(), "task_1.json")
	writeResults(path, map[string]any{"task": "1", "passed": 0, "total": 1, "scenarios": rep.Scenarios, "level": ""}, o, rep)
	b, err := os.ReadFile(filepath.Join(filepath.Dir(path), "task_1.html"))
	if err != nil {
		t.Fatal(err)
	}
	page := string(b)
	if strings.Contains(page, "<script") || strings.Contains(page, "<b>pending") {
		t.Fatalf("данные Orders не экранированы:\n%s", page)
	}
	for _, want := range []string{
		"&lt;script&gt;alert(1)&lt;/script&gt;", // id заказа
		"Отчёт раннера: задание 1", "Прошло 0 из 1", "S5 · Payment выполняет операцию и теряет ответ",
		"Проверки", "ни одного заказа с двумя списаниями", "Журнал фактов",
		"Время ответа на POST /orders", "предел 1000 мс", `class="bar over"`, `class="budget"`,
		"Проблемные заказы", "Orders → Payment", "Orders → Inventory", "201 · резерв",
		"без ответа; соединение закрыто через 1,9 с", "ответ потерян после списания", "201 · списано",
		"GET /payments", "итог: paid", "Вывод раннера целиком",
	} {
		if !strings.Contains(page, want) {
			t.Fatalf("в отчёте нет %q", want)
		}
	}
	// Никаких внешних ресурсов и скриптов.
	for _, bad := range []string{"http://", "https://", "<script", "src=", "@import"} {
		if strings.Contains(page, bad) {
			t.Fatalf("в отчёте %q", bad)
		}
	}
	txt, _ := os.ReadFile(filepath.Join(filepath.Dir(path), "task_1.txt"))
	js, _ := os.ReadFile(path)
	if !strings.Contains(string(js), `"payment_get_requests"`) {
		t.Fatalf("в results JSON нет сводки сценария:\n%s", js)
	}
	for name, doc := range map[string]string{"html": page, "txt": string(txt), "json": string(js)} {
		low := strings.ToLower(doc)
		for _, w := range append(forbidden, "key", "размыкател") {
			if strings.Contains(low, w) {
				t.Fatalf("подсказка %q в %s", w, name)
			}
		}
	}
}

func TestReportStandError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "r.json")
	o := newOutput(&strings.Builder{})
	o.println("СТЕНД: <сломалось>")
	writeResults(path, map[string]any{"task": "1"}, o, &report{Task: "1", Error: "<сломалось>", Total: 5})
	b, err := os.ReadFile(filepath.Join(filepath.Dir(path), "r.html"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "Стенд: &lt;сломалось&gt;") {
		t.Fatalf("%s", b)
	}
}
