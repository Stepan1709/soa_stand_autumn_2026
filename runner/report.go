package main

// HTML-отчёт прогона: итог, сценарии, проверки, журнал фактов, истории
// проблемных заказов и распределение времён ответа POST /orders.
// Страница самодостаточная: без внешних ресурсов и без скриптов. Всё, что
// пришло от Orders (id, статусы, тела), выводится через html/template и
// экранируется.

import (
	"fmt"
	"html/template"
	"io"
	"math"
	"sort"
	"strings"
	"time"
	_ "time/tzdata" // Europe/Moscow в контейнере без базы зон
)

type report struct {
	Task      string
	RunAt     time.Time
	Seconds   float64
	Passed    int
	Total     int
	Level     string
	Error     string
	Scenarios []scenarioResult
	Output    string // вывод раннера целиком
}

// scenarioView — то, что нужно отчёту сверх JSON-результатов.
type scenarioView struct {
	Latency    latencyChart
	Orders     []orderView
	MoreOrders int // проблемных заказов сверх показанных
}

type orderView struct {
	Label  string
	Final  string
	Checks []string // проваленные проверки, в которых есть этот заказ
	Steps  []stepView
}

type stepView struct {
	Rel     string // время от первого события заказа
	Who     string
	What    string
	Outcome string
	Tone    string // ok | warn | bad | ""
	at      time.Time
}

type latencyChart struct {
	W, H      int
	Bars      []latencyBar
	BudgetX   float64
	BudgetMS  int
	TopLabel  string
	Count     int
	NoResp    int
	OverCount int
	Median    string
	Max       string
}

type latencyBar struct {
	X, Y, W, H float64
	Over       bool
	Title      string
}

// View — данные отчёта для шаблона (поле view не экспортируется: не в JSON).
func (r scenarioResult) View() *scenarioView { return r.view }

// maxOrdersInReport — сколько проблемных заказов показывать на сценарий.
const maxOrdersInReport = 20

func buildView(res *scenarioResult, rs *runState) *scenarioView {
	v := &scenarioView{Latency: latencyView(rs)}
	if rs.bonus != nil {
		// S8: в проверках покупатели, а не заказы — истории только в журнале.
		return v
	}
	var ids []string
	checksOf := map[string][]string{}
	for _, c := range res.Checks {
		if c.OK {
			continue
		}
		for _, id := range c.Orders {
			if _, seen := checksOf[id]; !seen {
				ids = append(ids, id)
			}
			checksOf[id] = append(checksOf[id], c.Name)
		}
	}
	if len(ids) > maxOrdersInReport {
		v.MoreOrders = len(ids) - maxOrdersInReport
		ids = ids[:maxOrdersInReport]
	}
	for _, id := range ids {
		v.Orders = append(v.Orders, orderTimeline(rs, id, checksOf[id]))
	}
	return v
}

// orderTimeline — всё, что раннер и заглушки видели по заказу, по времени.
func orderTimeline(rs *runState, id string, checks []string) orderView {
	o := orderView{Label: cut(clean(id), 64), Checks: checks}
	if st, ok := rs.final[id]; ok {
		o.Final = clean(st)
	}
	var steps []stepView
	for _, s := range rs.orders {
		if s.ID == id || (s.ID == "" && s.label() == id) {
			tone := "ok"
			if s.Err != "" || s.Code >= 500 || s.Latency > time.Duration(rs.params.BudgetMS)*time.Millisecond {
				tone = "bad"
			} else if s.Invalid != "" {
				tone = "warn"
			}
			out := s.outcome()
			if s.Invalid != "" {
				out += "; " + s.Invalid
			}
			steps = append(steps, stepView{at: s.At, Who: "раннер → Orders", What: "POST /orders", Outcome: out, Tone: tone})
			break
		}
	}
	f := rs.facts
	for _, l := range f.Inventory.Log {
		if l.OrderID != id {
			continue
		}
		out, tone := invOutcome(l)
		path := "/reservations"
		if l.Method == "DELETE" {
			path = "/reservations/{order_id}"
		}
		steps = append(steps, stepView{at: l.At, Who: "Orders → Inventory", What: l.Method + " " + path, Outcome: out, Tone: tone})
	}
	for i, logs := range [][]payLog{f.Payment.Requests.Log, f.Payment.GetRequests.Log} {
		for _, l := range logs {
			if l.OrderID != id {
				continue
			}
			m := l.Method
			if m == "" {
				m = [...]string{"POST", "GET"}[i]
			}
			out, tone := payOutcome(l)
			steps = append(steps, stepView{at: l.At, Who: "Orders → Payment", What: m + " /payments", Outcome: out, Tone: tone})
		}
	}
	sort.SliceStable(steps, func(i, j int) bool { return steps[i].at.Before(steps[j].at) })
	var t0 time.Time
	for _, s := range steps {
		if !s.at.IsZero() && (t0.IsZero() || s.at.Before(t0)) {
			t0 = s.at
		}
	}
	for i := range steps {
		if !steps[i].at.IsZero() {
			steps[i].Rel = "+" + strings.Replace(fmt.Sprintf("%.3f с", steps[i].at.Sub(t0).Seconds()), ".", ",", 1)
		}
	}
	if o.Final != "" {
		tone := ""
		switch o.Final {
		case "paid":
			tone = "ok"
		case "rejected":
			tone = "warn"
		default:
			tone = "bad"
		}
		steps = append(steps, stepView{Rel: "в конце", Who: "раннер → Orders", What: "GET /orders/{id}", Outcome: "итог: " + o.Final, Tone: tone})
	}
	o.Steps = steps
	return o
}

func payOutcome(l payLog) (string, string) {
	var dur string
	if l.DoneAt != nil {
		dur = strings.Replace(fmt.Sprintf("%.1f с", l.DoneAt.Sub(l.At).Seconds()), ".", ",", 1)
	}
	switch l.Outcome {
	case "blackhole":
		switch {
		case dur != "" && l.ClientClosed:
			return "без ответа; Orders закрыл соединение через " + dur, "bad"
		case dur != "":
			return "без ответа; соединение закрыто через " + dur, "bad"
		}
		return "без ответа", "bad"
	case "lost":
		return "ответ потерян после списания", "bad"
	}
	out, tone := clean(l.Outcome), "warn"
	switch l.Outcome {
	case "201":
		out, tone = "201 · списано", "ok"
	case "200":
		tone = "ok"
	case "402":
		out = "402 · отказ банка"
	case "500":
		tone = "bad"
	}
	if l.Method == "POST" && l.AmountCents > 0 {
		out += fmt.Sprintf(" · сумма %d.%02d", l.AmountCents/100, l.AmountCents%100)
	}
	switch {
	case dur != "" && l.ClientClosed:
		out += " · Orders закрыл соединение через " + dur + ", не дождавшись ответа"
	case dur != "":
		out += " · ответ через " + dur
	}
	return out, tone
}

func invOutcome(l invLog) (string, string) {
	switch {
	case l.Op == "reserve" && l.Outcome == "201":
		return "201 · резерв", "ok"
	case l.Op == "reserve" && l.Outcome == "200":
		return "200 · резерв уже есть", "ok"
	case l.Op == "release":
		return clean(l.Outcome) + " · снятие резерва", "ok"
	case l.Outcome == "200":
		return "200", "ok"
	}
	return clean(l.Outcome), "warn"
}

// latencyView — гистограмма времён ответа на POST /orders с линией бюджета.
func latencyView(rs *runState) latencyChart {
	const w, h, bins = 640, 120, 32
	budget := time.Duration(rs.params.BudgetMS) * time.Millisecond
	c := latencyChart{W: w, H: h, BudgetMS: rs.params.BudgetMS}
	var lats []time.Duration
	for _, s := range rs.orders {
		c.Count++
		if s.Err != "" {
			c.NoResp++
		} else {
			lats = append(lats, s.Latency)
		}
		if s.Err != "" || s.Latency > budget {
			c.OverCount++
		}
	}
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	top := budget * 3 / 2
	if len(lats) > 0 {
		c.Median, c.Max = secs(lats[len(lats)/2]), secs(lats[len(lats)-1])
		if m := lats[len(lats)-1] * 21 / 20; m > top {
			top = m
		}
	}
	c.TopLabel = secs(top)
	counts := make([]int, bins)
	for _, l := range lats {
		i := int(float64(l) / float64(top) * bins)
		counts[min(i, bins-1)]++
	}
	peak := 1
	for _, n := range counts {
		peak = max(peak, n)
	}
	bw := float64(w) / bins
	for i, n := range counts {
		if n == 0 {
			continue
		}
		bh := math.Max(2, float64(n)/float64(peak)*float64(h-16))
		lo := time.Duration(float64(top) * float64(i) / bins)
		hi := time.Duration(float64(top) * float64(i+1) / bins)
		c.Bars = append(c.Bars, latencyBar{
			X: float64(i)*bw + 1, Y: float64(h) - bh, W: bw - 2, H: bh,
			Over:  hi > budget,
			Title: fmt.Sprintf("%s – %s: %d", secs(lo), secs(hi), n),
		})
	}
	c.BudgetX = float64(budget) / float64(top) * float64(w)
	return c
}

// moscow — зона времени в отчёте: у раннера в контейнере зоны нет, срок
// сдачи задан по Москве.
var moscow = func() *time.Location {
	if l, err := time.LoadLocation("Europe/Moscow"); err == nil {
		return l
	}
	return time.FixedZone("MSK", 3*3600)
}()

// when — момент прогона по Москве.
func when(t time.Time) string { return t.In(moscow).Format("02.01.2006 15:04:05") + " МСК" }

// durRU — длительность по-русски: «3 мин 52 с», «1 ч 2 мин», «45 с».
func durRU(s float64) string {
	d := time.Duration(s * float64(time.Second)).Round(time.Second)
	h, m, sec := int(d.Hours()), int(d.Minutes())%60, int(d.Seconds())%60
	var parts []string
	if h > 0 {
		parts = append(parts, fmt.Sprintf("%d ч", h))
	}
	if m > 0 {
		parts = append(parts, fmt.Sprintf("%d мин", m))
	}
	if sec > 0 || len(parts) == 0 {
		parts = append(parts, fmt.Sprintf("%d с", sec))
	}
	return strings.Join(parts, " ")
}

var reportTmpl = template.Must(template.New("report").Funcs(template.FuncMap{
	"verdict": verdict,
	"when":    when,
	"dur":     durRU,
}).Parse(reportHTML))

func renderReport(w io.Writer, rep *report) error {
	return reportTmpl.Execute(w, rep)
}

const reportHTML = `<!doctype html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src 'unsafe-inline'; img-src data:">
<title>Отчёт раннера: задание {{.Task}}</title>
<style>
:root { --bg:#f6f7f9; --panel:#fff; --text:#1d2330; --muted:#667085; --line:#e3e6eb; --accent:#2f6fdb;
  --ok:#1e7d4f; --ok-bg:#e5f4ec; --warn:#9a6200; --warn-bg:#fdf1dc; --bad:#b42318; --bad-bg:#fde8e6;
  --mono: ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace; color-scheme: light; }
@media (prefers-color-scheme: dark) { :root { --bg:#111418; --panel:#1a1e24; --text:#e6e9ee; --muted:#9aa3b2;
  --line:#2b313a; --accent:#6ea1ff; --ok:#5fcf95; --ok-bg:#173226; --warn:#f0b64e; --warn-bg:#3a2c12;
  --bad:#ff7b6e; --bad-bg:#3d1a17; color-scheme: dark; } }
* { box-sizing: border-box; }
body { margin:0; background:var(--bg); color:var(--text);
  font:14px/1.45 system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif; }
.wrap { max-width:1100px; margin:0 auto; padding:16px; }
h1 { font-size:20px; margin:0 0 4px; } h2 { font-size:17px; margin:0; } h3 { font-size:14px; margin:14px 0 6px; }
.muted { color:var(--muted); }
.panel { background:var(--panel); border:1px solid var(--line); border-radius:8px; padding:14px; margin:12px 0; min-width:0; }
.summary { display:flex; flex-wrap:wrap; gap:8px 20px; align-items:baseline; }
.big { font-size:22px; font-weight:600; }
.badge { display:inline-block; padding:1px 8px; border-radius:999px; font-size:12.5px; font-weight:600; }
.PASS, .ok { background:var(--ok-bg); color:var(--ok); } .FAIL, .bad { background:var(--bad-bg); color:var(--bad); }
.warn { background:var(--warn-bg); color:var(--warn); }
.chip { display:inline-block; padding:1px 7px; border-radius:999px; font-size:12.5px; background:var(--bg); }
.err { color:var(--bad); }
table { width:100%; border-collapse:collapse; font-size:13px; }
td, th { padding:4px 6px; border-bottom:1px solid var(--line); text-align:left; vertical-align:top; }
th { font-weight:500; color:var(--muted); font-size:12px; }
.mono, pre { font-family:var(--mono); font-size:12.5px; }
pre { white-space:pre-wrap; overflow-wrap:anywhere; margin:0; }
.scen-head { display:flex; gap:10px; align-items:baseline; flex-wrap:wrap; }
.checks td:first-child { width:52px; }
.journal { background:var(--bg); padding:8px 10px; border-radius:6px; }
details > summary { cursor:pointer; }
.order { border:1px solid var(--line); border-radius:6px; padding:8px 10px; margin:8px 0; }
.order .id { font-family:var(--mono); overflow-wrap:anywhere; }
.steps td.rel { font-family:var(--mono); color:var(--muted); white-space:nowrap; text-align:right; width:90px; }
.steps td.who { color:var(--muted); white-space:nowrap; width:150px; }
.steps td.what { font-family:var(--mono); white-space:nowrap; width:190px; }
.chart svg { width:100%; height:auto; display:block; }
.chart .bar { fill:var(--accent); } .chart .bar.over { fill:var(--bad); }
.chart .budget { stroke:var(--bad); stroke-width:1.5; stroke-dasharray:4 3; }
.chart .axis { stroke:var(--line); }
.chart text { fill:var(--muted); font-size:11px; }
.legend { font-size:12.5px; color:var(--muted); margin-top:4px; }
@media (max-width:700px) { .steps td.who { display:none; } .steps td.what { white-space:normal; width:auto; } }
</style>
</head>
<body><div class="wrap">
<h1>Отчёт раннера: задание {{.Task}}</h1>
<div class="muted">{{when .RunAt}}{{if .Seconds}} · прогон {{dur .Seconds}}{{end}}</div>

<div class="panel summary">
{{if .Error}}<span class="big err">Стенд: {{.Error}}</span>
{{else}}<span class="big">Прошло {{.Passed}} из {{.Total}}</span>{{if .Level}}<span>Уровень: <b>{{.Level}}</b></span>{{end}}
{{range .Scenarios}}<span><span class="badge {{verdict .Pass}}">{{verdict .Pass}}</span> {{.ID}}</span>{{end}}{{end}}
</div>

{{range .Scenarios}}
<section class="panel" id="{{.ID}}">
  <div class="scen-head"><span class="badge {{verdict .Pass}}">{{verdict .Pass}}</span><h2>{{.ID}} · {{.Title}}</h2>
  <span class="muted">{{printf "%.0f" .Seconds}} с</span></div>
  {{if .Error}}<p class="err">{{.Error}}</p>{{else}}
  <h3>Проверки</h3>
  <table class="checks">{{range .Checks}}
    <tr><td>{{if .OK}}<span class="badge ok">ok</span>{{else}}<span class="badge bad">FAIL</span>{{end}}</td>
    <td>{{.Name}}{{if .Detail}}<div class="muted">{{.Detail}}</div>{{end}}</td></tr>{{end}}
  </table>
  <h3>Журнал фактов</h3>
  <pre class="journal">{{range .Journal}}{{.}}
{{end}}</pre>
  {{with .View}}
  <h3>Время ответа на POST /orders</h3>
  <div class="chart">{{with .Latency}}
    <svg viewBox="0 0 {{.W}} {{.H}}" role="img" aria-label="Распределение времени ответа">
      <line class="axis" x1="0" y1="{{.H}}" x2="{{.W}}" y2="{{.H}}"></line>
      {{range .Bars}}<rect class="bar{{if .Over}} over{{end}}" x="{{.X}}" y="{{.Y}}" width="{{.W}}" height="{{.H}}"><title>{{.Title}}</title></rect>{{end}}
      <line class="budget" x1="{{.BudgetX}}" y1="0" x2="{{.BudgetX}}" y2="{{.H}}"></line>
      <text x="{{.BudgetX}}" y="11" dx="4">предел {{.BudgetMS}} мс</text>
    </svg>
    <div class="legend">0 … {{.TopLabel}}; ответов {{.Count}}{{if .Median}}, медиана {{.Median}}, максимум {{.Max}}{{end}}; дольше предела или без ответа: {{.OverCount}}{{if .NoResp}}, из них без ответа: {{.NoResp}}{{end}}</div>
  {{end}}</div>
  {{if .Orders}}
  <h3>Проблемные заказы</h3>
  {{range .Orders}}
  <details class="order">
    <summary><span class="id">{{.Label}}</span>{{if .Final}} · итог {{.Final}}{{end}}
      {{range .Checks}}<span class="chip bad">{{.}}</span> {{end}}</summary>
    <table class="steps">{{range .Steps}}
      <tr><td class="rel">{{.Rel}}</td><td class="who">{{.Who}}</td><td class="what">{{.What}}</td>
      <td><span class="chip {{.Tone}}">{{.Outcome}}</span></td></tr>{{end}}
    </table>
  </details>{{end}}
  {{if .MoreOrders}}<p class="muted">… и ещё {{.MoreOrders}}</p>{{end}}
  {{end}}
  {{end}}
  {{end}}
</section>
{{end}}

<details class="panel"><summary>Вывод раннера целиком</summary><pre>{{.Output}}</pre></details>
</div></body>
</html>
`
