package main

import (
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode"
)

type check struct {
	Name   string   `json:"name"`
	OK     bool     `json:"ok"`
	Detail string   `json:"detail,omitempty"`
	Orders []string `json:"orders,omitempty"` // заказы, на которых провал
	// kind помечает проверки, из которых складывается уровень «засчитано».
	kind string
}

const (
	kindDouble = "double"
	kindBudget = "budget"
)

func newCheck(name string, bad []string, detail string) check {
	c := check{Name: name, OK: len(bad) == 0, Orders: bad}
	if !c.OK {
		c.Detail = detail
	}
	return c
}

func commonChecks(p *Params, rs *runState) []check {
	f := rs.facts
	charges := f.Payment.Charges.ByOrder
	resv := f.Inventory.Reservations.ByOrder
	budget := time.Duration(p.BudgetMS) * time.Millisecond

	var (
		c5xx, slow, invalid           []string
		double, rejMoney, moneyNoPaid []string
		paidBad, rejHeld, pending     []string
		wrongAmount                   []string
		maxLat                        time.Duration
	)
	expected := map[string]int64{}
	for _, s := range rs.orders {
		if s.ID != "" {
			expected[s.ID] = s.expected()
		}
	}
	for _, s := range rs.orders {
		label := s.label()
		// Заказы, которые были в работе, когда Orders убили (S7): ответа на
		// них и не могло быть; их деньги и итог проверяются ниже, как у всех.
		if s.Phase == "kill" {
			continue
		}
		if s.Code >= 500 {
			c5xx = append(c5xx, label)
		}
		if s.Err != "" || s.Latency > budget {
			slow = append(slow, label)
		}
		if s.Latency > maxLat {
			maxLat = s.Latency
		}
		if s.Invalid != "" {
			invalid = append(invalid, label)
		}
		if s.ID != "" {
			switch st := rs.final[s.ID]; st {
			case "paid", "rejected", "pending":
			default:
				invalid = append(invalid, label)
			}
		}
	}
	for _, id := range rs.allIDs() {
		st := rs.final[id]
		n := charges[id]
		if n >= 2 {
			double = append(double, id)
		}
		if n > 0 && st == "rejected" {
			rejMoney = append(rejMoney, id)
		}
		if n > 0 && st != "paid" {
			moneyNoPaid = append(moneyNoPaid, id)
		}
		if st == "paid" && (n == 0 || resv[id] != "held") {
			paidBad = append(paidBad, id)
		}
		if st == "rejected" && resv[id] == "held" {
			rejHeld = append(rejHeld, id)
		}
		if st == "pending" {
			pending = append(pending, id)
		}
		// Сумма — по заказам с одним списанием: два списания ловит своя проверка.
		if n == 1 {
			got := f.Payment.Charges.AmountByOrder[id]
			bad := false
			switch st {
			case "paid", "rejected", "pending":
				bad = rs.amounts[id] != got
			}
			if want, ok := expected[id]; ok && want != got {
				bad = true
			}
			if bad {
				wrongAmount = append(wrongAmount, id)
			}
		}
	}

	slowC := newCheck(fmt.Sprintf("каждый ответ на POST /orders не дольше %d мс", p.BudgetMS), slow,
		fmt.Sprintf("%d из %d ответов дольше %d мс или без ответа, максимум %s", len(slow), rs.answerable(), p.BudgetMS, secs(maxLat)))
	slowC.kind = kindBudget
	doubleC := newCheck("ни одного заказа с двумя списаниями", double,
		fmt.Sprintf("заказов с 2+ списаниями: %d", len(double)))
	doubleC.kind = kindDouble

	return []check{
		newCheck("ни одного 5xx на POST /orders", c5xx, fmt.Sprintf("ответов 5xx: %d из %d", len(c5xx), rs.answerable())),
		slowC,
		newCheck("ответы Orders по контракту", invalid, fmt.Sprintf("нарушений: %d", len(invalid))),
		doubleC,
		newCheck("ни одного rejected, за который списаны деньги", rejMoney, fmt.Sprintf("таких заказов: %d", len(rejMoney))),
		newCheck("каждый заказ, за который списаны деньги, в итоге paid", moneyNoPaid, fmt.Sprintf("таких заказов: %d", len(moneyNoPaid))),
		newCheck("paid: деньги списаны и товар зарезервирован", paidBad, fmt.Sprintf("paid без списания или без резерва: %d", len(paidBad))),
		newCheck("списана сумма заказа", wrongAmount,
			fmt.Sprintf("списано не столько, сколько стоит заказ (Σ qty × price_cents) или сколько в amount_cents: %d", len(wrongAmount))),
		newCheck("rejected: резерв снят", rejHeld, fmt.Sprintf("rejected с действующим резервом: %d", len(rejHeld))),
		newCheck("в конце прогона нет pending", pending, fmt.Sprintf("заказов в pending: %d", len(pending))),
	}
}

func checksS1(_ *ScenarioParams, rs *runState) []check {
	f := rs.facts
	var bad, rejNoDecline []string
	for _, s := range rs.orders {
		if s.ID == "" {
			bad = append(bad, s.label())
			continue
		}
		st, n, rv := rs.final[s.ID], f.Payment.Charges.ByOrder[s.ID], f.Inventory.Reservations.ByOrder[s.ID]
		switch {
		case st == "paid" && n == 1:
		case st == "rejected" && n == 0 && rv != "held":
		default:
			bad = append(bad, s.ID)
		}
		if st == "rejected" && f.Payment.Declines.ByOrder[s.ID] == 0 {
			rejNoDecline = append(rejNoDecline, s.ID)
		}
	}
	return []check{
		newCheck("каждый заказ paid с одним списанием или rejected без списания и со снятым резервом", bad,
			fmt.Sprintf("не так: %d из %d", len(bad), len(rs.orders))),
		newCheck("rejected — только когда банк отклонил оплату", rejNoDecline,
			fmt.Sprintf("rejected без отказа банка: %d", len(rejNoDecline))),
	}
}

// checksS2 — доля оплаченных. Заказы, которые банк отклонил (402, rejected,
// денег нет), в расчёт не входят.
func checksS2(p *ScenarioParams, rs *runState) []check {
	paid, total := 0, 0
	var notPaid []string
	for _, s := range rs.orders {
		if s.ID != "" && rs.legitDecline(s.ID) {
			continue
		}
		total++
		if s.ID != "" && rs.final[s.ID] == "paid" {
			paid++
		} else {
			notPaid = append(notPaid, s.label())
		}
	}
	need := p.MinPaid * float64(total)
	c := check{
		Name: fmt.Sprintf("в итоге оплачено не меньше %.0f %% заказов", p.MinPaid*100),
		OK:   float64(paid) >= need-1e-9,
	}
	if !c.OK {
		c.Orders = notPaid
	}
	c.Detail = fmt.Sprintf("оплачено %d из %d (%.0f %%)", paid, total, pct(paid, total))
	if skipped := len(rs.orders) - total; skipped > 0 {
		c.Detail += fmt.Sprintf("; не считаются %d, отклонённые банком", skipped)
	}
	return []check{c}
}

func checksS4(p *ScenarioParams, rs *runState) []check {
	down := rs.downRequests()
	c1 := check{
		Name:   fmt.Sprintf("за время отказа Payment получил не больше %d запросов", p.MaxRequestsDown),
		OK:     down <= p.MaxRequestsDown,
		Detail: fmt.Sprintf("запросов к Payment за время отказа: %d", down),
	}
	var bad []string
	after := 0
	for _, s := range rs.orders {
		if s.Phase != "after" {
			continue
		}
		if s.ID != "" && rs.legitDecline(s.ID) {
			continue
		}
		after++
		if s.ID == "" || rs.final[s.ID] != "paid" {
			bad = append(bad, s.label())
		}
	}
	return []check{c1, newCheck("заказы после подъёма Payment оплачены", bad,
		fmt.Sprintf("не оплачено: %d из %d", len(bad), after))}
}

// ---------- печать ----------

func (s *sent) label() string {
	if s.ID != "" {
		return s.ID
	}
	return fmt.Sprintf("заказ №%d (id неизвестен)", s.N)
}

func (s *sent) outcome() string {
	switch {
	case s.Err != "":
		return fmt.Sprintf("без ответа (%s) за %s", s.Err, secs(s.Latency))
	case s.Code == 201:
		return fmt.Sprintf("201 %s за %s", clean(s.Status), secs(s.Latency))
	default:
		return fmt.Sprintf("%d за %s", s.Code, secs(s.Latency))
	}
}

func journal(rs *runState, def *scenarioDef) []string {
	f := rs.facts
	budget := time.Duration(rs.params.BudgetMS) * time.Millisecond
	var (
		n201, n5xx, nErr, nSlow, nOther int
		lats                            []time.Duration
	)
	for _, s := range rs.orders {
		switch {
		case s.Err != "":
			nErr++
		case s.Code == 201:
			n201++
		case s.Code >= 500:
			n5xx++
		default:
			nOther++
		}
		if s.Err == "" {
			lats = append(lats, s.Latency)
		}
		if s.Err != "" || s.Latency > budget {
			nSlow++
		}
	}
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	var p50, pmax time.Duration
	if len(lats) > 0 {
		p50, pmax = lats[len(lats)/2], lats[len(lats)-1]
	}
	counts := map[string]int{}
	for _, id := range rs.knownIDs() {
		counts[rs.final[id]]++
	}
	known := map[string]bool{}
	for _, id := range rs.knownIDs() {
		known[id] = true
	}
	charged, notFound, unknownCharged, double := 0, 0, 0, 0
	for id, n := range f.Payment.Charges.ByOrder {
		if rs.prev[id] {
			continue
		}
		charged++
		switch {
		case rs.final[id] == "404":
			notFound++
		case !known[id]:
			unknownCharged++
		}
		if n >= 2 {
			double++
		}
	}

	out := []string{
		fmt.Sprintf("POST /orders: отправлено %d; 201: %d, 5xx: %d, другие коды: %d, без ответа: %d; дольше %d мс: %d; медиана %s, максимум %s",
			len(rs.orders), n201, n5xx, nOther, nErr, rs.params.BudgetMS, nSlow, secs(p50), secs(pmax)),
		"итог GET /orders/{id}: " + fmtCounts(counts),
		fmt.Sprintf("Payment: запросов на оплату %d (%s); списаний %d по %d заказам, заказов с 2+ списаниями: %d",
			f.Payment.Requests.Total, fmtCounts(f.Payment.Requests.ByStatus),
			f.Payment.Charges.Total, charged, double),
	}
	if def.ID == "S4" {
		out = append(out, fmt.Sprintf("запросов к Payment за время отказа: %d", rs.downRequests()))
	}
	if busScenario(def) {
		out = append(out, journalBus(rs, def)...)
	}
	if notFound > 0 {
		out = append(out, fmt.Sprintf("списания по заказам, которых Orders не знает (GET 404): %d", notFound))
	}
	if unknownCharged > 0 {
		out = append(out, fmt.Sprintf("списания по заказам, которые Orders знает, но чей id раннер не получил в ответе на POST: %d", unknownCharged))
	}
	if n := rs.prevRequests(); n > 0 {
		out = append(out, fmt.Sprintf("запросы к Payment по заказам прошлых сценариев: %d", n))
	}
	out = append(out, fmt.Sprintf("Inventory: резервов %d (держатся %d, сняты %d)",
		len(f.Inventory.Reservations.ByOrder), f.Inventory.Reservations.Held, f.Inventory.Reservations.Released))
	return out
}

// orderStory — всё, что известно об одном заказе, одной строкой.
func orderStory(rs *runState, id string) string {
	f := rs.facts
	var parts []string
	for _, s := range rs.orders {
		if s.ID == id || s.label() == id {
			parts = append(parts, "POST "+s.outcome())
			if s.Invalid != "" {
				parts = append(parts, s.Invalid)
			}
			break
		}
	}
	if st, ok := rs.final[id]; ok {
		parts = append(parts, "итог "+st)
	}
	var outs []string
	for _, l := range f.Payment.Requests.Log {
		if l.OrderID == id {
			outs = append(outs, l.Outcome)
		}
	}
	if len(outs) > 12 {
		outs = append(outs[:12:12], fmt.Sprintf("… всего %d", len(outs)))
	}
	if len(outs) > 0 {
		parts = append(parts, fmt.Sprintf("запросы к Payment: %s", strings.Join(outs, ", ")))
	}
	parts = append(parts, fmt.Sprintf("списаний %d", f.Payment.Charges.ByOrder[id]))
	if rv := f.Inventory.Reservations.ByOrder[id]; rv != "" {
		parts = append(parts, "резерв "+rv)
	}
	if rs.facts.Bus.Requests.Total > 0 {
		parts = append(parts, fmt.Sprintf("событий в шине %d", len(f.Bus.EventsByOrder[id])))
	}
	return strings.Join(parts, "; ")
}

func fmtCounts(m map[string]int) string {
	if len(m) == 0 {
		return "нет"
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		name := k
		if name == "" {
			name = "?"
		}
		parts = append(parts, fmt.Sprintf("%s: %d", name, m[k]))
	}
	return strings.Join(parts, ", ")
}

func secs(d time.Duration) string { return fmt.Sprintf("%.2f с", d.Seconds()) }

func pct(a, b int) float64 {
	if b == 0 {
		return 0
	}
	return 100 * float64(a) / float64(b)
}

// clean заменяет управляющие символы пробелом, а обратные кавычки и
// невидимые символы форматирования — «?»: чужой текст в выводе не должен
// становиться командой CI («::error::» с новой строки) или разметкой.
func clean(s string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r == '`' || r == '\uFFFD' || unicode.In(r, unicode.Cf, unicode.Zl, unicode.Zp):
			return '?'
		case r < 0x20 || r == 0x7f || unicode.IsControl(r):
			return ' '
		}
		return r
	}, s)
}

// trim — чужой текст для вывода: очищенный и не длиннее n символов.
func trim(s string, n int) string {
	s = strings.TrimSpace(clean(s))
	if len([]rune(s)) > n {
		return string([]rune(s)[:n]) + "…"
	}
	return s
}

func shortErr(err error) string {
	s := err.Error()
	switch {
	case strings.Contains(s, "Client.Timeout") || strings.Contains(s, "deadline exceeded"):
		return "раннер не дождался ответа"
	case strings.Contains(s, "EOF"):
		return "соединение закрыто"
	case strings.Contains(s, "connection refused"):
		return "соединение отклонено"
	case strings.Contains(s, "connection reset"):
		return "соединение сброшено"
	}
	return trim(s, 60)
}

// keyFacts — сводка прогона числами.
func keyFacts(rs *runState) map[string]int {
	f := rs.facts
	budget := time.Duration(rs.params.BudgetMS) * time.Millisecond
	m := map[string]int{
		"orders_sent":               len(rs.orders),
		"payment_requests":          f.Payment.Requests.Total,
		"payment_requests_down":     f.Payment.Requests.InBlackhole,
		"payment_get_requests":      f.Payment.GetRequests.Total,
		"payment_get_requests_down": f.Payment.GetRequests.InBlackhole,
		"payment_load_down":         rs.downRequests(),
		"payment_requests_prev":     rs.prevRequests(),
		"charges":                   f.Payment.Charges.Total,
		"charged_orders":            len(f.Payment.Charges.ByOrder),
		"declines":                  f.Payment.Declines.Total,
	}
	if b := f.Bus; b.Requests.Total > 0 {
		m["bus_publish"] = b.Requests.Total
		m["bus_publish_down"] = b.Requests.InDown
		m["bus_accepted"] = b.Accepted
		for id := range b.EventsByOrder {
			if !rs.prev[id] {
				m["bus_orders_with_event"]++
			}
		}
	}
	for _, s := range rs.orders {
		switch {
		case s.Err != "":
			m["post_no_response"]++
		case s.Code == 201:
			m["post_201"]++
		case s.Code >= 500:
			m["post_5xx"]++
		default:
			m["post_other"]++
		}
		if s.Err != "" || s.Latency > budget {
			m["post_over_budget"]++
		}
	}
	for _, id := range rs.knownIDs() {
		switch rs.final[id] {
		case "paid", "rejected", "pending":
			m["final_"+rs.final[id]]++
		default:
			m["final_other"]++
		}
	}
	for id, n := range f.Payment.Charges.ByOrder {
		if rs.prev[id] {
			continue
		}
		if n >= 2 {
			m["double_charged_orders"]++
		}
		if rs.final[id] != "paid" {
			m["charged_not_paid"]++
		}
		if rs.final[id] == "404" {
			m["charged_unknown_404"]++
		}
	}
	return m
}

// answerable — заказы, на которые Orders мог ответить (без убитых посреди
// работы в S7).
func (rs *runState) answerable() int {
	n := 0
	for _, s := range rs.orders {
		if s.Phase != "kill" {
			n++
		}
	}
	return n
}
