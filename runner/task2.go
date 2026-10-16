package main

// Задание 2: шина событий (S6, S7) и доставка delivery.confirmed (S8).

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

// flowS7 — заказы при недоступной шине, затем ещё kill_orders заказов, пока
// Payment отвечает с задержкой: деньги за них уже списаны, а ответа Orders
// ещё не получил. В этот момент Orders убивают и через restart_s запускают
// снова; дальше шина и Payment работают, уходят orders_after заказов.
// Возвращает момент, когда Orders убили.
func (r *runner) flowS7(p *ScenarioParams, rnd *rand.Rand, pm PaymentModes, bus BusModes,
	setModes func(PaymentModes, BusModes) error, orders *[]*sent, mu *sync.Mutex) (time.Time, error) {
	r.flow(rnd, p.Orders, p.RatePerS, "", orders, mu)()

	slow := pm
	slow.LatencyMS = p.KillLatencyMS
	if err := setModes(slow, bus); err != nil {
		return time.Time{}, err
	}
	tSlow := time.Now()
	waitKill := r.flow(rnd, p.KillOrders, 10, "kill", orders, mu)
	// Перезапуск — когда все эти заказы ждут ответа Payment, но не позже
	// чем за секунду до первого ответа.
	deadline := tSlow.Add(time.Duration(p.KillLatencyMS)*time.Millisecond - time.Second)
	for time.Now().Before(deadline) {
		if f, err := r.facts(); err == nil && paymentsInFlight(f, tSlow) >= p.KillOrders {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	at, err := r.restartOrders(p.RestartS)
	waitKill()
	if err != nil {
		return at, err
	}
	if err := setModes(pm, BusModes{}); err != nil {
		return at, err
	}
	r.flow(rnd, p.OrdersAfter, p.AfterRatePerS, "after", orders, mu)()
	return at, nil
}

// paymentsInFlight — списания после since, ответа на которые Orders ещё не
// получил.
func paymentsInFlight(f *Facts, since time.Time) int {
	n := 0
	for _, l := range f.Payment.Requests.Log {
		if !l.At.Before(since) && l.Outcome == "201" && l.DoneAt == nil {
			n++
		}
	}
	return n
}

// runS8 — шина доставляет события delivery.confirmed; после части
// подтверждений Orders убивают и запускают снова; когда всё подтверждено,
// шина ещё раз присылает по копии каждого события. Затем балансы.
func (r *runner) runS8(def *scenarioDef, p *ScenarioParams, started time.Time) (*runState, error) {
	label := def.ID + " · " + def.Title
	r.setLabel(label)
	if err := r.admin(http.MethodPut, "/admin/modes", map[string]any{"seed": r.params.Seed, "label": label}); err != nil {
		return nil, fmt.Errorf("режимы заглушек: %w", err)
	}
	if err := r.admin(http.MethodPost, "/admin/bus/deliver", map[string]any{
		"events": p.Events, "users": p.Users, "max_copies": p.MaxCopies,
		"parallel_rate": p.ParallelRate, "rate_per_s": p.RatePerS,
	}); err != nil {
		return nil, fmt.Errorf("доставка событий: %w", err)
	}
	send := seconds(float64(p.Events) / p.RatePerS)
	r.waitDelivery(send+30*time.Second, func(f *Facts) bool {
		return f.Bus.Delivery != nil && f.Bus.Delivery.AckedEvents >= p.KillAfterEvents
	})
	killedAt, err := r.restartOrders(p.RestartS)
	if err != nil {
		return nil, err
	}
	allAcked := func(f *Facts) bool { return f.Bus.Delivery != nil && f.Bus.Delivery.Pending == 0 }
	r.waitDelivery(send+60*time.Second, allAcked)
	if err := r.admin(http.MethodPost, "/admin/bus/redeliver", nil); err != nil {
		return nil, fmt.Errorf("доставка копий событий: %w", err)
	}
	r.waitDelivery(send+30*time.Second, allAcked)
	time.Sleep(2 * time.Second)

	f, err := r.facts()
	if err != nil {
		return nil, fmt.Errorf("факты заглушек: %w", err)
	}
	rs := &runState{params: r.params, facts: f, prev: map[string]bool{}, killedAt: killedAt, started: started,
		final: map[string]string{}, amounts: map[string]int64{},
		bonus: map[string]bonusResult{}, unknownUser: "u-0"}
	users := []string{rs.unknownUser}
	if d := f.Bus.Delivery; d != nil {
		for u := range d.ExpectedByUser {
			users = append(users, u)
		}
	}
	for _, u := range users {
		rs.bonus[u] = r.getBonus(u)
	}
	rs.duration = time.Since(started)
	return rs, nil
}

// waitDelivery опрашивает факты, пока ok не станет true или не выйдет limit.
func (r *runner) waitDelivery(limit time.Duration, ok func(*Facts) bool) {
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if f, err := r.facts(); err == nil && ok(f) {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func (r *runner) getBonus(user string) bonusResult {
	code, b, err := r.do(http.MethodGet, r.orders+"/users/"+url.PathEscape(user)+"/bonus", nil, 3*time.Second)
	if err != nil {
		return bonusResult{Err: "ошибка GET: " + shortErr(err)}
	}
	if code != http.StatusOK {
		return bonusResult{Err: fmt.Sprintf("GET %d", code)}
	}
	var resp struct {
		Points *json.Number `json:"points"`
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	if err := dec.Decode(&resp); err != nil {
		return bonusResult{Err: "ответ не JSON"}
	}
	if resp.Points == nil {
		return bonusResult{Err: "в ответе нет points"}
	}
	n, err := resp.Points.Int64()
	if err != nil {
		return bonusResult{Err: "points не целое: " + trim(resp.Points.String(), 20)}
	}
	return bonusResult{Points: n}
}

// ---------- проверки ----------

// checksBus — S6, S7: события order.paid в шине.
func checksBus(_ *ScenarioParams, rs *runState) []check {
	bus := rs.facts.Bus
	var noEvent, extra, manyIDs []string
	for _, id := range rs.allIDs() {
		if rs.final[id] == "paid" && len(bus.EventsByOrder[id]) == 0 {
			noEvent = append(noEvent, id)
		}
	}
	ids := make([]string, 0, len(bus.EventsByOrder))
	for id := range bus.EventsByOrder {
		if !rs.prev[id] {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	for _, id := range ids {
		if rs.final[id] != "paid" {
			extra = append(extra, id)
		}
		if len(bus.EventsByOrder[id]) > 1 {
			manyIDs = append(manyIDs, id)
		}
	}
	return []check{
		newCheck("каждый paid заказ есть в шине", noEvent, fmt.Sprintf("paid без события order.paid: %d", len(noEvent))),
		newCheck("события order.paid — только для оплаченных заказов", extra,
			fmt.Sprintf("события по заказам, которые в итоге не paid: %d", len(extra))),
		newCheck("у всех копий события заказа один id", manyIDs,
			fmt.Sprintf("заказов, по которым в шине события с разными id: %d", len(manyIDs))),
	}
}

// checksS8 — доставка подтверждена, балансы сходятся.
func checksS8(_ *ScenarioParams, rs *runState) []check {
	d := rs.facts.Bus.Delivery
	if d == nil {
		return []check{{Name: "шина доставила события", OK: false, Detail: "доставки не было"}}
	}
	var unacked, wrong []string
	for _, e := range d.Events {
		if e.Acked < e.Copies {
			unacked = append(unacked, e.ID)
		}
	}
	users := sortedUsers(d.ExpectedByUser)
	for _, u := range users {
		if b := rs.bonus[u]; b.Err != "" || b.Points != int64(d.ExpectedByUser[u]) {
			wrong = append(wrong, u)
		}
	}
	var zero []string
	if b := rs.bonus[rs.unknownUser]; b.Err != "" || b.Points != 0 {
		zero = append(zero, rs.unknownUser)
	}
	return []check{
		newCheck("каждая доставка события подтверждена ответом 2xx", unacked,
			fmt.Sprintf("не подтверждено доставок: %d", d.Pending)),
		newCheck("баланс покупателя — сумма бонусов по разным событиям", wrong,
			fmt.Sprintf("не сходится у %d из %d покупателей", len(wrong), len(users))),
		newCheck("у покупателя без начислений баланс 0", zero, "GET /users/"+rs.unknownUser+"/bonus: "+bonusText(rs.bonus[rs.unknownUser])),
	}
}

func sortedUsers(m map[string]int) []string {
	users := make([]string, 0, len(m))
	for u := range m {
		users = append(users, u)
	}
	// u-2 раньше u-10.
	sort.Slice(users, func(i, j int) bool {
		if len(users[i]) != len(users[j]) {
			return len(users[i]) < len(users[j])
		}
		return users[i] < users[j]
	})
	return users
}

func bonusText(b bonusResult) string {
	if b.Err != "" {
		return b.Err
	}
	return fmt.Sprintf("%d", b.Points)
}

// ---------- журнал ----------

// busScenario — сценарий проверяет события в шине.
func busScenario(def *scenarioDef) bool { return def.ID == "S6" || def.ID == "S7" }

// journalBus — строки журнала про шину (S6, S7).
func journalBus(rs *runState, def *scenarioDef) []string {
	bus := rs.facts.Bus
	withEvent, paidNoEvent := 0, 0
	for _, id := range rs.allIDs() {
		if len(bus.EventsByOrder[id]) > 0 {
			withEvent++
		} else if rs.final[id] == "paid" {
			paidNoEvent++
		}
	}
	out := []string{fmt.Sprintf("шина: публикаций %d (%s), во время отказа %d; заказов с событием %d, paid без события %d",
		bus.Requests.Total, fmtCounts(bus.Requests.ByStatus), bus.Requests.InDown, withEvent, paidNoEvent)}
	if !rs.killedAt.IsZero() {
		p := rs.params.scenario(def.ID)
		out = append(out, fmt.Sprintf("Orders убит (kill -9) через %.0f с после начала сценария и запущен снова через %.0f с",
			rs.killedAt.Sub(rs.started).Seconds(), p.RestartS))
	}
	return out
}

// journalS8 — журнал S8: доставка и балансы.
func journalS8(rs *runState) []string {
	d := rs.facts.Bus.Delivery
	if d == nil {
		return []string{"доставки не было"}
	}
	copies := 0
	for _, e := range d.Events {
		copies += e.Copies
	}
	out := []string{
		fmt.Sprintf("шина: событий %d для %d покупателей, доставок %d (с копиями), попыток %d, не подтверждено %d",
			len(d.Events), len(d.ExpectedByUser), copies, d.Attempts, d.Pending),
	}
	if !rs.killedAt.IsZero() {
		out = append(out, fmt.Sprintf("Orders убит (kill -9) через %.0f с после начала сценария и запущен снова через %.0f с",
			rs.killedAt.Sub(rs.started).Seconds(), rs.params.scenario("S8").RestartS))
	}
	var parts []string
	for _, u := range sortedUsers(d.ExpectedByUser) {
		parts = append(parts, fmt.Sprintf("%s %s/%d", u, bonusText(rs.bonus[u]), d.ExpectedByUser[u]))
	}
	out = append(out, "балансы (получено/ожидалось): "+strings.Join(parts, ", "))
	return out
}

// userStory — S8: всё про покупателя одной строкой.
func userStory(rs *runState, user string) string {
	d := rs.facts.Bus.Delivery
	if d == nil {
		return ""
	}
	events, copies := 0, 0
	for _, e := range d.Events {
		if e.UserID == user {
			events++
			copies += e.Copies
		}
	}
	return fmt.Sprintf("баланс %s, ожидалось %d; событий %d, доставок с копиями %d",
		bonusText(rs.bonus[user]), d.ExpectedByUser[user], events, copies)
}

// keyFactsS8 — сводка S8 числами.
func keyFactsS8(rs *runState) map[string]int {
	m := map[string]int{}
	d := rs.facts.Bus.Delivery
	if d == nil {
		return m
	}
	m["bus_events"] = len(d.Events)
	m["bus_delivery_attempts"] = d.Attempts
	m["bus_delivery_pending"] = d.Pending
	for _, e := range d.Events {
		m["bus_deliveries"] += e.Copies
	}
	for u, want := range d.ExpectedByUser {
		if b := rs.bonus[u]; b.Err != "" || b.Points != int64(want) {
			m["bonus_wrong_users"]++
		}
	}
	return m
}
