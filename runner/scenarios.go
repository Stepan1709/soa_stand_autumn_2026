package main

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/url"
	"sort"
	"sync"
	"time"
)

type scenarioDef struct {
	ID    string
	Title string
	// extra — проверки, специфичные для сценария (общие — в commonChecks).
	extra func(p *ScenarioParams, rs *runState) []check
	// noOrders — сценарий без заказов (S8): общих проверок заказов нет.
	noOrders bool
}

var scenarioDefs = []scenarioDef{
	{ID: "S1", Title: "Payment работает, часть оплат банк отклоняет (402)", extra: checksS1},
	{ID: "S2", Title: "Payment отвечает 500 на часть запросов", extra: checksS2},
	{ID: "S3", Title: "Payment выполняет операцию, но отвечает медленно", extra: nil},
	{ID: "S4", Title: "Payment не отвечает, потом поднимается", extra: checksS4},
	{ID: "S5", Title: "Payment выполняет операцию и теряет ответ", extra: nil},
	{ID: "S6", Title: "Шина событий недоступна, потом поднимается", extra: checksBus},
	{ID: "S7", Title: "Orders убивают посреди работы и запускают снова", extra: checksBus},
	{ID: "S8", Title: "Доставка подтверждена: события приходят несколько раз", extra: checksS8, noOrders: true},
}

func scenarioByID(id string) *scenarioDef {
	for i := range scenarioDefs {
		if scenarioDefs[i].ID == id {
			return &scenarioDefs[i]
		}
	}
	return nil
}

// ---------- заказы ----------

type item struct {
	SKU        string `json:"sku"`
	Qty        int    `json:"qty"`
	PriceCents int64  `json:"price_cents"`
}

type orderReq struct {
	UserID string `json:"user_id"`
	Items  []item `json:"items"`
}

// sent — один POST /orders глазами раннера.
type sent struct {
	N       int
	At      time.Time // когда раннер отправил запрос
	Phase   string    // "" | "down" | "after" | "kill" (S7: Orders убит, пока заказ в работе)
	Req     orderReq
	Latency time.Duration
	Code    int
	Err     string // сетевая ошибка или раннер не дождался ответа
	Invalid string // ответ не по контракту
	ID      string
	Status  string // статус из ответа на POST
}

var prices = []int64{1990, 9900, 49900, 129900}

func genOrder(rnd *rand.Rand) orderReq {
	n := 1 + rnd.IntN(3)
	o := orderReq{UserID: fmt.Sprintf("u-%d", 1+rnd.IntN(20))}
	used := map[int]bool{}
	for len(o.Items) < n {
		s := 1 + rnd.IntN(10)
		if used[s] {
			continue
		}
		used[s] = true
		o.Items = append(o.Items, item{
			SKU:        fmt.Sprintf("sku-%d", s),
			Qty:        1 + rnd.IntN(3),
			PriceCents: prices[rnd.IntN(len(prices))],
		})
	}
	return o
}

func (r *runner) postOrder(s *sent) {
	start := time.Now()
	s.At = start
	code, b, err := r.do(http.MethodPost, r.orders+"/orders", s.Req,
		time.Duration(r.params.RequestTimeoutMS)*time.Millisecond)
	s.Latency = time.Since(start)
	if err != nil {
		s.Err = shortErr(err)
		return
	}
	s.Code = code
	if code != http.StatusCreated {
		if code < 500 {
			s.Invalid = fmt.Sprintf("код %d, ожидался 201: %s", code, trim(string(b), 80))
		}
		return
	}
	// id и статус приходят от Orders: дальше они только сравниваются, а в
	// вывод попадают через short/clean.
	var resp struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}
	if err := json.Unmarshal(b, &resp); err != nil || resp.ID == "" {
		s.Invalid = "в ответе 201 нет id: " + trim(string(b), 80)
		return
	}
	s.ID = resp.ID
	s.Status = resp.Status
	switch resp.Status {
	case "paid", "rejected", "pending":
	default:
		s.Status = trim(resp.Status, 20)
		s.Invalid = fmt.Sprintf("неизвестный статус «%s»", s.Status)
	}
}

// flow отправляет n заказов равномерно с темпом rate в секунду, не дожидаясь
// ответов на предыдущие. Возвращает функцию ожидания всех ответов.
func (r *runner) flow(rnd *rand.Rand, n int, rate float64, phase string, out *[]*sent, mu *sync.Mutex) func() {
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < n; i++ {
		at := start.Add(time.Duration(float64(i) / rate * float64(time.Second)))
		time.Sleep(time.Until(at))
		mu.Lock()
		s := &sent{N: len(*out) + 1, Phase: phase, Req: genOrder(rnd)}
		*out = append(*out, s)
		mu.Unlock()
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.postOrder(s)
		}()
	}
	return wg.Wait
}

// ---------- состояние после прогона ----------

type runState struct {
	params *Params
	orders []*sent
	facts  *Facts
	final  map[string]string // order_id → paid|rejected|pending|404|ошибка
	// amounts — amount_cents из GET /orders/{id}.
	amounts map[string]int64
	// prev — id заказов прошлых сценариев этого прогона: в проверках текущего
	// сценария не участвуют.
	prev     map[string]bool
	duration time.Duration
	// started — начало сценария; killedAt — когда Orders убили (S7, S8),
	// нулевое — не убивали.
	started  time.Time
	killedAt time.Time
	// bonus — S8: ответы GET /users/{id}/bonus по покупателям; unknownUser —
	// покупатель без начислений.
	bonus       map[string]bonusResult
	unknownUser string
}

// bonusResult — ответ GET /users/{id}/bonus: points или ошибка.
type bonusResult struct {
	Points int64
	Err    string
}

// expected — сумма заказа по запросу раннера: Σ qty × price_cents.
func (s *sent) expected() int64 {
	var sum int64
	for _, it := range s.Req.Items {
		sum += int64(it.Qty) * it.PriceCents
	}
	return sum
}

// legitDecline — банк отклонил оплату, Orders честно ответил rejected и
// денег не списано: такой заказ и не мог быть оплачен.
func (rs *runState) legitDecline(id string) bool {
	p := rs.facts.Payment
	return p.Declines.ByOrder[id] > 0 && rs.final[id] == "rejected" && p.Charges.ByOrder[id] == 0
}

// downRequests — запросы к Payment (оплата и поиск списаний) за время
// отказа по заказам текущего сценария.
func (rs *runState) downRequests() int {
	n := 0
	p := rs.facts.Payment
	for _, logs := range [][]payLog{p.Requests.Log, p.GetRequests.Log} {
		for _, l := range logs {
			if l.Blackhole && !rs.prev[l.OrderID] {
				n++
			}
		}
	}
	return n
}

// prevRequests — запросы к Payment по заказам прошлых сценариев.
func (rs *runState) prevRequests() int {
	n := 0
	p := rs.facts.Payment
	for _, logs := range [][]payLog{p.Requests.Log, p.GetRequests.Log} {
		for _, l := range logs {
			if rs.prev[l.OrderID] {
				n++
			}
		}
	}
	return n
}

// display — заказ для журнала: подпись раннера целиком, id от Orders — коротко.
func (rs *runState) display(id string) string {
	for _, s := range rs.orders {
		if s.ID == "" && s.label() == id {
			return id
		}
	}
	return short(id)
}

// knownIDs — заказы, чьи id раннер знает из ответов на POST.
func (rs *runState) knownIDs() []string {
	var ids []string
	for _, s := range rs.orders {
		if s.ID != "" {
			ids = append(ids, s.ID)
		}
	}
	return ids
}

// allIDs — известные раннеру заказы плюс все order_id, с которыми Orders
// ходил в Payment (например, если ответ на POST не дошёл до раннера), кроме
// заказов прошлых сценариев.
func (rs *runState) allIDs() []string {
	seen := map[string]bool{}
	var ids []string
	add := func(id string) {
		if id != "" && !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
	}
	for _, id := range rs.knownIDs() {
		add(id)
	}
	extra := make([]string, 0)
	for id := range rs.facts.Payment.Requests.ByOrder {
		extra = append(extra, id)
	}
	for id := range rs.facts.Payment.Charges.ByOrder {
		extra = append(extra, id)
	}
	for id := range rs.facts.Bus.EventsByOrder {
		extra = append(extra, id)
	}
	sort.Strings(extra)
	for _, id := range extra {
		if !rs.prev[id] {
			add(id)
		}
	}
	return ids
}

// finalStatuses опрашивает GET /orders/{id} и дописывает итоги в out и amounts.
func (r *runner) finalStatuses(ids []string, out map[string]string, amounts map[string]int64) {
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 16)
	for _, id := range ids {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			st, amt := r.getOrder(id)
			mu.Lock()
			out[id], amounts[id] = st, amt
			mu.Unlock()
		}()
	}
	wg.Wait()
}

func (r *runner) getOrder(id string) (string, int64) {
	code, b, err := r.do(http.MethodGet, r.orders+"/orders/"+url.PathEscape(id), nil, 3*time.Second)
	if err != nil {
		return "ошибка GET: " + shortErr(err), 0
	}
	if code == http.StatusNotFound {
		return "404", 0
	}
	if code != http.StatusOK {
		return fmt.Sprintf("GET %d", code), 0
	}
	var resp struct {
		Status      string `json:"status"`
		AmountCents int64  `json:"amount_cents"`
	}
	if err := json.Unmarshal(b, &resp); err != nil {
		return "GET: не JSON", 0
	}
	switch resp.Status {
	case "paid", "rejected", "pending":
		return resp.Status, resp.AmountCents
	}
	return fmt.Sprintf("статус «%s»", trim(resp.Status, 20)), resp.AmountCents
}

// ---------- прогон сценария ----------

func (r *runner) runScenario(def *scenarioDef) (*runState, error) {
	p := r.params.scenario(def.ID)
	started := time.Now()

	if err := r.admin(http.MethodPost, "/admin/reset", nil); err != nil {
		return nil, fmt.Errorf("сброс заглушек: %w", err)
	}
	if def.ID == "S8" {
		return r.runS8(def, p, started)
	}
	pm := p.Payment
	pm.BlackholeAfterS = 0
	if def.ID == "S4" {
		pm.BlackholeAfterS = p.DownAfterS
	}
	bus := p.Bus
	bus.DownAfterS = 0
	switch def.ID {
	case "S6":
		bus.DownAfterS = p.DownAfterS
	case "S7":
		// Шина недоступна до перезапуска Orders: окно с запасом, его
		// закрывают следующие режимы.
		bus.DownS = maxS
	}
	r.setLabel(def.ID + " · " + def.Title)
	label := def.ID + " · " + def.Title
	setModes := func(pm PaymentModes, bus BusModes) error {
		modes := map[string]any{"seed": r.params.Seed, "payment": pm, "bus": bus, "label": label}
		if err := r.admin(http.MethodPut, "/admin/modes", modes); err != nil {
			return fmt.Errorf("режимы заглушек: %w", err)
		}
		return nil
	}
	if err := setModes(pm, bus); err != nil {
		return nil, err
	}
	t0 := time.Now()
	var killedAt time.Time

	// Состав заказов детерминирован seed и номером сценария.
	rnd := rand.New(rand.NewPCG(r.params.Seed, uint64(def.ID[1])))
	var (
		orders []*sent
		mu     sync.Mutex
	)
	if def.ID == "S4" {
		// Заказы «во время отказа» — с начала окна отказа, «после» — через
		// 2 с после его конца.
		time.Sleep(time.Until(t0.Add(seconds(p.DownAfterS))))
		waitDown := r.flow(rnd, p.Orders, p.RatePerS, "down", &orders, &mu)
		up := t0.Add(seconds(p.DownAfterS+p.Payment.BlackholeS) + 2*time.Second)
		time.Sleep(time.Until(up))
		waitAfter := r.flow(rnd, p.OrdersAfter, p.AfterRatePerS, "after", &orders, &mu)
		waitDown()
		waitAfter()
	} else if def.ID == "S7" {
		at, err := r.flowS7(p, rnd, pm, bus, setModes, &orders, &mu)
		killedAt = at
		if err != nil {
			return nil, err
		}
	} else {
		r.flow(rnd, p.Orders, p.RatePerS, "", &orders, &mu)()
	}
	if def.ID == "S6" {
		// Пауза — от конца отказа шины, а не от последнего заказа.
		time.Sleep(time.Until(t0.Add(seconds(p.DownAfterS + p.Bus.DownS))))
	}

	time.Sleep(seconds(r.params.WaitS))

	prev := make(map[string]bool, len(r.prev))
	for id := range r.prev {
		prev[id] = true
	}
	rs := &runState{params: r.params, orders: orders, prev: prev, killedAt: killedAt, started: started,
		final: map[string]string{}, amounts: map[string]int64{}}
	defer r.remember(rs)

	// Сначала статусы, потом факты: заказ, оплаченный между двумя
	// снимками, выглядит как «деньги есть, заказ не paid» — такие заказы
	// перечитываются ещё раз.
	r.finalStatuses(rs.knownIDs(), rs.final, rs.amounts)
	f, err := r.facts()
	if err != nil {
		return nil, fmt.Errorf("факты заглушек: %w", err)
	}
	rs.facts = f
	var extra, again []string
	for _, id := range rs.allIDs() {
		if _, ok := rs.final[id]; !ok {
			extra = append(extra, id)
		}
	}
	r.finalStatuses(extra, rs.final, rs.amounts)
	for _, id := range rs.allIDs() {
		if f.Payment.Charges.ByOrder[id] > 0 && rs.final[id] != "paid" {
			again = append(again, id)
		}
	}
	r.finalStatuses(again, rs.final, rs.amounts)
	rs.duration = time.Since(started)
	return rs, nil
}

// remember запоминает все заказы сценария — и известные раннеру, и те, что
// видны только в фактах заглушек.
func (r *runner) remember(rs *runState) {
	for _, id := range rs.knownIDs() {
		r.prev[id] = true
	}
	if rs.facts == nil {
		return
	}
	p := rs.facts.Payment
	for _, logs := range [][]payLog{p.Requests.Log, p.GetRequests.Log} {
		for _, l := range logs {
			if l.OrderID != "" {
				r.prev[l.OrderID] = true
			}
		}
	}
	for id := range p.Charges.ByOrder {
		r.prev[id] = true
	}
	for id := range rs.facts.Inventory.Reservations.ByOrder {
		r.prev[id] = true
	}
	for id := range rs.facts.Bus.EventsByOrder {
		r.prev[id] = true
	}
}

func seconds(v float64) time.Duration { return time.Duration(v * float64(time.Second)) }
