package main

// Шина событий — задание 2 (contracts/events.yaml).
//
// Входящие: POST /events — Orders публикует order.paid. Во время отказа шины
// (BusModes: окно [установка+down_after_s, …+down_s)) часть запросов висит до
// конца окна и закрывается без ответа, остальные сразу получают 503.
//
// Исходящие: POST /admin/bus/deliver — шина доставляет в Orders события
// delivery.confirmed (POST {ORDERS_URL}/webhooks/delivery): каждое 1…max_copies
// раз, часть копий одновременно, каждая копия — раз в секунду, пока Orders
// не ответит 2xx. POST /admin/bus/redeliver — ещё по одной копии каждого
// события. Факты — в /admin/facts (bus).

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"sort"
	"time"
)

// BusModes — режим шины. Не указано — шина работает.
type BusModes struct {
	// DownS — сколько секунд шина недоступна; DownAfterS — через сколько
	// секунд после установки режимов начинается отказ.
	DownS      float64 `json:"down_s"`
	DownAfterS float64 `json:"down_after_s"`
	// HangRate — доля запросов, которые во время отказа висят до его конца;
	// остальные сразу получают 503.
	HangRate float64 `json:"hang_rate"`
}

func (m BusModes) validate() error {
	for name, v := range map[string]float64{"bus.down_s": m.DownS, "bus.down_after_s": m.DownAfterS} {
		if !(v >= 0 && v <= maxWindowS) {
			return fmt.Errorf("%s: ожидается 0…%d", name, maxWindowS)
		}
	}
	if !(m.HangRate >= 0 && m.HangRate <= 1) {
		return fmt.Errorf("bus.hang_rate: ожидается 0…1")
	}
	return nil
}

// busSalt отделяет случайности шины от случайностей Payment при одном seed.
const busSalt = 0x9e3779b97f4a7c15

// maxEventID — предел длины id события.
const maxEventID = 128

// busLog — один POST /events.
type busLog struct {
	At      time.Time `json:"at"`
	ID      string    `json:"id"`
	OrderID string    `json:"order_id"`
	// Outcome — 202, 400, 503 или hang (висел до конца отказа).
	Outcome string `json:"outcome"`
	Down    bool   `json:"down"`
	// Dup — событие с этим id уже было принято (копия).
	Dup          bool       `json:"dup,omitempty"`
	DoneAt       *time.Time `json:"done_at,omitempty"`
	ClientClosed bool       `json:"client_closed,omitempty"`
}

// busWindowLocked — окно отказа шины [from, until); нулевые — отказа нет.
func (s *state) busWindowLocked() (from, until time.Time) {
	bm := s.modes.Bus
	if bm.DownS <= 0 {
		return time.Time{}, time.Time{}
	}
	from = s.modesSetAt.Add(seconds(bm.DownAfterS))
	return from, from.Add(seconds(bm.DownS))
}

func (s *state) busDownLocked(now time.Time) bool {
	from, until := s.busWindowLocked()
	return !until.IsZero() && !now.Before(from) && now.Before(until)
}

type publishRequest struct {
	ID         string `json:"id"`
	Type       string `json:"type"`
	OrderID    string `json:"order_id"`
	OccurredAt string `json:"occurred_at"`
}

// handlePublish — POST /events.
func (s *state) handlePublish(w http.ResponseWriter, r *http.Request) {
	var body publishRequest
	decodeErr := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&body)
	now := time.Now()

	s.mu.Lock()
	entry := busLog{At: now, ID: cutID(body.ID), OrderID: cutID(body.OrderID)}
	ev := event{At: now, Service: "bus", Method: http.MethodPost, Path: "/events", OrderID: body.OrderID,
		EventID: cutID(body.ID)}
	if s.busDownLocked(now) {
		entry.Down = true
		n := s.busN
		s.busN++
		if rand.New(rand.NewPCG(s.modes.Seed^busSalt, n)).Float64() < s.modes.Bus.HangRate {
			entry.Outcome = "hang"
			wt := s.logBusLocked(entry, ev, true)
			s.mu.Unlock()
			gone := s.hangWhile(w, r, s.busDownLocked)
			s.finish(wt, "no_response", gone)
			if !gone {
				closeWithoutResponse(w)
			}
			return
		}
		entry.Outcome = "503"
		s.logBusLocked(entry, ev, false)
		s.mu.Unlock()
		writeErr(w, http.StatusServiceUnavailable, "unavailable", "шина недоступна")
		return
	}
	if decodeErr != nil || body.ID == "" || len(body.ID) > maxEventID || body.Type != "order.paid" || body.OrderID == "" {
		entry.Outcome = "400"
		s.logBusLocked(entry, ev, false)
		s.mu.Unlock()
		writeErr(w, http.StatusBadRequest, "bad_request",
			fmt.Sprintf("ожидается {id (1…%d символов), type: order.paid, order_id}", maxEventID))
		return
	}
	key := body.OrderID + "\x00" + body.ID
	entry.Dup = s.busSeen[key]
	if !entry.Dup {
		s.busSeen[key] = true
		s.busByOrder[body.OrderID] = append(s.busByOrder[body.OrderID], body.ID)
	}
	entry.Outcome = "202"
	s.logBusLocked(entry, ev, false)
	s.mu.Unlock()
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "accepted"})
}

func (s *state) logBusLocked(entry busLog, ev event, wait bool) waiting {
	s.busLogs = append(s.busLogs, entry)
	ev.Outcome, ev.Wait = entry.Outcome, wait
	return waiting{run: s.run, seq: s.addEventLocked(ev), idx: len(s.busLogs) - 1, bus: true}
}

// ---------- доставка delivery.confirmed в Orders ----------

type deliverRequest struct {
	Events       int     `json:"events"`
	Users        int     `json:"users"`
	MaxCopies    int     `json:"max_copies"`
	ParallelRate float64 `json:"parallel_rate"`
	RatePerS     float64 `json:"rate_per_s"`
}

func (d deliverRequest) validate() error {
	switch {
	case d.Events < 1 || d.Events > 10000:
		return errors.New("events: 1…10000")
	case d.Users < 1 || d.Users > d.Events:
		return errors.New("users: 1…events")
	case d.MaxCopies < 1 || d.MaxCopies > 10:
		return errors.New("max_copies: 1…10")
	case !(d.ParallelRate >= 0 && d.ParallelRate <= 1):
		return errors.New("parallel_rate: 0…1")
	case !(d.RatePerS > 0 && d.RatePerS <= 1000):
		return errors.New("rate_per_s: 0…1000")
	}
	return nil
}

type dlvEvent struct {
	ID       string `json:"id"`
	UserID   string `json:"user_id"`
	Points   int    `json:"bonus_points"`
	Copies   int    `json:"copies"`
	Acked    int    `json:"acked"`
	Attempts int    `json:"attempts"`
}

type delivery struct {
	req    deliverRequest
	events []*dlvEvent
	ctx    context.Context
	cancel context.CancelFunc
	// wait и retry — dlvWait и dlvRetry на момент начала доставки.
	wait, retry time.Duration
}

// pendingLocked — копий, ещё не подтверждённых Orders.
func (d *delivery) pendingLocked() int {
	n := 0
	for _, e := range d.events {
		n += e.Copies - e.Acked
	}
	return n
}

// dlvClient — к Orders: без редиректов, как ordersClient.
var dlvClient = &http.Client{
	Transport: &http.Transport{MaxIdleConns: 100, MaxIdleConnsPerHost: 100, IdleConnTimeout: 30 * time.Second},
	CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	},
}

// dlvWait — сколько ждать ответа Orders на одну доставку; dlvRetry — пауза
// между попытками одной копии.
var (
	dlvWait  = 5 * time.Second
	dlvRetry = time.Second
)

// handleDeliver — POST /admin/bus/deliver: новая партия событий. Пока прошлая
// не подтверждена целиком — 409.
func (s *state) handleDeliver(w http.ResponseWriter, r *http.Request) {
	var req deliverRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<12))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if err := req.validate(); err != nil {
		writeErr(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if s.ordersURL == "" {
		writeErr(w, http.StatusServiceUnavailable, "no_orders", "адрес Orders заглушкам не задан (ORDERS_URL)")
		return
	}
	s.mu.Lock()
	if s.dlv != nil && s.dlv.pendingLocked() > 0 {
		s.mu.Unlock()
		writeErr(w, http.StatusConflict, "busy", "прошлая доставка ещё не подтверждена")
		return
	}
	if s.dlv != nil {
		s.dlv.cancel()
	}
	// Номер партии — в seed: иначе вторая партия после той же reset
	// повторила бы id событий первой, и Orders верно счёл бы их копиями.
	s.dlvBatch++
	rnd := rand.New(rand.NewPCG(s.modes.Seed^busSalt, uint64(s.run)<<32|uint64(s.dlvBatch)))
	ctx, cancel := context.WithCancel(context.Background())
	d := &delivery{req: req, ctx: ctx, cancel: cancel, wait: dlvWait, retry: dlvRetry}
	type copyPlan struct {
		ev    *dlvEvent
		delay time.Duration
	}
	var plan []copyPlan
	for i := 0; i < req.Events; i++ {
		ev := &dlvEvent{
			ID:     fmt.Sprintf("evt-%016x", rnd.Uint64()),
			UserID: fmt.Sprintf("u-%d", 1+i%req.Users),
			Points: 10 * (1 + rnd.IntN(10)),
			Copies: 1 + rnd.IntN(req.MaxCopies),
		}
		d.events = append(d.events, ev)
		start := time.Duration(float64(i) / req.RatePerS * float64(time.Second))
		for k := 0; k < ev.Copies; k++ {
			delay := start
			if k > 0 && rnd.Float64() >= req.ParallelRate {
				// Не одновременно: копия приходит позже, через 0,3…1,5 с.
				delay += time.Duration(300+rnd.IntN(1200)) * time.Millisecond
			}
			plan = append(plan, copyPlan{ev, delay})
		}
	}
	s.dlv = d
	s.addExpectedLocked(d)
	run := s.run
	view := s.deliveryFactsLocked()
	s.mu.Unlock()

	sort.SliceStable(plan, func(i, j int) bool { return plan[i].delay < plan[j].delay })
	t0 := time.Now()
	go func() {
		for _, c := range plan {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Until(t0.Add(c.delay))):
			}
			go s.deliverCopy(d, run, c.ev)
		}
	}()
	writeJSON(w, http.StatusAccepted, view)
}

// handleRedeliver — POST /admin/bus/redeliver: ещё по одной копии каждого
// события последней партии (с тем же темпом).
func (s *state) handleRedeliver(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	d := s.dlv
	if d == nil {
		s.mu.Unlock()
		writeErr(w, http.StatusConflict, "no_delivery", "доставки ещё не было")
		return
	}
	for _, ev := range d.events {
		ev.Copies++
	}
	run := s.run
	events := append([]*dlvEvent(nil), d.events...)
	rate := d.req.RatePerS
	view := s.deliveryFactsLocked()
	s.mu.Unlock()

	t0 := time.Now()
	go func() {
		for i, ev := range events {
			select {
			case <-d.ctx.Done():
				return
			case <-time.After(time.Until(t0.Add(time.Duration(float64(i) / rate * float64(time.Second))))):
			}
			go s.deliverCopy(d, run, ev)
		}
	}()
	writeJSON(w, http.StatusAccepted, view)
}

// deliverCopy доставляет одну копию события, пока Orders не ответит 2xx.
func (s *state) deliverCopy(d *delivery, run int, ev *dlvEvent) {
	body, _ := json.Marshal(map[string]any{
		"id": ev.ID, "type": "delivery.confirmed", "user_id": ev.UserID,
		"bonus_points": ev.Points, "occurred_at": time.Now().UTC().Format(time.RFC3339),
	})
	for {
		if d.ctx.Err() != nil {
			return
		}
		now := time.Now()
		outcome := postDelivery(d.ctx, d.wait, s.ordersURL+"/webhooks/delivery", body)
		s.mu.Lock()
		if s.run != run || d.ctx.Err() != nil {
			s.mu.Unlock()
			return
		}
		ev.Attempts++
		ok := len(outcome) == 3 && outcome[0] == '2'
		if ok {
			ev.Acked++
		}
		// Без order_id: доставка — не заказ, в списке заказов дашборда её нет.
		s.addEventLocked(event{At: now, Service: "bus", Method: http.MethodPost, Path: "/webhooks/delivery",
			Outcome: outcome})
		s.mu.Unlock()
		if ok {
			return
		}
		select {
		case <-d.ctx.Done():
			return
		case <-time.After(d.retry):
		}
	}
}

// postDelivery — один POST в Orders: код ответа, «no_response» или «no_connection».
func postDelivery(parent context.Context, wait time.Duration, url string, body []byte) string {
	ctx, cancel := context.WithTimeout(parent, wait)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := dlvClient.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return "no_response"
		}
		return "no_connection"
	}
	resp.Body.Close()
	return fmt.Sprint(resp.StatusCode)
}

// ---------- факты ----------

type BusFacts struct {
	Requests struct {
		Total    int            `json:"total"`
		ByStatus map[string]int `json:"by_status"`
		InDown   int            `json:"in_down"`
	} `json:"requests"`
	// Accepted — принятых публикаций (202), включая копии.
	Accepted int `json:"accepted"`
	// EventsByOrder — order_id → разные id принятых событий по порядку.
	EventsByOrder map[string][]string `json:"events_by_order"`
	Log           []busLog            `json:"log"`
	Down          *window             `json:"down,omitempty"`
	Delivery      *DeliveryFacts      `json:"delivery,omitempty"`
}

type DeliveryFacts struct {
	Events []dlvEvent `json:"events"`
	// Pending — копий, ещё не подтверждённых Orders (2xx).
	Pending int `json:"pending"`
	// AckedEvents — событий, у которых подтверждена хотя бы одна копия.
	AckedEvents int `json:"acked_events"`
	Attempts    int `json:"attempts"`
	// ExpectedByUser — сумма bonus_points по разным событиям покупателя.
	ExpectedByUser map[string]int `json:"expected_by_user"`
}

func (s *state) busFactsLocked() BusFacts {
	var f BusFacts
	f.Requests.ByStatus = map[string]int{}
	f.EventsByOrder = map[string][]string{}
	for _, l := range s.busLogs {
		f.Requests.Total++
		f.Requests.ByStatus[l.Outcome]++
		if l.Down {
			f.Requests.InDown++
		}
		if l.Outcome == "202" {
			f.Accepted++
		}
	}
	for id, evs := range s.busByOrder {
		f.EventsByOrder[id] = append([]string(nil), evs...)
	}
	f.Log = append([]busLog{}, s.busLogs...)
	if from, until := s.busWindowLocked(); !until.IsZero() {
		f.Down = &window{from, until}
	}
	if s.dlv != nil {
		df := s.deliveryFactsLocked()
		f.Delivery = &df
	}
	return f
}

func (s *state) deliveryFactsLocked() DeliveryFacts {
	df := DeliveryFacts{ExpectedByUser: map[string]int{}}
	if s.dlv == nil {
		return df
	}
	for _, e := range s.dlv.events {
		df.Events = append(df.Events, *e)
		df.Pending += e.Copies - e.Acked
		df.Attempts += e.Attempts
		if e.Acked > 0 {
			df.AckedEvents++
		}
		df.ExpectedByUser[e.UserID] += e.Points
	}
	return df
}

// addExpectedLocked добавляет bonus_points новой партии к ожидаемым балансам
// (redeliver их не меняет: копии тех же событий).
func (s *state) addExpectedLocked(d *delivery) {
	for _, e := range d.events {
		s.bonusExpected[e.UserID] += e.Points
	}
}
