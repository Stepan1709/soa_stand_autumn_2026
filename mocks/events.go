package main

import (
	_ "embed"
	"net/http"
	"strconv"
	"time"
)

// event — одна строка ленты дашборда: запрос к Payment или Inventory либо
// заказ, отправленный в Orders с дашборда (service = "orders").
// Если ответ задерживается (latency_ms, blackhole), у запроса Wait = true, а
// когда заглушка ответит или закроет соединение, в ленту добавляется событие
// Kind = "done" с Ref = seq запроса и итоговым исходом («client_closed» —
// соединение закрыл клиент, не дождавшись ответа).
type event struct {
	Seq     int64     `json:"seq"`
	At      time.Time `json:"at"`
	Kind    string    `json:"kind,omitempty"` // "" — запрос, "done" — ответ на запрос Ref
	Ref     int64     `json:"ref,omitempty"`
	Service string    `json:"service,omitempty"` // payment | inventory | orders
	Method  string    `json:"method,omitempty"`
	Path    string    `json:"path,omitempty"` // шаблон пути, без order_id
	OrderID string    `json:"order_id,omitempty"`
	// EventID — у публикаций в шину (bus, /events): id события из тела, как
	// в фактах events_by_order; пусто — тело не разобралось или id нет.
	EventID     string `json:"event_id,omitempty"`
	Outcome     string `json:"outcome"`
	AmountCents int64  `json:"amount_cents,omitempty"`
	Wait        bool   `json:"wait,omitempty"`
	// Charged — запрос к Payment создал новое списание (засчитывается, когда
	// у запроса есть исход: сразу или по событию done).
	Charged bool `json:"charged,omitempty"`
	// Status и LatencyMS — у заказов, отправленных с дашборда: статус из
	// ответа Orders и время ответа.
	Status    string `json:"status,omitempty"`
	LatencyMS int64  `json:"latency_ms,omitempty"`
}

// maxEventOrderID — order_id в ленте обрезается: дашборду хватает начала.
const maxEventOrderID = 128

// eventsPage — сколько событий отдаёт один GET /admin/events.
const eventsPage = 2000

// maxEvents — сколько последних событий хранит лента: старые вытесняются.
var maxEvents = 50000

// addEventLocked добавляет событие в ленту и возвращает его seq.
func (s *state) addEventLocked(e event) int64 {
	e.OrderID = cutID(e.OrderID)
	s.lastSeq++
	e.Seq = s.lastSeq
	s.events = append(s.events, e)
	if n := len(s.events); n > maxEvents {
		// Сдвиг раз в maxEvents/10 событий: память ограничена, копирование редкое.
		if n >= maxEvents+maxEvents/10+1 {
			s.events = append([]event(nil), s.events[n-maxEvents:]...)
		}
	}
	return e.Seq
}

// eventsLocked — хранимая часть ленты: последние maxEvents событий.
func (s *state) eventsLocked() []event {
	if n := len(s.events); n > maxEvents {
		return s.events[n-maxEvents:]
	}
	return s.events
}

// waiting — запрос с задержанным ответом: как отметить его завершение.
type waiting struct {
	run int
	seq int64
	idx int  // индекс в журнале s.reqs, s.gets или s.busLogs
	get bool // журнал s.gets
	bus bool // журнал s.busLogs
}

// finish отмечает конец задержанного запроса: в журнале фактов (done_at) и
// в ленте (событие done). clientClosed — соединение закрыл клиент, не
// дождавшись ответа. После reset старый запрос ни на что не влияет.
func (s *state) finish(wt waiting, outcome string, clientClosed bool) {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if wt.run != s.run {
		return
	}
	if wt.bus {
		if wt.idx < len(s.busLogs) {
			s.busLogs[wt.idx].DoneAt = &now
			s.busLogs[wt.idx].ClientClosed = clientClosed
		}
	} else if wt.get {
		if wt.idx < len(s.gets) {
			s.gets[wt.idx].DoneAt = &now
			s.gets[wt.idx].ClientClosed = clientClosed
		}
	} else if wt.idx < len(s.reqs) {
		s.reqs[wt.idx].DoneAt = &now
		s.reqs[wt.idx].ClientClosed = clientClosed
	}
	if clientClosed {
		outcome = "client_closed"
	}
	s.addEventLocked(event{At: now, Kind: "done", Ref: wt.seq, Outcome: outcome})
}

type eventsResp struct {
	// Boot и Run меняются при перезапуске заглушек и на каждый reset:
	// дашборд тогда начинает ленту заново.
	Boot      int64     `json:"boot"`
	Run       int       `json:"run"`
	Now       time.Time `json:"now"`
	StartedAt time.Time `json:"started_at"`
	Modes     modesView `json:"modes"`
	Blackhole *window   `json:"blackhole,omitempty"`
	// BusDown — окно отказа шины (режим bus.down_s), если задано.
	BusDown *window `json:"bus_down,omitempty"`
	// Seq — последний отданный seq: следующий запрос — ?since=Seq.
	Seq  int64 `json:"seq"`
	More bool  `json:"more"`
	// Truncated — часть событий после since уже вытеснена из ленты: дашборд
	// начинает ленту заново с того, что есть.
	Truncated bool          `json:"truncated,omitempty"`
	Events    []event       `json:"events"`
	Summary   eventsSummary `json:"summary"`
	// Sending — отправка заказов с дашборда (POST /admin/orders), если была.
	Sending *sendingView `json:"sending,omitempty"`
	// OrdersEnabled — заглушкам задан адрес Orders: дашборд может слать заказы.
	OrdersEnabled bool `json:"orders_enabled"`
	// RunnerActive — идёт прогон раннера (make scenarios или из дашборда):
	// ручные действия со страницы получают 409. RunnerSource — откуда
	// прогон: cli | dashboard (пусто — неизвестно).
	RunnerActive bool   `json:"runner_active"`
	RunnerSource string `json:"runner_source,omitempty"`
}

type window struct {
	From  time.Time `json:"from"`
	Until time.Time `json:"until"`
}

type eventsSummary struct {
	Payment struct {
		Requests  int            `json:"requests"`   // POST /payments
		ByOutcome map[string]int `json:"by_outcome"` // POST /payments по исходам
		Gets      int            `json:"gets"`       // GET /payments
		GetsDown  int            `json:"gets_down"`  // GET /payments без ответа
	} `json:"payment"`
	Charges struct {
		Total         int `json:"total"`
		Orders        int `json:"orders"`
		DoubleCharged int `json:"double_charged"`
	} `json:"charges"`
	Declines     int `json:"declines"`
	Reservations struct {
		Held     int `json:"held"`
		Released int `json:"released"`
	} `json:"reservations"`
	Bus busSummary `json:"bus"`
	// Delivery — доставка delivery.confirmed в Orders; null — её не было.
	Delivery *deliverySummary `json:"delivery"`
}

// busSummary — публикации order.paid в шину (POST /events).
type busSummary struct {
	Publish   int            `json:"publish"`    // всего POST /events
	ByOutcome map[string]int `json:"by_outcome"` // 202 | 400 | 503 | hang
	Accepted  int            `json:"accepted"`   // принято (202), включая копии
	// OrdersWithEvent — заказов, по которым принято хотя бы одно событие.
	OrdersWithEvent int `json:"orders_with_event"`
	InDown          int `json:"in_down"` // запросов во время отказа шины
}

// deliverySummary — сводка доставки delivery.confirmed.
type deliverySummary struct {
	Events      int `json:"events"`       // событий в партии
	Deliveries  int `json:"deliveries"`   // копий всего (с учётом redeliver)
	AckedEvents int `json:"acked_events"` // событий с подтверждённой копией
	Pending     int `json:"pending"`      // копий без ответа 2xx
	Attempts    int `json:"attempts"`     // POST /webhooks/delivery всего
	Users       int `json:"users"`        // покупателей в партии
}

// handleEvents — GET /admin/events?since=<seq>: лента для дашборда. Отдаёт
// события с seq > since (не больше eventsPage, More — есть ещё), сводку и
// режимы. Если since из прошлого reset (run сменился), лента отдаётся с
// начала: дашборд сравнивает run и начинает заново. Снимок собирается под
// локом, в JSON — после.
func (s *state) handleEvents(w http.ResponseWriter, r *http.Request) {
	since, _ := strconv.ParseInt(r.URL.Query().Get("since"), 10, 64)
	s.mu.Lock()
	out := s.eventsSnapshotLocked(since)
	s.mu.Unlock()
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, out)
}

func (s *state) eventsSnapshotLocked(since int64) eventsResp {
	out := eventsResp{
		Boot:          s.boot,
		Run:           s.run,
		Now:           time.Now(),
		StartedAt:     s.modesSetAt,
		Modes:         s.modes.view(),
		Seq:           since,
		Events:        []event{},
		Sending:       s.sendingLocked(),
		OrdersEnabled: s.ordersURL != "",
	}
	out.RunnerActive, out.RunnerSource = s.runnerActiveLocked()
	if from, until := s.blackholeLocked(); !until.IsZero() {
		out.Blackhole = &window{from, until}
	}
	if from, until := s.busWindowLocked(); !until.IsZero() {
		out.BusDown = &window{from, until}
	}
	if evs := s.eventsLocked(); len(evs) > 0 {
		if since > 0 && since < evs[0].Seq-1 {
			out.Truncated = true
		}
		i := max(since-evs[0].Seq+1, 0)
		if i < int64(len(evs)) {
			j := min(i+eventsPage, int64(len(evs)))
			out.Events = append(out.Events, evs[i:j]...)
			out.More = j < int64(len(evs))
		}
	}
	out.Seq = s.lastSeq
	if n := len(out.Events); n > 0 {
		out.Seq = out.Events[n-1].Seq
	}

	sum := &out.Summary
	sum.Payment.ByOutcome = map[string]int{}
	for _, rl := range s.reqs {
		sum.Payment.Requests++
		sum.Payment.ByOutcome[rl.Outcome]++
		if rl.Outcome == "402" {
			sum.Declines++
		}
	}
	for _, l := range s.gets {
		sum.Payment.Gets++
		if l.Blackhole {
			sum.Payment.GetsDown++
		}
	}
	// Списание засчитывается, когда у запроса есть исход: при задержке
	// ответа — после неё (или после того, как клиент ушёл).
	byOrder := map[string]int{}
	for _, rl := range s.reqs {
		if rl.charged && (!rl.wait || rl.DoneAt != nil) {
			sum.Charges.Total++
			byOrder[rl.OrderID]++
		}
	}
	sum.Charges.Orders = len(byOrder)
	for _, n := range byOrder {
		if n >= 2 {
			sum.Charges.DoubleCharged++
		}
	}
	for _, rv := range s.resv {
		if rv.Status == "held" {
			sum.Reservations.Held++
		} else {
			sum.Reservations.Released++
		}
	}
	sum.Bus.ByOutcome = map[string]int{}
	for _, l := range s.busLogs {
		sum.Bus.Publish++
		sum.Bus.ByOutcome[l.Outcome]++
		if l.Outcome == "202" {
			sum.Bus.Accepted++
		}
		if l.Down {
			sum.Bus.InDown++
		}
	}
	sum.Bus.OrdersWithEvent = len(s.busByOrder)
	if d := s.dlv; d != nil {
		ds := &deliverySummary{Events: len(d.events)}
		users := map[string]bool{}
		for _, e := range d.events {
			ds.Deliveries += e.Copies
			ds.Pending += e.Copies - e.Acked
			ds.Attempts += e.Attempts
			if e.Acked > 0 {
				ds.AckedEvents++
			}
			users[e.UserID] = true
		}
		ds.Users = len(users)
		sum.Delivery = ds
	}
	return out
}

//go:embed ui.html
var uiPage []byte

// handleUI — GET /ui: страница дашборда. Сама страница без токена, данные
// она берёт из /admin/events с токеном из фрагмента ссылки (#token=…).
func handleUI(w http.ResponseWriter, _ *http.Request) {
	h := w.Header()
	h.Set("Content-Type", "text/html; charset=utf-8")
	h.Set("Cache-Control", "no-store")
	h.Set("Referrer-Policy", "no-referrer")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Content-Security-Policy", "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; "+
		"connect-src 'self'; img-src data:; base-uri 'none'; form-action 'none'; frame-ancestors 'none'")
	_, _ = w.Write(uiPage)
}
