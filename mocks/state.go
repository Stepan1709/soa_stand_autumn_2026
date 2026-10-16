package main

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Modes — режимы отказов. PUT /admin/modes заменяет их целиком: не указанное
// поле — режим выключен.
type Modes struct {
	Seed    uint64       `json:"seed"`
	Payment PaymentModes `json:"payment"`
	// Bus — шина событий (задание 2), bus.go.
	Bus BusModes `json:"bus"`
	// Label — подпись текущего сценария для дашборда (/ui), её ставит
	// раннер; при ручных экспериментах пусто или «ручной режим».
	Label string `json:"label,omitempty"`
	// Finished — прогон раннера завершён: дашборд пишет «прогон завершён»
	// без отсчёта времени.
	Finished bool `json:"finished,omitempty"`
}

// modesView — режимы наружу (GET/PUT /admin/modes, факты, дашборд): без
// числа для генератора случайностей.
type modesView struct {
	Label    string       `json:"label"`
	Finished bool         `json:"finished,omitempty"`
	Payment  PaymentModes `json:"payment"`
	Bus      BusModes     `json:"bus"`
}

func (m Modes) view() modesView {
	return modesView{Label: m.Label, Finished: m.Finished, Payment: m.Payment, Bus: m.Bus}
}

// maxLabel — предел длины label, символов.
const maxLabel = 200

type PaymentModes struct {
	// FailRate — доля ответов 500 на POST /payments, операция не выполняется.
	FailRate float64 `json:"fail_rate"`
	// DeclineRate — доля отказов банка 402, операция не выполняется.
	DeclineRate float64 `json:"decline_rate"`
	// LatencyMS — задержка ответа на POST /payments.
	LatencyMS int `json:"latency_ms"`
	// BlackholeS — сколько секунд Payment не отвечает: соединение
	// принимается и висит, затем закрывается без ответа.
	BlackholeS float64 `json:"blackhole_s"`
	// BlackholeAfterS — через сколько секунд после установки режимов
	// начинается отказ: окно [установка+after, установка+after+blackhole_s).
	BlackholeAfterS float64 `json:"blackhole_after_s"`
	// LostResponse — "" | "off" | "first_per_order" | "rate:p": ответ после
	// выполненной операции не доходит, соединение закрывается.
	// first_per_order — теряется первый такой ответ по каждому order_id;
	// rate:p — доля потерянных ответов.
	LostResponse string `json:"lost_response"`
}

// maxWindowS — предел blackhole_s и blackhole_after_s, с.
const maxWindowS = 3600

func (m PaymentModes) validate() error {
	for name, v := range map[string]float64{"fail_rate": m.FailRate, "decline_rate": m.DeclineRate} {
		if !(v >= 0 && v <= 1) {
			return fmt.Errorf("%s: ожидается 0…1", name)
		}
	}
	if m.LatencyMS < 0 || m.LatencyMS > maxWindowS*1000 {
		return fmt.Errorf("latency_ms: ожидается 0…%d", maxWindowS*1000)
	}
	for name, v := range map[string]float64{"blackhole_s": m.BlackholeS, "blackhole_after_s": m.BlackholeAfterS} {
		if !(v >= 0 && v <= maxWindowS) { // !(…) ловит и NaN
			return fmt.Errorf("%s: ожидается 0…%d", name, maxWindowS)
		}
	}
	if _, err := parseLost(m.LostResponse); err != nil {
		return err
	}
	return nil
}

type lostMode struct {
	firstPerOrder bool
	rate          float64
}

func parseLost(s string) (lostMode, error) {
	switch {
	case s == "" || s == "off":
		return lostMode{}, nil
	case s == "first_per_order":
		return lostMode{firstPerOrder: true}, nil
	case strings.HasPrefix(s, "rate:"):
		p, err := strconv.ParseFloat(strings.TrimPrefix(s, "rate:"), 64)
		if err != nil || !(p >= 0 && p <= 1) {
			return lostMode{}, fmt.Errorf("lost_response: rate:p, p в 0…1")
		}
		return lostMode{rate: p}, nil
	}
	return lostMode{}, fmt.Errorf("lost_response: off | first_per_order | rate:p")
}

type charge struct {
	ChargeID    string `json:"charge_id"`
	OrderID     string `json:"order_id"`
	AmountCents int64  `json:"amount_cents"`
	Currency    string `json:"currency"`
	Status      string `json:"status"`
}

// keyed — запомненный исход операции под Idempotency-Key.
type keyed struct {
	body     chargeRequest
	declined bool
	charge   *charge
}

type reqLog struct {
	At        time.Time `json:"at"`
	OrderID   string    `json:"order_id"`
	Outcome   string    `json:"outcome"` // 201, 200, 402, 422, 500, 400, lost, blackhole
	Blackhole bool      `json:"blackhole"`
	Method    string    `json:"method"` // POST
	// AmountCents — amount_cents из тела запроса.
	AmountCents int64 `json:"amount_cents,omitempty"`
	// DoneAt — когда заглушка ответила или закрыла соединение, если ответ
	// задерживался (latency_ms, blackhole); иначе пусто — ответ сразу.
	DoneAt *time.Time `json:"done_at,omitempty"`
	// ClientClosed — клиент закрыл соединение раньше, чем заглушка ответила
	// (DoneAt — момент закрытия). Операция при этом выполнена, если исход 201.
	ClientClosed bool `json:"client_closed,omitempty"`

	charged bool // запрос создал новое списание
	wait    bool // ответ задерживается: списание засчитывается дашбордом по DoneAt
}

// getLog — один GET /payments?order_id=.
type getLog struct {
	At           time.Time  `json:"at"`
	OrderID      string     `json:"order_id"`
	Outcome      string     `json:"outcome"` // 200, 400, blackhole
	Blackhole    bool       `json:"blackhole"`
	Method       string     `json:"method"` // GET
	DoneAt       *time.Time `json:"done_at,omitempty"`
	ClientClosed bool       `json:"client_closed,omitempty"`
}

// invLog — один запрос к Inventory.
type invLog struct {
	At      time.Time `json:"at"`
	Method  string    `json:"method"`
	Op      string    `json:"op"` // reserve | release | get
	OrderID string    `json:"order_id"`
	Outcome string    `json:"outcome"` // 201, 200, 204, 400
}

type reservation struct {
	OrderID string     `json:"order_id"`
	Status  string     `json:"status"` // held | released
	Items   []resvItem `json:"items"`
}

type resvItem struct {
	SKU string `json:"sku"`
	Qty int    `json:"qty"`
}

type state struct {
	adminToken string
	// ordersURL — адрес Orders для заказов с дашборда (ORDERS_URL); пусто —
	// отправка выключена. runnerUI — сервис прогонов из дашборда (make ui).
	ordersURL string
	runnerUI  http.Handler
	// now — часы для признака «идёт прогон» (в тестах подменяются); nil — time.Now.
	now func() time.Time

	mu sync.Mutex

	modes      Modes
	lost       lostMode
	modesSetAt time.Time
	epoch      int // растёт на каждый reset/modes: висящие запросы отпускаются
	run        int // растёт на каждый reset: журналы начинаются заново
	boot       int64

	// events — лента запросов для дашборда (GET /admin/events) с момента
	// reset; seq сквозной и не сбрасывается.
	events  []event
	lastSeq int64

	n        uint64 // счётчик POST /payments — вход генератора случайностей
	seq      int    // счётчик charge_id
	charges  []charge
	keys     map[string]*keyed
	lostOnce map[string]bool // order_id, по которым ответ уже терялся
	reqs     []reqLog
	gets     []getLog
	invLogs  []invLog

	resv         map[string]*reservation
	resvRequests map[string]int

	// Шина (bus.go): busN — счётчик запросов во время отказа (вход генератора
	// случайностей), busSeen — принятые пары order_id+id, busByOrder — разные
	// id событий по заказу, dlv — доставка delivery.confirmed в Orders.
	busN       uint64
	busLogs    []busLog
	busSeen    map[string]bool
	busByOrder map[string][]string
	dlv        *delivery
	// bonusExpected — ожидаемый бонусный баланс покупателя: bonus_points по
	// всем партиям /admin/bus/deliver с reset (Orders копит баланс по всем).
	bonusExpected map[string]int
	dlvBatch      uint64 // партий /admin/bus/deliver с reset

	// Заказы с дашборда: текущая отправка и id, полученные от Orders.
	send     *sending
	known    []string
	knownSet map[string]bool
	checking bool // идёт POST /admin/orders/check
	// balancing — идёт POST /admin/bus/balances (balances.go).
	balancing bool

	// owner — владелец прогона раннера: идёт ли прогон (runlock.go).
	owner runOwner
}

func newState(m Modes) *state {
	s := &state{boot: time.Now().UnixMilli()}
	s.resetLocked()
	s.applyModesLocked(m)
	return s
}

func (s *state) resetLocked() {
	s.modes = Modes{Seed: s.modes.Seed}
	if s.modes.Seed == 0 {
		s.modes.Seed = 1
	}
	s.lost = lostMode{}
	s.modesSetAt = time.Now()
	s.epoch++
	s.run++
	s.events = nil
	s.n = 0
	s.seq = 0
	s.charges = nil
	s.keys = map[string]*keyed{}
	s.lostOnce = map[string]bool{}
	s.reqs = nil
	s.gets = nil
	s.invLogs = nil
	s.resv = map[string]*reservation{}
	s.resvRequests = map[string]int{}
	s.busN = 0
	s.busLogs = nil
	s.busSeen = map[string]bool{}
	s.busByOrder = map[string][]string{}
	s.bonusExpected = map[string]int{}
	s.dlvBatch = 0
	if s.dlv != nil {
		s.dlv.cancel()
		s.dlv = nil
	}
	// Ручная отправка заказов с дашборда прерывается: её заказы не должны
	// попасть в новый журнал.
	s.stopSendingLocked(stopReset)
	s.known = nil
	s.knownSet = map[string]bool{}
}

func (s *state) applyModesLocked(m Modes) {
	if m.Seed == 0 {
		m.Seed = s.modes.Seed
	}
	s.modes = m
	s.lost, _ = parseLost(m.Payment.LostResponse)
	s.modesSetAt = time.Now()
	s.epoch++
	// Счётчик случайностей начинается заново: одинаковые seed и режимы дают
	// одинаковую последовательность исходов.
	s.n = 0
}

// blackholeLocked — окно отказа [from, until); нулевые времена — отказа нет.
func (s *state) blackholeLocked() (from, until time.Time) {
	pm := s.modes.Payment
	if pm.BlackholeS <= 0 {
		return time.Time{}, time.Time{}
	}
	from = s.modesSetAt.Add(seconds(pm.BlackholeAfterS))
	return from, from.Add(seconds(pm.BlackholeS))
}

func (s *state) inBlackholeLocked(now time.Time) bool {
	from, until := s.blackholeLocked()
	return !until.IsZero() && !now.Before(from) && now.Before(until)
}

func seconds(v float64) time.Duration { return time.Duration(v * float64(time.Second)) }

// ---------- admin ----------

func (s *state) handleSetModes(w http.ResponseWriter, r *http.Request) {
	var m Modes
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&m); err != nil {
		writeErr(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if err := m.Payment.validate(); err != nil {
		writeErr(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if err := m.Bus.validate(); err != nil {
		writeErr(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if n := len([]rune(m.Label)); n > maxLabel {
		writeErr(w, http.StatusBadRequest, "bad_request", fmt.Sprintf("label: не длиннее %d символов", maxLabel))
		return
	}
	s.mu.Lock()
	// Метка конца прогона — finished; любые другие режимы от раннера —
	// прогон идёт. Чужой прогон раннер не трогает.
	if isRunner(r) && !s.runnerGateLocked(r, m.Finished) {
		s.mu.Unlock()
		writeErr(w, http.StatusConflict, "runner_active", otherRunMsg)
		return
	}
	s.applyModesLocked(m)
	out := s.modes.view()
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, out)
}

func (s *state) handleGetModes(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	out := s.modes.view()
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, out)
}

func (s *state) handleReset(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	if isRunner(r) && !s.runnerGateLocked(r, false) {
		s.mu.Unlock()
		writeErr(w, http.StatusConflict, "runner_active", otherRunMsg)
		return
	}
	s.resetLocked()
	s.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

type Facts struct {
	V         int            `json:"v"`
	Modes     modesView      `json:"modes"`
	Payment   PaymentFacts   `json:"payment"`
	Inventory InventoryFacts `json:"inventory"`
	Bus       BusFacts       `json:"bus"`
}

type PaymentFacts struct {
	Requests struct {
		Total       int            `json:"total"`
		ByStatus    map[string]int `json:"by_status"`
		ByOrder     map[string]int `json:"by_order"`
		InBlackhole int            `json:"in_blackhole"`
		Log         []reqLog       `json:"log"`
	} `json:"requests"`
	// GetRequests — GET /payments?order_id=.
	GetRequests struct {
		Total       int      `json:"total"`
		InBlackhole int      `json:"in_blackhole"`
		Log         []getLog `json:"log"`
	} `json:"get_requests"`
	Charges struct {
		Total   int            `json:"total"`
		ByOrder map[string]int `json:"by_order"`
		// AmountByOrder — сумма списанного по order_id, копейки.
		AmountByOrder map[string]int64 `json:"amount_by_order"`
	} `json:"charges"`
	Declines struct {
		Total   int            `json:"total"`
		ByOrder map[string]int `json:"by_order"`
	} `json:"declines"`
	Blackhole *struct {
		From  time.Time `json:"from"`
		Until time.Time `json:"until"`
	} `json:"blackhole,omitempty"`
}

type InventoryFacts struct {
	Requests struct {
		Reserve int `json:"reserve"`
		Release int `json:"release"`
	} `json:"requests"`
	// Log — все запросы к Inventory по порядку.
	Log          []invLog `json:"log"`
	Reservations struct {
		Held     int               `json:"held"`
		Released int               `json:"released"`
		ByOrder  map[string]string `json:"by_order"`
	} `json:"reservations"`
}

// handleFacts — GET /admin/facts. Снимок собирается под локом, в JSON —
// после: медленный читатель не задерживает Payment и Inventory.
func (s *state) handleFacts(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	f := s.factsLocked()
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, f)
}

func (s *state) factsLocked() Facts {
	f := Facts{V: 1, Modes: s.modes.view()}
	p := &f.Payment
	p.Requests.ByStatus = map[string]int{}
	p.Requests.ByOrder = map[string]int{}
	p.Requests.Log = append([]reqLog{}, s.reqs...)
	p.Charges.ByOrder = map[string]int{}
	p.Charges.AmountByOrder = map[string]int64{}
	p.Declines.ByOrder = map[string]int{}
	for _, rl := range s.reqs {
		p.Requests.Total++
		p.Requests.ByStatus[rl.Outcome]++
		if rl.OrderID != "" {
			p.Requests.ByOrder[rl.OrderID]++
		}
		if rl.Blackhole {
			p.Requests.InBlackhole++
		}
		if rl.Outcome == "402" {
			p.Declines.Total++
			p.Declines.ByOrder[rl.OrderID]++
		}
	}
	for _, c := range s.charges {
		p.Charges.Total++
		p.Charges.ByOrder[c.OrderID]++
		p.Charges.AmountByOrder[c.OrderID] += c.AmountCents
	}
	p.GetRequests.Total = len(s.gets)
	p.GetRequests.Log = append([]getLog{}, s.gets...)
	for _, l := range s.gets {
		if l.Blackhole {
			p.GetRequests.InBlackhole++
		}
	}
	if from, until := s.blackholeLocked(); !until.IsZero() {
		p.Blackhole = &struct {
			From  time.Time `json:"from"`
			Until time.Time `json:"until"`
		}{from, until}
	}

	inv := &f.Inventory
	inv.Requests.Reserve = s.resvRequests["reserve"]
	inv.Requests.Release = s.resvRequests["release"]
	inv.Log = append([]invLog{}, s.invLogs...)
	inv.Reservations.ByOrder = map[string]string{}
	for id, rv := range s.resv {
		inv.Reservations.ByOrder[id] = rv.Status
		if rv.Status == "held" {
			inv.Reservations.Held++
		} else {
			inv.Reservations.Released++
		}
	}
	f.Bus = s.busFactsLocked()
	return f
}

// ---------- helpers ----------

func (s *state) rng(n uint64) *rand.Rand {
	return rand.New(rand.NewPCG(s.modes.Seed, n))
}

// orderDraw — случайная величина, зависящая только от seed и order_id.
func (s *state) orderDraw(orderID string) float64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(orderID))
	return rand.New(rand.NewPCG(s.modes.Seed, h.Sum64())).Float64()
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, errCode, msg string) {
	writeJSON(w, code, map[string]string{"error": errCode, "message": msg})
}
