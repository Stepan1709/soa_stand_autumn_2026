// Раннер сценариев: гоняет поток заказов в Orders, ломает заглушки через
// /admin/*, после паузы сверяет статусы заказов с журналом фактов заглушек.
package main

import (
	"bytes"
	"embed"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

//go:embed params/*.json
var paramFiles embed.FS

// Params — параметры сценариев. Файл подмены (-params) накладывается поверх
// встроенного: переопределяются только присутствующие в нём поля.
type Params struct {
	Seed             uint64  `json:"seed"`
	BudgetMS         int     `json:"budget_ms"`
	WaitS            float64 `json:"wait_s"`
	RequestTimeoutMS int     `json:"request_timeout_ms"`
	Scenarios        struct {
		S1 ScenarioParams `json:"S1"`
		S2 ScenarioParams `json:"S2"`
		S3 ScenarioParams `json:"S3"`
		S4 ScenarioParams `json:"S4"`
		S5 ScenarioParams `json:"S5"`
		S6 ScenarioParams `json:"S6"`
		S7 ScenarioParams `json:"S7"`
		S8 ScenarioParams `json:"S8"`
	} `json:"scenarios"`
}

type ScenarioParams struct {
	Orders   int          `json:"orders"`
	RatePerS float64      `json:"rate_per_s"`
	Payment  PaymentModes `json:"payment"`
	// S2: минимальная доля оплаченных заказов.
	MinPaid float64 `json:"min_paid,omitempty"`
	// S4: через сколько секунд после установки режимов Payment перестаёт
	// отвечать, заказы после подъёма Payment и предел запросов за время отказа.
	DownAfterS      float64 `json:"down_after_s,omitempty"`
	OrdersAfter     int     `json:"orders_after,omitempty"`
	AfterRatePerS   float64 `json:"after_rate_per_s,omitempty"`
	MaxRequestsDown int     `json:"max_requests_down,omitempty"`

	// Задание 2. Bus — режим шины (S6: окно отказа с down_after_s; S7: шина
	// недоступна до перезапуска Orders).
	Bus BusModes `json:"bus,omitempty"`
	// S7: заказов, которые уходят перед перезапуском, пока Payment отвечает
	// с задержкой kill_latency_ms; restart_s — сколько Orders лежит.
	KillOrders    int     `json:"kill_orders,omitempty"`
	KillLatencyMS int     `json:"kill_latency_ms,omitempty"`
	RestartS      float64 `json:"restart_s,omitempty"`
	// S8: событий delivery.confirmed, покупателей, копий на событие (1…),
	// доля одновременных копий; перезапуск — после kill_after_events
	// подтверждённых событий. Темп — rate_per_s.
	Events          int     `json:"events,omitempty"`
	Users           int     `json:"users,omitempty"`
	MaxCopies       int     `json:"max_copies,omitempty"`
	ParallelRate    float64 `json:"parallel_rate,omitempty"`
	KillAfterEvents int     `json:"kill_after_events,omitempty"`
}

type BusModes struct {
	DownS    float64 `json:"down_s,omitempty"`
	HangRate float64 `json:"hang_rate,omitempty"`
	// DownAfterS в файле параметров не задаётся: раннер берёт его из
	// down_after_s сценария.
	DownAfterS float64 `json:"down_after_s,omitempty"`
}

type PaymentModes struct {
	FailRate    float64 `json:"fail_rate,omitempty"`
	DeclineRate float64 `json:"decline_rate,omitempty"`
	LatencyMS   int     `json:"latency_ms,omitempty"`
	BlackholeS  float64 `json:"blackhole_s,omitempty"`
	// BlackholeAfterS в файле параметров не задаётся: раннер берёт его из
	// down_after_s сценария.
	BlackholeAfterS float64 `json:"blackhole_after_s,omitempty"`
	LostResponse    string  `json:"lost_response,omitempty"`
}

func (p *Params) scenario(id string) *ScenarioParams {
	switch id {
	case "S1":
		return &p.Scenarios.S1
	case "S2":
		return &p.Scenarios.S2
	case "S3":
		return &p.Scenarios.S3
	case "S4":
		return &p.Scenarios.S4
	case "S5":
		return &p.Scenarios.S5
	case "S6":
		return &p.Scenarios.S6
	case "S7":
		return &p.Scenarios.S7
	case "S8":
		return &p.Scenarios.S8
	}
	return nil
}

var tasks = map[string][]string{
	"1": {"S1", "S2", "S3", "S4", "S5"},
	"2": {"S1", "S6", "S7", "S8"},
}

func main() {
	var (
		task       = flag.String("task", envOr("TASK", "1"), "номер задания")
		only       = flag.String("only", os.Getenv("ONLY"), "только эти сценарии, через запятую: S3 или S2,S5")
		paramsFile = flag.String("params", os.Getenv("PARAMS"), "файл подмены параметров (JSON)")
		wait       = flag.String("wait", os.Getenv("WAIT"), "пауза после последнего заказа, с (по умолчанию из параметров)")
		ordersURL  = flag.String("orders-url", envOr("ORDERS_URL", "http://orders:8080"), "адрес Orders")
		mocksURL   = flag.String("mocks-url", envOr("MOCKS_URL", "http://mocks:8080"), "адрес заглушек")
		results    = flag.String("results", os.Getenv("RESULTS"), "куда записать результаты в JSON")
		serveAddr  = flag.String("serve", "", "режим сервиса для дашборда: слушать адрес (:8081) и запускать прогоны по POST /run")
		allowFrom  = flag.String("allow-from", os.Getenv("RUNNER_ALLOW_FROM"), "-serve: пускать только с адресов этого имени (mocks-admin)")
		chaosDir   = flag.String("chaos", os.Getenv("CHAOS_DIR"), "каталог, через который make перезапускает Orders (S7, S8)")
	)
	flag.Parse()

	if *serveAddr != "" {
		os.Exit(serve(serveConfig{
			addr: *serveAddr, task: *task, paramsFile: *paramsFile, results: *results,
			orders: *ordersURL, mocks: *mocksURL, allowFrom: *allowFrom,
			token: os.Getenv("MOCKS_ADMIN_TOKEN"),
		}))
	}

	ids, ok := tasks[*task]
	if !ok {
		failRun(*results, *task, 0, fmt.Sprintf("неизвестное задание %q", *task))
	}
	if *only != "" {
		ids = splitOnly(*only)
	}

	params, err := loadParams(*task, *paramsFile)
	if err != nil {
		failRun(*results, *task, len(ids), fmt.Sprintf("параметры: %v", err))
	}
	if bad := unknownID(params, ids); bad != "" {
		failRun(*results, *task, len(ids), fmt.Sprintf("неизвестный сценарий %q", bad))
	}
	if *wait != "" {
		v, err := strconv.ParseFloat(*wait, 64)
		if err != nil || !validWait(v) {
			failRun(*results, *task, len(ids), "WAIT: ожидается число секунд 0…3600")
		}
		params.WaitS = v
	}

	r := newRunner(params, *ordersURL, *mocksURL, os.Getenv("MOCKS_ADMIN_TOKEN"), newOutput(os.Stdout))
	r.chaosDir = *chaosDir
	// Прогон из дашборда (make ui) ходит в те же заглушки: два прогона
	// сразу испортили бы друг другу проверку.
	if r.dashboardBusy() {
		fmt.Println("СТЕНД: сейчас идёт другой прогон сценариев (из дашборда или make scenarios): оба прогона ходят в одни и те же заглушки.")
		fmt.Println("Дождитесь его конца и запустите make scenarios снова.")
		os.Exit(2)
	}
	// Ctrl+C и SIGTERM (docker stop): метка конца прогона сразу, иначе ручные
	// кнопки дашборда ждали бы страховочные 150 с.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		r.out.println("\nСТЕНД: прогон прерван.")
		r.endRun("прогон прерван")
		os.Exit(130)
	}()
	os.Exit(r.run(*task, ids, *results))
}

// splitOnly — список сценариев из -only: «s2, S5» → [S2 S5].
func splitOnly(only string) []string {
	var ids []string
	for _, s := range strings.Split(only, ",") {
		s = strings.ToUpper(strings.TrimSpace(s))
		if s != "" {
			ids = append(ids, s)
		}
	}
	return ids
}

// unknownID — первый сценарий, которого нет, или "".
func unknownID(p *Params, ids []string) string {
	for _, id := range ids {
		if p.scenario(id) == nil || scenarioByID(id) == nil {
			return id
		}
	}
	return ""
}

func validWait(v float64) bool { return v >= 0 && v <= maxS }

func newRunner(params *Params, ordersURL, mocksURL, token string, out *output) *runner {
	return &runner{
		runID:      newRunID(),
		out:        out,
		prev:       map[string]bool{},
		adminToken: token,
		params:     params,
		orders:     strings.TrimRight(ordersURL, "/"),
		mocks:      strings.TrimRight(mocksURL, "/"),
		// По редиректам не ходит: ответ Orders с Location не уводит раннер
		// на другой адрес (например, в admin-сеть к заглушкам).
		client: &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        200,
				MaxIdleConnsPerHost: 200,
				IdleConnTimeout:     30 * time.Second,
			},
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}
}

// dashboardBusy — идёт ли другой прогон: из дашборда (статус runner-ui через
// прокси заглушек) или любой прогон раннера по данным заглушек (GET
// /admin/run: второй make scenarios). Любая ошибка — «не идёт»: runner-ui
// обычно не поднят.
func (r *runner) dashboardBusy() bool {
	code, b, err := r.do(http.MethodGet, r.mocks+"/admin/runner/status", nil, 4*time.Second)
	if err == nil && code == http.StatusOK {
		var st struct {
			State string `json:"state"`
		}
		if json.Unmarshal(b, &st) == nil && st.State == "running" {
			return true
		}
	}
	code, b, err = r.do(http.MethodGet, r.mocks+"/admin/run", nil, 4*time.Second)
	if err == nil && code == http.StatusOK {
		var st struct {
			Active bool `json:"runner_active"`
		}
		if json.Unmarshal(b, &st) == nil && st.Active {
			return true
		}
	}
	return false
}

func loadParams(task, override string) (*Params, error) {
	var p Params
	b, err := paramFiles.ReadFile("params/task" + task + ".json")
	if err != nil {
		return nil, err
	}
	if err := strictUnmarshal(b, &p); err != nil {
		return nil, fmt.Errorf("встроенный файл: %w", err)
	}
	if override != "" {
		b, err := os.ReadFile(override)
		if err != nil {
			return nil, err
		}
		if err := strictUnmarshal(b, &p); err != nil {
			return nil, fmt.Errorf("%s: %w", override, err)
		}
	}
	if err := p.validate(task); err != nil {
		return nil, err
	}
	return &p, nil
}

// validate — числа в параметрах осмысленны, иначе прогон бессмыслен.
func (p *Params) validate(task string) error {
	switch {
	case p.BudgetMS <= 0:
		return fmt.Errorf("budget_ms: ожидается > 0")
	case p.RequestTimeoutMS <= 0:
		return fmt.Errorf("request_timeout_ms: ожидается > 0")
	case !(p.WaitS >= 0 && p.WaitS <= maxS):
		return fmt.Errorf("wait_s: ожидается 0…%d", maxS)
	}
	for _, id := range tasks[task] {
		if err := p.scenario(id).validate(id); err != nil {
			return fmt.Errorf("%s: %w", id, err)
		}
	}
	return nil
}

// maxS — предел длительностей в параметрах, с.
const maxS = 3600

func (sp *ScenarioParams) validate(id string) error {
	frac := func(name string, v float64) error {
		if !(v >= 0 && v <= 1) {
			return fmt.Errorf("%s: ожидается доля 0…1", name)
		}
		return nil
	}
	dur := func(name string, v float64) error {
		if !(v >= 0 && v <= maxS) {
			return fmt.Errorf("%s: ожидается 0…%d с", name, maxS)
		}
		return nil
	}
	pm := sp.Payment
	for _, err := range []error{
		frac("payment.fail_rate", pm.FailRate),
		frac("payment.decline_rate", pm.DeclineRate),
		frac("min_paid", sp.MinPaid),
		dur("payment.blackhole_s", pm.BlackholeS),
		dur("down_after_s", sp.DownAfterS),
		frac("bus.hang_rate", sp.Bus.HangRate),
		dur("bus.down_s", sp.Bus.DownS),
		dur("restart_s", sp.RestartS),
		frac("parallel_rate", sp.ParallelRate),
	} {
		if err != nil {
			return err
		}
	}
	if sp.Bus.DownAfterS != 0 {
		return fmt.Errorf("bus.down_after_s не задаётся: используйте down_after_s")
	}
	if id == "S8" {
		return sp.validateS8()
	}
	switch {
	case sp.Orders <= 0:
		return fmt.Errorf("orders: ожидается > 0")
	case !(sp.RatePerS > 0):
		return fmt.Errorf("rate_per_s: ожидается > 0")
	case pm.LatencyMS < 0 || pm.LatencyMS > maxS*1000:
		return fmt.Errorf("payment.latency_ms: ожидается 0…%d", maxS*1000)
	case pm.BlackholeAfterS != 0:
		return fmt.Errorf("payment.blackhole_after_s не задаётся: используйте down_after_s")
	case sp.OrdersAfter < 0 || sp.MaxRequestsDown < 0:
		return fmt.Errorf("orders_after и max_requests_down: ожидается ≥ 0")
	case sp.OrdersAfter > 0 && !(sp.AfterRatePerS > 0):
		return fmt.Errorf("after_rate_per_s: ожидается > 0")
	}
	if lr := pm.LostResponse; strings.HasPrefix(lr, "rate:") {
		v, err := strconv.ParseFloat(strings.TrimPrefix(lr, "rate:"), 64)
		if err != nil {
			return fmt.Errorf("payment.lost_response: rate:p, p — число")
		}
		if err := frac("payment.lost_response rate:p", v); err != nil {
			return err
		}
	}
	switch id {
	case "S2":
		if !(sp.MinPaid > 0) {
			return fmt.Errorf("min_paid: ожидается доля > 0")
		}
	case "S4":
		if !(pm.BlackholeS > 0) {
			return fmt.Errorf("payment.blackhole_s: ожидается > 0")
		}
		if send := float64(sp.Orders) / sp.RatePerS; !(send < pm.BlackholeS) {
			return fmt.Errorf("orders/rate_per_s = %.1f с: заказы во время отказа должны уйти быстрее payment.blackhole_s = %.1f с", send, pm.BlackholeS)
		}
		if sp.OrdersAfter <= 0 {
			return fmt.Errorf("orders_after: ожидается > 0")
		}
	case "S6":
		if !(sp.Bus.DownS > 0) {
			return fmt.Errorf("bus.down_s: ожидается > 0")
		}
		if send := float64(sp.Orders) / sp.RatePerS; !(sp.DownAfterS+send < sp.DownAfterS+sp.Bus.DownS) {
			return fmt.Errorf("orders/rate_per_s = %.1f с: заказы должны уйти быстрее bus.down_s = %.1f с", send, sp.Bus.DownS)
		}
	case "S7":
		switch {
		case sp.KillOrders <= 0:
			return fmt.Errorf("kill_orders: ожидается > 0")
		case sp.KillLatencyMS < 1000 || sp.KillLatencyMS > 60000:
			return fmt.Errorf("kill_latency_ms: ожидается 1000…60000")
		case !(sp.RestartS >= 1):
			return fmt.Errorf("restart_s: ожидается ≥ 1")
		case sp.OrdersAfter <= 0:
			return fmt.Errorf("orders_after: ожидается > 0")
		}
	}
	return nil
}

func (sp *ScenarioParams) validateS8() error {
	switch {
	case sp.Events <= 0 || sp.Events > 10000:
		return fmt.Errorf("events: ожидается 1…10000")
	case sp.Users <= 0 || sp.Users > sp.Events:
		return fmt.Errorf("users: ожидается 1…events")
	case sp.MaxCopies < 1 || sp.MaxCopies > 10:
		return fmt.Errorf("max_copies: ожидается 1…10")
	case !(sp.RatePerS > 0 && sp.RatePerS <= 1000):
		return fmt.Errorf("rate_per_s: ожидается 0…1000")
	case sp.KillAfterEvents <= 0 || sp.KillAfterEvents >= sp.Events:
		return fmt.Errorf("kill_after_events: ожидается 1…events-1")
	case !(sp.RestartS >= 1):
		return fmt.Errorf("restart_s: ожидается ≥ 1")
	}
	return nil
}

func strictUnmarshal(b []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

type runner struct {
	out *output
	// prev — id заказов, известные по прошлым сценариям этого прогона: Orders
	// и его база между сценариями не пересоздаются, и работа Orders по старым
	// заказам может продолжаться в следующем сценарии.
	prev       map[string]bool
	adminToken string
	params     *Params
	orders     string
	mocks      string
	client     *http.Client

	// progress — ход прогона для runner -serve (nil в обычном режиме).
	progress func(current string, done []scenarioResult)
	// fromDashboard — прогон из дашборда (runner -serve), иначе make scenarios.
	fromDashboard bool
	// chaosDir — каталог, через который make перезапускает Orders (chaos.go);
	// пусто — перезапуск недоступен (прогон из дашборда).
	chaosDir string
	// aliveEvery — шаг признака жизни для заглушек во время прогона (0 — aliveStep).
	aliveEvery time.Duration
	// ordersWait — сколько ждать Orders перед прогоном (0 — 120 с; в тестах меньше).
	ordersWait time.Duration
	// runID — id прогона для заглушек; started — метка начала поставлена,
	// метки конца ещё не было; label — подпись текущего сценария.
	runID   string
	started atomic.Bool
	labelMu sync.Mutex
	label   string
	// lastResults и lastReport — итог прогона: то же, что в results JSON и
	// HTML-отчёте.
	lastResults map[string]any
	lastReport  *report
}

// ---------- HTTP к заглушкам и Orders ----------

func (r *runner) do(method, url string, body any, timeout time.Duration) (int, []byte, error) {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, url, rd)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if strings.HasPrefix(url, r.mocks+"/admin/") {
		if r.adminToken != "" {
			req.Header.Set("X-Admin-Token", r.adminToken)
		}
		// Изменяющие запросы к /admin/* заглушки принимают только как JSON.
		if method != http.MethodGet {
			req.Header.Set("Content-Type", "application/json")
		}
		// Заглушки по этим заголовкам знают, что идёт прогон и чей он, и не
		// пускают ручные кнопки дашборда и второй прогон (mocks/runlock.go).
		req.Header.Set("X-Stand-Client", "runner")
		req.Header.Set("X-Stand-Run", r.source())
		req.Header.Set("X-Stand-Run-Id", r.runID)
	}
	c := *r.client
	c.Timeout = timeout
	resp, err := c.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	// Ответы Orders — не больше 1 МБ; факты заглушек бывают крупнее.
	limit := int64(1 << 20)
	if strings.HasPrefix(url, r.mocks+"/") {
		limit = 64 << 20
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, limit))
	return resp.StatusCode, b, err
}

func (r *runner) admin(method, path string, body any) error {
	code, b, err := r.do(method, r.mocks+path, body, 5*time.Second)
	if err != nil {
		return err
	}
	if code/100 != 2 {
		return fmt.Errorf("%s %s: %d %s", method, path, code, strings.TrimSpace(string(b)))
	}
	return nil
}

func (r *runner) waitHealthy(name, url string, limit time.Duration) error {
	deadline := time.Now().Add(limit)
	var last string
	for time.Now().Before(deadline) {
		code, _, err := r.do(http.MethodGet, url+"/healthz", nil, 2*time.Second)
		if err == nil && code == http.StatusOK {
			return nil
		}
		if err != nil {
			last = shortErr(err)
		} else {
			last = fmt.Sprintf("ответ %d", code)
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("%s не отвечает на GET /healthz за %v: %s", name, limit, last)
}

// payLog — один запрос к Payment в журнале заглушки.
type payLog struct {
	At          time.Time  `json:"at"`
	OrderID     string     `json:"order_id"`
	Outcome     string     `json:"outcome"`
	Blackhole   bool       `json:"blackhole"`
	Method      string     `json:"method"`
	AmountCents int64      `json:"amount_cents"`
	DoneAt      *time.Time `json:"done_at"`
	// ClientClosed — соединение закрыл клиент (Orders), не дождавшись ответа.
	ClientClosed bool `json:"client_closed"`
}

// invLog — один запрос к Inventory в журнале заглушки.
type invLog struct {
	At      time.Time `json:"at"`
	Method  string    `json:"method"`
	Op      string    `json:"op"`
	OrderID string    `json:"order_id"`
	Outcome string    `json:"outcome"`
}

type Facts struct {
	Payment struct {
		Requests struct {
			Total       int            `json:"total"`
			ByStatus    map[string]int `json:"by_status"`
			ByOrder     map[string]int `json:"by_order"`
			InBlackhole int            `json:"in_blackhole"`
			Log         []payLog       `json:"log"`
		} `json:"requests"`
		GetRequests struct {
			Total       int      `json:"total"`
			InBlackhole int      `json:"in_blackhole"`
			Log         []payLog `json:"log"`
		} `json:"get_requests"`
		Charges struct {
			Total         int              `json:"total"`
			ByOrder       map[string]int   `json:"by_order"`
			AmountByOrder map[string]int64 `json:"amount_by_order"`
		} `json:"charges"`
		Declines struct {
			Total   int            `json:"total"`
			ByOrder map[string]int `json:"by_order"`
		} `json:"declines"`
	} `json:"payment"`
	Inventory struct {
		Requests struct {
			Reserve int `json:"reserve"`
			Release int `json:"release"`
		} `json:"requests"`
		Log          []invLog `json:"log"`
		Reservations struct {
			Held     int               `json:"held"`
			Released int               `json:"released"`
			ByOrder  map[string]string `json:"by_order"`
		} `json:"reservations"`
	} `json:"inventory"`
	Bus busFacts `json:"bus"`
}

// busLog — одна публикация в шину (POST /events).
type busLog struct {
	At      time.Time  `json:"at"`
	ID      string     `json:"id"`
	OrderID string     `json:"order_id"`
	Outcome string     `json:"outcome"`
	Down    bool       `json:"down"`
	Dup     bool       `json:"dup"`
	DoneAt  *time.Time `json:"done_at"`
}

// dlvEvent — событие delivery.confirmed, которое шина доставляет в Orders.
type dlvEvent struct {
	ID       string `json:"id"`
	UserID   string `json:"user_id"`
	Points   int    `json:"bonus_points"`
	Copies   int    `json:"copies"`
	Acked    int    `json:"acked"`
	Attempts int    `json:"attempts"`
}

type busFacts struct {
	Requests struct {
		Total    int            `json:"total"`
		ByStatus map[string]int `json:"by_status"`
		InDown   int            `json:"in_down"`
	} `json:"requests"`
	Accepted      int                 `json:"accepted"`
	EventsByOrder map[string][]string `json:"events_by_order"`
	Log           []busLog            `json:"log"`
	Delivery      *struct {
		Events         []dlvEvent     `json:"events"`
		Pending        int            `json:"pending"`
		AckedEvents    int            `json:"acked_events"`
		Attempts       int            `json:"attempts"`
		ExpectedByUser map[string]int `json:"expected_by_user"`
	} `json:"delivery"`
}

func (r *runner) facts() (*Facts, error) {
	code, b, err := r.do(http.MethodGet, r.mocks+"/admin/facts", nil, 5*time.Second)
	if err != nil {
		return nil, err
	}
	if code != http.StatusOK {
		return nil, fmt.Errorf("GET /admin/facts: %d", code)
	}
	var f Facts
	if err := json.Unmarshal(b, &f); err != nil {
		return nil, err
	}
	return &f, nil
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// failRun — прогон не начался: пишет результаты с ошибкой (если задан путь)
// и выходит с кодом 2.
func failRun(resultsPath, task string, total int, msg string) {
	o := newOutput(os.Stdout)
	o.printf("СТЕНД: %s\n", msg)
	writeResults(resultsPath, map[string]any{
		"task": task, "error": clean(msg), "passed": 0, "total": total,
		"scenarios": []scenarioResult{}, "level": "",
	}, o, &report{Task: task, RunAt: time.Now(), Error: clean(msg), Total: total})
	os.Exit(2)
}
