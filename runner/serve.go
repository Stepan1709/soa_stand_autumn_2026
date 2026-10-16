package main

// Режим сервиса (runner -serve :8081, сервис runner-ui, make ui): прогоны
// сценариев по кнопке с дашборда. Сам прогон — тот же r.run, что и в
// make scenarios; сервис только запускает его в горутине и отдаёт ход и итог.
//
//	POST /run {"task": "2", "ids": ["S1"], "wait_s": 30}
//	                     — запустить прогон (409, если идёт); без task —
//	                       задание 1, без ids — все сценарии задания,
//	                       доступные из дашборда. Старый вид
//	                       {"only": "S1,S5", "wait": 30} тоже принимается.
//	GET  /status         — ход и итог последнего прогона (с полем task)
//	GET  /report?task=N  — HTML-отчёт последнего прогона задания N; без task —
//	                       задания последнего прогона (до первого прогона —
//	                       задания из -task); нет в памяти (runner-ui
//	                       перезапущен) — results/ui_task_N.html с диска
//	GET  /params?task=N  — открытые параметры сценариев задания N, которые
//	                       можно запустить из дашборда (без task — -task)
//
// S7 и S8 перезапускают Orders через runner/chaos.sh на хосте: из дашборда
// они не запускаются никогда, только make scenarios TASK=2.
//
// Результаты — в -results с подстановкой {task} вместо номера задания
// (/results/ui_task_{task}.json): файлы task_N.* от make scenarios не
// затираются.
//
// Пускает только с токеном MOCKS_ADMIN_TOKEN и (с -allow-from) только с
// адресов заглушек в admin-сети: сервис Orders сюда не достучится.

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"
)

type serveConfig struct {
	addr, task, paramsFile, results string
	orders, mocks                   string
	allowFrom, token                string
}

type server struct {
	cfg   serveConfig
	allow *allowList

	mu         sync.Mutex
	state      string // idle | running | done
	task       string // задание текущего или последнего прогона
	ids        []string
	waitS      float64
	current    string
	startedAt  time.Time
	finishedAt time.Time
	done       []scenarioResult
	out        *output
	results    map[string]any
	rep        *report
	reps       map[string]*report // последний отчёт каждого задания
	cur        *runner            // текущий прогон (для метки конца по сигналу)
}

func serve(cfg serveConfig) int {
	if _, ok := tasks[cfg.task]; !ok {
		log.Printf("runner -serve: неизвестное задание %q", cfg.task)
		return 2
	}
	for task := range dashboardIDs {
		if _, err := loadParams(task, cfg.paramsFile); err != nil {
			log.Printf("runner -serve: параметры задания %s: %v", task, err)
			return 2
		}
	}
	s := &server{cfg: cfg, state: "idle"}
	if cfg.allowFrom != "" {
		s.allow = &allowList{name: cfg.allowFrom}
	}
	srv := &http.Server{
		Addr:              cfg.addr,
		Handler:           s.routes(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	// SIGTERM (make down) посреди прогона: метка конца прогона сразу, иначе
	// ручные кнопки дашборда ждали бы страховочные 150 с.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		s.mu.Lock()
		rn := s.cur
		s.mu.Unlock()
		if rn != nil {
			rn.endRun("прогон прерван")
		}
		os.Exit(130)
	}()
	log.Printf("runner -serve: слушаю %s", cfg.addr)
	if err := srv.ListenAndServe(); err != nil {
		log.Printf("runner -serve: %v", err)
	}
	return 2
}

func (s *server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /run", s.guard(s.handleRun))
	mux.HandleFunc("GET /status", s.guard(s.handleStatus))
	mux.HandleFunc("GET /report", s.guard(s.handleReport))
	mux.HandleFunc("GET /params", s.guard(s.handleParams))
	return mux
}

// guard — токен и адрес отправителя.
func (s *server) guard(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.allow != nil && !s.allow.allowed(r.RemoteAddr) {
			writeJSONResp(w, http.StatusForbidden, apiErr("forbidden", "только через заглушки (дашборд)"))
			return
		}
		if s.cfg.token != "" && subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Admin-Token")), []byte(s.cfg.token)) != 1 {
			writeJSONResp(w, http.StatusForbidden, apiErr("forbidden", "нужен заголовок X-Admin-Token"))
			return
		}
		h(w, r)
	}
}

// dashboardIDs — сценарии, которые можно запустить из дашборда. S7 и S8
// перезапускают Orders через runner/chaos.sh на хосте — только make.
var dashboardIDs = map[string][]string{
	"1": tasks["1"],
	"2": {"S1", "S6"},
}

const msgChaosOnly = "S7 и S8 перезапускают Orders и запускаются только через make scenarios TASK=2"

// resultsPath — файл результатов задания: {task} в -results заменяется
// номером; без {task} путь годен только для задания из -task, для прочих —
// ui_task_N.json рядом с ним.
func (s *server) resultsPath(task string) string {
	p := s.cfg.results
	switch {
	case p == "":
		return ""
	case strings.Contains(p, "{task}"):
		return strings.ReplaceAll(p, "{task}", task)
	case task == s.cfg.task:
		return p
	}
	return filepath.Join(filepath.Dir(p), "ui_task_"+task+".json")
}

// queryTask — задание из ?task=N; def, если не задано.
func queryTask(r *http.Request, def string) (string, bool) {
	t := strings.TrimSpace(r.URL.Query().Get("task"))
	if t == "" {
		return def, true
	}
	_, ok := dashboardIDs[t]
	return t, ok
}

type runRequest struct {
	Task  string   `json:"task"`
	IDs   []string `json:"ids"`
	WaitS *float64 `json:"wait_s"`
	// Старый вид запроса.
	Only string   `json:"only"`
	Wait *float64 `json:"wait"`
}

// checkIDs — сценарии в верхнем регистре или текст ошибки.
func checkIDs(task string, ids []string) ([]string, string) {
	var out []string
	for _, id := range ids {
		id = strings.ToUpper(strings.TrimSpace(id))
		if id != "" {
			out = append(out, id)
		}
	}
	if len(out) == 0 {
		return nil, "нет сценариев"
	}
	for _, id := range out {
		if id == "S7" || id == "S8" {
			return nil, msgChaosOnly
		}
	}
	for _, id := range out {
		if !slices.Contains(dashboardIDs[task], id) {
			return nil, fmt.Sprintf("неизвестный сценарий %q", clean(id))
		}
	}
	return out, ""
}

func (s *server) handleRun(w http.ResponseWriter, r *http.Request) {
	var req runRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<12))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeJSONResp(w, http.StatusBadRequest, apiErr("bad_request", `ожидается {"task": "1", "ids": ["S1", "S5"], "wait_s": 30}`))
		return
	}
	task := strings.TrimSpace(req.Task)
	if task == "" {
		task = "1"
	}
	if _, ok := dashboardIDs[task]; !ok {
		writeJSONResp(w, http.StatusBadRequest, apiErr("bad_request", fmt.Sprintf("неизвестное задание %q", clean(task))))
		return
	}
	raw := dashboardIDs[task]
	switch {
	case len(req.IDs) > 0:
		raw = req.IDs
	case req.Only != "":
		raw = splitOnly(req.Only)
	}
	ids, msg := checkIDs(task, raw)
	if msg != "" {
		writeJSONResp(w, http.StatusBadRequest, apiErr("bad_request", msg))
		return
	}
	params, err := loadParams(task, s.cfg.paramsFile)
	if err != nil {
		writeJSONResp(w, http.StatusInternalServerError, apiErr("params", err.Error()))
		return
	}
	if bad := unknownID(params, ids); bad != "" {
		writeJSONResp(w, http.StatusBadRequest, apiErr("bad_request", fmt.Sprintf("неизвестный сценарий %q", clean(bad))))
		return
	}
	wait := req.WaitS
	if wait == nil {
		wait = req.Wait
	}
	if wait != nil {
		if !validWait(*wait) {
			writeJSONResp(w, http.StatusBadRequest, apiErr("bad_request", "wait_s: ожидается число секунд 0…3600"))
			return
		}
		params.WaitS = *wait
	}

	s.mu.Lock()
	if s.state == "running" {
		s.mu.Unlock()
		writeJSONResp(w, http.StatusConflict, apiErr("busy", "прогон уже идёт"))
		return
	}
	out := newOutput(os.Stdout)
	rn := newRunner(params, s.cfg.orders, s.cfg.mocks, s.cfg.token, out)
	rn.fromDashboard = true
	rn.progress = func(current string, done []scenarioResult) {
		s.mu.Lock()
		s.current, s.done = current, done
		s.mu.Unlock()
	}
	s.state, s.task, s.ids, s.waitS, s.current = "running", task, ids, params.WaitS, ""
	s.startedAt, s.finishedAt = time.Now(), time.Time{}
	s.done, s.out, s.results, s.rep = nil, out, nil, nil
	s.cur = rn
	st := s.statusLocked()
	s.mu.Unlock()

	go s.execute(rn, task, ids)
	writeJSONResp(w, http.StatusAccepted, st)
}

func (s *server) execute(rn *runner, task string, ids []string) {
	defer func() {
		if p := recover(); p != nil {
			rn.out.printf("СТЕНД: прогон прерван: %v\n", p)
			rn.endRun("прогон прерван")
		}
		rn.client.CloseIdleConnections()
		s.mu.Lock()
		s.state, s.current, s.finishedAt = "done", "", time.Now()
		s.results, s.rep = rn.lastResults, rn.lastReport
		if rn.lastReport != nil {
			if s.reps == nil {
				s.reps = map[string]*report{}
			}
			s.reps[task] = rn.lastReport
		}
		s.cur = nil
		s.mu.Unlock()
	}()
	rn.run(task, ids, s.resultsPath(task))
}

type scenarioStatus struct {
	ID    string `json:"id"`
	Title string `json:"title"`
	State string `json:"state"` // pending | running | pass | fail
	Error string `json:"error,omitempty"`
}

type runStatus struct {
	State      string           `json:"state"`
	Task       string           `json:"task"`
	Current    string           `json:"current,omitempty"`
	StartedAt  *time.Time       `json:"started_at,omitempty"`
	FinishedAt *time.Time       `json:"finished_at,omitempty"`
	WaitS      float64          `json:"wait_s"`
	Scenarios  []scenarioStatus `json:"scenarios"`
	Text       string           `json:"text"`
	Results    map[string]any   `json:"results"`
	Level      string           `json:"level,omitempty"`
	Passed     int              `json:"passed"`
	Total      int              `json:"total"`
	Report     bool             `json:"report"`
}

func (s *server) statusLocked() runStatus {
	task := s.task
	if task == "" {
		task = s.cfg.task
	}
	st := runStatus{State: s.state, Task: task, Current: s.current, WaitS: s.waitS, Scenarios: []scenarioStatus{},
		Results: s.results, Total: len(s.ids), Report: s.rep != nil}
	if !s.startedAt.IsZero() {
		t := s.startedAt
		st.StartedAt = &t
	}
	if !s.finishedAt.IsZero() {
		t := s.finishedAt
		st.FinishedAt = &t
	}
	if s.out != nil {
		st.Text = s.out.text()
	}
	byID := map[string]scenarioResult{}
	for _, res := range s.done {
		byID[res.ID] = res
	}
	for _, id := range s.ids {
		ss := scenarioStatus{ID: id, State: "pending"}
		if def := scenarioByID(id); def != nil {
			ss.Title = def.Title
		}
		if res, ok := byID[id]; ok {
			ss.State, ss.Error = "fail", res.Error
			if res.Pass {
				ss.State = "pass"
				st.Passed++
			}
		} else if id == s.current && s.state == "running" {
			ss.State = "running"
		}
		st.Scenarios = append(st.Scenarios, ss)
	}
	if s.results != nil {
		if lvl, ok := s.results["level"].(string); ok {
			st.Level = lvl
		}
	}
	return st
}

func (s *server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	st := s.statusLocked()
	s.mu.Unlock()
	writeJSONResp(w, http.StatusOK, st)
}

func (s *server) handleReport(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	def := s.task
	if def == "" {
		def = s.cfg.task
	}
	task, ok := queryTask(r, def)
	rep := s.reps[task]
	s.mu.Unlock()
	if !ok {
		writeJSONResp(w, http.StatusBadRequest, apiErr("bad_request", fmt.Sprintf("неизвестное задание %q", clean(task))))
		return
	}
	var b bytes.Buffer
	if rep == nil {
		if page := s.reportFile(task); page != nil {
			writeHTML(w, page)
			return
		}
		writeJSONResp(w, http.StatusNotFound, apiErr("no_report", "отчёта ещё нет: прогон не запускался или не закончился"))
		return
	}
	if err := renderReport(&b, rep); err != nil {
		writeJSONResp(w, http.StatusInternalServerError, apiErr("report", err.Error()))
		return
	}
	writeHTML(w, b.Bytes())
}

// reportFile — HTML-отчёт задания с диска (рядом с файлом результатов) или nil.
func (s *server) reportFile(task string) []byte {
	p := s.resultsPath(task)
	if p == "" {
		return nil
	}
	page := strings.TrimSuffix(p, filepath.Ext(p)) + ".html"
	b, err := os.ReadFile(page)
	if err != nil || len(b) == 0 {
		return nil
	}
	return b
}

func writeHTML(w http.ResponseWriter, page []byte) {
	h := w.Header()
	h.Set("Content-Type", "text/html; charset=utf-8")
	h.Set("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'; img-src data:")
	h.Set("X-Content-Type-Options", "nosniff")
	_, _ = w.Write(page)
}

// publicScenario — открытые параметры сценария для кнопок «как в S1…S5».
type publicScenario struct {
	Title         string       `json:"title"`
	Orders        int          `json:"orders"`
	RatePerS      float64      `json:"rate_per_s"`
	Payment       PaymentModes `json:"payment"`
	OrdersAfter   int          `json:"orders_after,omitempty"`
	AfterRatePerS float64      `json:"after_rate_per_s,omitempty"`
	Bus           *BusModes    `json:"bus,omitempty"`
}

func (s *server) handleParams(w http.ResponseWriter, r *http.Request) {
	task, ok := queryTask(r, s.cfg.task)
	if !ok {
		writeJSONResp(w, http.StatusBadRequest, apiErr("bad_request", fmt.Sprintf("неизвестное задание %q", clean(task))))
		return
	}
	p, err := loadParams(task, s.cfg.paramsFile)
	if err != nil {
		writeJSONResp(w, http.StatusInternalServerError, apiErr("params", err.Error()))
		return
	}
	out := map[string]publicScenario{}
	for _, id := range dashboardIDs[task] {
		sp, def := p.scenario(id), scenarioByID(id)
		pm := sp.Payment
		if id == "S4" {
			pm.BlackholeAfterS = sp.DownAfterS
		}
		ps := publicScenario{Title: def.Title, Orders: sp.Orders, RatePerS: sp.RatePerS, Payment: pm,
			OrdersAfter: sp.OrdersAfter, AfterRatePerS: sp.AfterRatePerS}
		if sp.Bus != (BusModes{}) {
			bus := sp.Bus
			bus.DownAfterS = sp.DownAfterS
			ps.Bus = &bus
		}
		out[id] = ps
	}
	writeJSONResp(w, http.StatusOK, map[string]any{"task": task, "ids": dashboardIDs[task],
		"wait_s": p.WaitS, "budget_ms": p.BudgetMS, "scenarios": out})
}

// allowList — адреса, которым можно: loopback и адреса имени name (в
// compose — mocks-admin, имя есть только в admin-сети). Удачный ответ DNS
// кэшируется на 5 с, неудача — на 1 с; DNS опрашивается вне лока: медленный
// резолвер не держит остальные запросы.
type allowList struct {
	name   string
	lookup func(ctx context.Context, host string) ([]string, error) // nil — системный резолвер

	mu       sync.Mutex
	ips      map[string]bool
	until    time.Time // до какого момента кэш годен
	resolved time.Time // когда последний раз резолвили
}

func (a *allowList) allowed(remote string) bool {
	host, _, err := net.SplitHostPort(remote)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	if ip.IsLoopback() {
		return true
	}
	key := ip.String()
	a.mu.Lock()
	now := time.Now()
	// Чужой адрес не заставляет резолвить имя на каждый запрос: не чаще раза в
	// секунду. Кто первым взялся резолвить — резолвит, остальные берут кэш.
	need := now.After(a.until) || (!a.ips[key] && now.Sub(a.resolved) > time.Second)
	if need {
		a.resolved = now
	}
	ok := a.ips[key]
	a.mu.Unlock()
	if !need {
		return ok
	}

	lookup := a.lookup
	if lookup == nil {
		lookup = net.DefaultResolver.LookupHost
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	addrs, err := lookup(ctx, a.name)
	cancel()

	a.mu.Lock()
	defer a.mu.Unlock()
	if err != nil {
		// Неудача кэшируется на 1 с; прежние адреса остаются.
		a.until = time.Now().Add(time.Second)
		return a.ips[key]
	}
	a.ips = map[string]bool{}
	for _, s := range addrs {
		if p := net.ParseIP(s); p != nil {
			a.ips[p.String()] = true
		}
	}
	a.until = time.Now().Add(5 * time.Second)
	return a.ips[key]
}

func apiErr(code, msg string) map[string]string {
	return map[string]string{"error": code, "message": msg}
}

func writeJSONResp(w http.ResponseWriter, code int, v any) {
	h := w.Header()
	h.Set("Content-Type", "application/json")
	h.Set("Cache-Control", "no-store")
	h.Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
