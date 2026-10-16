package main

// «Идёт прогон раннера» — ручные кнопки дашборда и второй прогон не должны
// вмешиваться в прогон сценариев: «Очистить журнал» посреди S1 обнулила бы
// факты, ручные режимы, заказы и чужой прогон испортили бы проверку.
//
// Кто прислал admin-запрос — по заголовкам:
//
//	X-Stand-Client: runner — раннер (make scenarios и прогон из дашборда,
//	                         runner -serve); X-Stand-Run: cli | dashboard —
//	                         откуда прогон, X-Stand-Run-Id — id прогона;
//	X-Stand-Client: ui     — страница дашборда (/ui);
//	без X-Stand-Client     — ручной curl с токеном: осознанное действие,
//	                         не блокируется.
//
// Схема. У прогона есть владелец — id и источник прогона раннера.
//   - Начало: раннер сразу после проверки стенда шлёт POST /admin/run/start.
//     Сброс и режимы без finished от раннера тоже делают его владельцем, если
//     прогона нет (заглушки перезапущены посреди прогона).
//   - Конец: PUT /admin/modes с finished: true от владельца — раннер ставит эту
//     метку в конце прогона, при ошибке стенда и при прерывании (Ctrl+C,
//     SIGTERM). Метка чужого прогона не снимает блокировку.
//   - Признак жизни: любой admin-запрос владельца; во время долгих пауз
//     (ожидание после заказов, окно отказа S4) раннер раз в 20 с шлёт лёгкий
//     GET /admin/modes.
//   - Страховка: runnerIdle без запросов владельца (раннер убит и метку конца
//     не поставил) — прогон считается завершённым. 150 с — с большим запасом
//     над шагом 20 с и больше самой длинной паузы внутри сценария даже без
//     признака жизни (S4: ~60 с окна + 30 с ожидания).
//
// Пока прогон идёт:
//   - ручные действия страницы (режимы, сброс, заказы, проверка статусов,
//     «Прогнать») — 409 runner_active;
//   - начало, сброс и режимы от другого раннера (второй make scenarios) —
//     409 runner_active;
//   - GET /admin/run и /admin/events отдают runner_active и runner_source
//     (источник владельца).
// Начало прогона отменяет ручную отправку заказов с дашборда.

import (
	"net/http"
	"strings"
	"time"
)

// runnerIdle — через сколько без запросов владельца прогон считается
// завершённым (раннер убит, метку конца не поставил).
const runnerIdle = 150 * time.Second

// runnerBusyMsg — ответ 409 на ручное действие со страницы во время прогона.
const runnerBusyMsg = "Идёт прогон сценариев (make scenarios или из дашборда) — ручные действия недоступны до его конца"

// otherRunMsg — ответ 409 второму раннеру.
const otherRunMsg = "Идёт другой прогон сценариев (make scenarios или из дашборда): оба ходят в одни и те же заглушки. Дождитесь его конца"

const (
	hdrClient = "X-Stand-Client"
	hdrRun    = "X-Stand-Run"
	hdrRunID  = "X-Stand-Run-Id"
)

// runOwner — владелец текущего прогона (под s.mu).
type runOwner struct {
	id     string
	source string // cli | dashboard | ""
	active bool
	seen   time.Time // последний admin-запрос владельца
}

func (s *state) clock() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

func isRunner(r *http.Request) bool { return r.Header.Get(hdrClient) == "runner" }
func isUI(r *http.Request) bool     { return r.Header.Get(hdrClient) == "ui" }

func runID(r *http.Request) string {
	id := strings.TrimSpace(r.Header.Get(hdrRunID))
	if len(id) > 64 {
		id = id[:64]
	}
	return id
}

func runSource(r *http.Request) string {
	switch v := r.Header.Get(hdrRun); v {
	case "cli", "dashboard":
		return v
	}
	return ""
}

// runnerActiveLocked — идёт ли прогон; источник — у владельца.
func (s *state) runnerActiveLocked() (bool, string) {
	o := s.owner
	if !o.active || s.clock().Sub(o.seen) > runnerIdle {
		return false, ""
	}
	return true, o.source
}

// noteRunner — admin-запрос раннера: владелец жив. Запросы чужого раннера
// прогон не продлевают.
func (s *state) noteRunner(r *http.Request) {
	s.mu.Lock()
	if active, _ := s.runnerActiveLocked(); active && s.owner.id == runID(r) {
		s.owner.seen = s.clock()
	}
	s.mu.Unlock()
}

// runnerGateLocked — изменяющий запрос раннера (начало, сброс, режимы):
// false — идёт чужой прогон. finish — метка конца: снимает блокировку, только
// если прогон свой. Иначе раннер становится владельцем; новый прогон
// отменяет ручную отправку заказов.
func (s *state) runnerGateLocked(r *http.Request, finish bool) bool {
	id := runID(r)
	active, _ := s.runnerActiveLocked()
	if active && s.owner.id != id {
		return false
	}
	if finish {
		s.owner.active = false
		return true
	}
	if !active {
		s.stopSendingLocked(stopRunner)
	}
	src := runSource(r)
	if active && src == "" {
		src = s.owner.source
	}
	s.owner = runOwner{id: id, source: src, active: true, seen: s.clock()}
	return true
}

// manual — ручное действие: со страницы во время прогона — 409. Запросы
// раннера (их проверяет runnerGateLocked) и без X-Stand-Client (curl)
// проходят.
func (s *state) manual(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if isUI(r) {
			s.mu.Lock()
			busy, _ := s.runnerActiveLocked()
			s.mu.Unlock()
			if busy {
				writeErr(w, http.StatusConflict, "runner_active", runnerBusyMsg)
				return
			}
		}
		h(w, r)
	}
}

// handleRunStart — POST /admin/run/start: раннер начинает прогон (сразу после
// проверки стенда, до ожидания Orders). 409 — идёт другой прогон.
func (s *state) handleRunStart(w http.ResponseWriter, r *http.Request) {
	if !isRunner(r) {
		writeErr(w, http.StatusBadRequest, "bad_request", "только для раннера (X-Stand-Client: runner)")
		return
	}
	s.mu.Lock()
	ok := s.runnerGateLocked(r, false)
	s.mu.Unlock()
	if !ok {
		writeErr(w, http.StatusConflict, "runner_active", otherRunMsg)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type runView struct {
	RunnerActive bool   `json:"runner_active"`
	RunnerSource string `json:"runner_source,omitempty"`
}

// handleRunStatus — GET /admin/run: идёт ли прогон (для make и раннера).
func (s *state) handleRunStatus(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	var v runView
	v.RunnerActive, v.RunnerSource = s.runnerActiveLocked()
	s.mu.Unlock()
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, v)
}
