package main

// Прогон и заглушки: метки начала и конца прогона, признак жизни. По ним
// заглушки знают, что идёт прогон и чей он, и не пускают ручные кнопки
// дашборда и второй прогон (схема — в mocks/runlock.go).

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net/http"
	"time"
)

func newRunID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// source — откуда прогон, для заголовка X-Stand-Run.
func (r *runner) source() string {
	if r.fromDashboard {
		return "dashboard"
	}
	return "cli"
}

func (r *runner) setLabel(l string) {
	r.labelMu.Lock()
	r.label = l
	r.labelMu.Unlock()
}

// errOtherRun — заглушки заняты чужим прогоном.
var errOtherRun = errors.New("идёт другой прогон сценариев (make scenarios или из дашборда): оба прогона ходят в одни и те же заглушки. Дождитесь его конца и запустите снова")

// startRun — метка начала прогона (POST /admin/run/start). Старые заглушки
// без этого пути (404) — прогон идёт без метки начала: её поставит первый
// сброс; метка конца ставится всё равно.
func (r *runner) startRun() error {
	code, _, err := r.do(http.MethodPost, r.mocks+"/admin/run/start", nil, 5*time.Second)
	switch {
	case err != nil:
		return err
	case code == http.StatusConflict:
		return errOtherRun
	}
	r.started.Store(true)
	return nil
}

// endRun — метка конца прогона: отказы выключены, подпись «… · what»,
// finished. Только один раз и только если начало отмечено. Журнал последнего
// сценария остаётся в заглушках: его можно разобрать на дашборде.
func (r *runner) endRun(what string) {
	if !r.started.CompareAndSwap(true, false) {
		return
	}
	r.labelMu.Lock()
	label := r.label
	r.labelMu.Unlock()
	if label != "" {
		label += " · "
	}
	_ = r.admin(http.MethodPut, "/admin/modes", map[string]any{"seed": r.params.Seed, "payment": map[string]any{},
		"label": label + what, "finished": true})
}

// aliveStep — как часто раннер напоминает заглушкам, что прогон идёт: во
// время долгих пауз (ожидание после заказов, окно отказа S4) других
// admin-запросов нет. Заглушки считают прогон брошенным через 150 с без
// запросов раннера.
const aliveStep = 20 * time.Second

// keepAlive раз в aliveStep шлёт лёгкий GET /admin/modes, пока не вызвана
// возвращённая функция остановки.
func (r *runner) keepAlive() (stop func()) {
	step := r.aliveEvery
	if step <= 0 {
		step = aliveStep
	}
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		t := time.NewTicker(step)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				_, _, _ = r.do(http.MethodGet, r.mocks+"/admin/modes", nil, 5*time.Second)
			}
		}
	}()
	return func() {
		close(done)
		<-finished
	}
}
