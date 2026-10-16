package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type chargeRequest struct {
	OrderID     string `json:"order_id"`
	AmountCents int64  `json:"amount_cents"`
	Currency    string `json:"currency"`
}

// handleCharge — POST /payments.
func (s *state) handleCharge(w http.ResponseWriter, r *http.Request) {
	var body chargeRequest
	decodeErr := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&body)
	key := r.Header.Get("Idempotency-Key")
	now := time.Now()

	s.mu.Lock()
	entry := reqLog{At: now, OrderID: body.OrderID, Method: http.MethodPost, AmountCents: body.AmountCents}
	ev := event{At: now, Service: "payment", Method: http.MethodPost, Path: "/payments",
		OrderID: body.OrderID, AmountCents: body.AmountCents}

	// Во время отказа Payment не отвечает ни на какой запрос, даже кривой.
	if s.inBlackholeLocked(now) {
		entry.Outcome = "blackhole"
		entry.Blackhole = true
		wt := s.logChargeLocked(entry, ev, true)
		s.mu.Unlock()
		gone := s.hangWhile(w, r, s.inBlackholeLocked)
		s.finish(wt, "no_response", gone)
		if !gone {
			closeWithoutResponse(w)
		}
		return
	}

	if decodeErr != nil || body.OrderID == "" || body.AmountCents <= 0 || body.Currency == "" {
		entry.Outcome = "400"
		s.logChargeLocked(entry, ev, false)
		s.mu.Unlock()
		writeErr(w, http.StatusBadRequest, "bad_request", "ожидается {order_id, amount_cents > 0, currency}")
		return
	}

	// Случайные величины: 500 и потеря ответа — из генератора (seed, n) в
	// фиксированном порядке, исход зависит от seed и номера запроса; отказ
	// банка — от seed и order_id, у всех запросов по заказу он один.
	n := s.n
	s.n++
	rnd := s.rng(n)
	failDraw, lostDraw := rnd.Float64(), rnd.Float64()
	declineDraw := s.orderDraw(body.OrderID)
	pm := s.modes.Payment
	latency := time.Duration(pm.LatencyMS) * time.Millisecond

	var (
		code      int
		resp      any
		ch        *charge
		newCharge bool
	)
	switch {
	case failDraw < pm.FailRate:
		code, resp = http.StatusInternalServerError, map[string]string{"error": "internal", "message": "внутренняя ошибка"}

	case key != "" && s.keys[key] != nil:
		k := s.keys[key]
		switch {
		case k.body != body:
			code, resp = http.StatusUnprocessableEntity, map[string]string{
				"error": "idempotency_key_reused", "message": "ключ уже использован с другим телом запроса"}
		case k.declined:
			code, resp = http.StatusPaymentRequired, map[string]string{"error": "declined", "message": "банк отклонил оплату"}
		default:
			code, resp, ch = http.StatusOK, *k.charge, k.charge
		}

	case declineDraw < pm.DeclineRate:
		code, resp = http.StatusPaymentRequired, map[string]string{"error": "declined", "message": "банк отклонил оплату"}
		if key != "" {
			s.keys[key] = &keyed{body: body, declined: true}
		}

	default:
		s.seq++
		c := charge{
			ChargeID:    fmt.Sprintf("ch_%06d", s.seq),
			OrderID:     body.OrderID,
			AmountCents: body.AmountCents,
			Currency:    body.Currency,
			Status:      "captured",
		}
		s.charges = append(s.charges, c)
		ch = &s.charges[len(s.charges)-1]
		if key != "" {
			cc := c
			s.keys[key] = &keyed{body: body, charge: &cc}
		}
		code, resp, newCharge = http.StatusCreated, c, true
	}

	lose := false
	if ch != nil {
		switch {
		case s.lost.firstPerOrder && !s.lostOnce[body.OrderID]:
			lose = true
		case s.lost.rate > 0 && lostDraw < s.lost.rate:
			lose = true
		}
		if lose {
			s.lostOnce[body.OrderID] = true
		}
	}
	if lose {
		entry.Outcome = "lost"
	} else {
		entry.Outcome = fmt.Sprint(code)
	}
	entry.charged, ev.Charged = newCharge, newCharge
	wt := s.logChargeLocked(entry, ev, latency > 0)
	s.mu.Unlock()

	if latency > 0 {
		// Операция уже выполнена; если клиент ушёл раньше, чем заглушка
		// ответила, это видно в фактах и на дашборде.
		t := time.NewTimer(latency)
		select {
		case <-t.C:
			s.finish(wt, entry.Outcome, false)
		case <-r.Context().Done():
			t.Stop()
			s.finish(wt, entry.Outcome, true)
			return
		}
	}
	if lose {
		closeWithoutResponse(w)
		return
	}
	writeJSON(w, code, resp)
}

// logChargeLocked записывает POST /payments в журнал фактов и в ленту.
// wait — ответ задержится: конец отметит s.finish(возвращённое).
func (s *state) logChargeLocked(entry reqLog, ev event, wait bool) waiting {
	entry.wait = wait
	s.reqs = append(s.reqs, entry)
	ev.Outcome, ev.Wait = entry.Outcome, wait
	return waiting{run: s.run, seq: s.addEventLocked(ev), idx: len(s.reqs) - 1}
}

// logGetLocked — то же для GET /payments?order_id=.
func (s *state) logGetLocked(entry getLog, wait bool) waiting {
	s.gets = append(s.gets, entry)
	seq := s.addEventLocked(event{At: entry.At, Service: "payment", Method: http.MethodGet, Path: "/payments",
		OrderID: entry.OrderID, Outcome: entry.Outcome, Wait: wait})
	return waiting{run: s.run, seq: seq, idx: len(s.gets) - 1, get: true}
}

// handleLookup — GET /payments?order_id=.
func (s *state) handleLookup(w http.ResponseWriter, r *http.Request) {
	orderID := r.URL.Query().Get("order_id")
	now := time.Now()
	s.mu.Lock()
	entry := getLog{At: now, OrderID: orderID, Method: http.MethodGet}
	if s.inBlackholeLocked(now) {
		entry.Outcome, entry.Blackhole = "blackhole", true
		wt := s.logGetLocked(entry, true)
		s.mu.Unlock()
		gone := s.hangWhile(w, r, s.inBlackholeLocked)
		s.finish(wt, "no_response", gone)
		if !gone {
			closeWithoutResponse(w)
		}
		return
	}
	if orderID == "" {
		entry.Outcome = "400"
		s.logGetLocked(entry, false)
		s.mu.Unlock()
		writeErr(w, http.StatusBadRequest, "bad_request", "нужен параметр order_id")
		return
	}
	entry.Outcome = "200"
	s.logGetLocked(entry, false)
	out := []charge{}
	for _, c := range s.charges {
		if c.OrderID == orderID {
			out = append(out, c)
		}
	}
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, out)
}

// hangWhile держит соединение без ответа, пока длится отказ (down — Payment
// или шины) и клиент не ушёл. true — соединение закрыл клиент.
func (s *state) hangWhile(w http.ResponseWriter, r *http.Request, down func(time.Time) bool) bool {
	s.mu.Lock()
	epoch := s.epoch
	s.mu.Unlock()
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-r.Context().Done():
			return true
		case now := <-t.C:
			s.mu.Lock()
			still := s.epoch == epoch && down(now)
			s.mu.Unlock()
			if !still {
				// Сначала отметка в фактах, потом закрытие: клиент не увидит
				// закрытия раньше, чем оно попадёт в журнал.
				return false
			}
		}
	}
}

func closeWithoutResponse(w http.ResponseWriter) {
	hj, ok := w.(http.Hijacker)
	if !ok {
		panic(http.ErrAbortHandler)
	}
	conn, _, err := hj.Hijack()
	if err != nil {
		panic(http.ErrAbortHandler)
	}
	_ = conn.Close()
}
