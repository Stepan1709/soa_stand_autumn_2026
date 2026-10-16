package main

// Балансы бонусов с дашборда. POST /admin/bus/balances спрашивает Orders
// GET /users/{id}/bonus по каждому покупателю, которому с reset доставлялись
// события delivery.confirmed, и по покупателю без начислений (u-0).
// expected — сумма bonus_points по всем партиям (s.bonusExpected). Адрес Orders — только из
// ORDERS_URL, клиент — ordersClient (без редиректов).

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// bonusUnknownUser — покупатель без начислений: баланс должен быть 0.
const bonusUnknownUser = "u-0"

// bonusWait — сколько ждать ответа на один GET /users/{id}/bonus.
const bonusWait = 3 * time.Second

// balancesWait — общий предел одной проверки балансов.
var balancesWait = 20 * time.Second

type balanceView struct {
	UserID   string `json:"user_id"`
	Expected int    `json:"expected"`
	// Points — баланс из ответа Orders; null — ответа с числом нет (см. Error).
	Points *int64 `json:"points"`
	// Error — почему баланса нет; пусто, если Orders ответил 200 с points.
	Error string `json:"error"`
}

// userLess — u-2 раньше u-10: сначала по числу после «u-», потом по строке.
func userLess(a, b string) bool {
	na, ea := strconv.Atoi(strings.TrimPrefix(a, "u-"))
	nb, eb := strconv.Atoi(strings.TrimPrefix(b, "u-"))
	switch {
	case ea == nil && eb == nil && na != nb:
		return na < nb
	case (ea == nil) != (eb == nil):
		return ea == nil
	}
	return a < b
}

// handleBalances — POST /admin/bus/balances. Ответ {"users": [...]}:
// покупатели всех партий по порядку (u-1, u-2, …), u-0 — последним. Одна
// проверка за раз, не дольше balancesWait: кого не успели спросить — с
// error.
func (s *state) handleBalances(w http.ResponseWriter, r *http.Request) {
	if s.ordersURL == "" {
		writeErr(w, http.StatusServiceUnavailable, "no_orders", "адрес Orders заглушкам не задан (ORDERS_URL)")
		return
	}
	s.mu.Lock()
	if s.balancing {
		s.mu.Unlock()
		writeErr(w, http.StatusConflict, "busy", "проверка балансов уже идёт")
		return
	}
	s.balancing = true
	defer func() {
		s.mu.Lock()
		s.balancing = false
		s.mu.Unlock()
	}()
	expected := make(map[string]int, len(s.bonusExpected))
	for u, v := range s.bonusExpected {
		expected[u] = v
	}
	s.mu.Unlock()

	var ids []string
	for u := range expected {
		if u != bonusUnknownUser {
			ids = append(ids, u)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return userLess(ids[i], ids[j]) })
	ids = append(ids, bonusUnknownUser)

	out := make([]balanceView, len(ids))
	for i, u := range ids {
		out[i] = balanceView{UserID: u, Expected: expected[u], Error: fmt.Sprintf("не спросили: проверка дольше %g с", balancesWait.Seconds())}
	}
	ctx, cancel := context.WithTimeout(r.Context(), balancesWait)
	defer cancel()
	var wg sync.WaitGroup
	sem := make(chan struct{}, 8)
users:
	for i := range out {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			break users
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			out[i].Points, out[i].Error = s.getBonus(ctx, out[i].UserID)
		}()
	}
	wg.Wait()
	writeJSON(w, http.StatusOK, map[string]any{"users": out})
}

// getBonus — один GET /users/{id}/bonus; разбор как у раннера (runner/task2.go).
func (s *state) getBonus(parent context.Context, user string) (*int64, string) {
	ctx, cancel := context.WithTimeout(parent, bonusWait)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, s.ordersURL+"/users/"+url.PathEscape(user)+"/bonus", nil)
	resp, err := ordersClient.Do(req)
	if err != nil {
		if ctx.Err() != nil || errors.Is(err, context.DeadlineExceeded) {
			return nil, "нет ответа"
		}
		return nil, "нет соединения"
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if resp.StatusCode != http.StatusOK {
		return nil, "GET " + strconv.Itoa(resp.StatusCode)
	}
	var v struct {
		Points *json.Number `json:"points"`
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	if dec.Decode(&v) != nil {
		return nil, "ответ не JSON"
	}
	if v.Points == nil {
		return nil, "в ответе нет points"
	}
	n, err := v.Points.Int64()
	if err != nil {
		return nil, "points не целое: " + sanitize(v.Points.String(), 20)
	}
	return &n, ""
}
