// Orders — наивная реализация.
//
// Оформление заказа: зарезервировать товар в Inventory, списать деньги в
// Payment, записать заказ в базу. Пока Payment и Inventory работают без отказов, всё работает.
//
// Задание 2 (необязательное): об оплаченном заказе сервис сообщает в шину
// событий (order.paid), а за подтверждённую доставку начисляет покупателю
// бонусы. Пока шина работает и Orders не падает, тоже всё работает.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const schema = `
CREATE TABLE IF NOT EXISTS orders (
    id           TEXT PRIMARY KEY,
    user_id      TEXT NOT NULL,
    status       TEXT NOT NULL,
    amount_cents BIGINT NOT NULL,
    items        JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS bonuses (
    user_id TEXT PRIMARY KEY,
    points  BIGINT NOT NULL
)`

type Item struct {
	SKU        string `json:"sku"`
	Qty        int    `json:"qty"`
	PriceCents int64  `json:"price_cents"`
}

type OrderRequest struct {
	UserID string `json:"user_id"`
	Items  []Item `json:"items"`
}

type app struct {
	db           *pgxpool.Pool
	paymentURL   string
	inventoryURL string
	eventsURL    string // шина событий; пусто — события не публикуются
}

func main() {
	a := &app{
		paymentURL:   strings.TrimRight(mustEnv("PAYMENT_URL"), "/"),
		inventoryURL: strings.TrimRight(mustEnv("INVENTORY_URL"), "/"),
		eventsURL:    strings.TrimRight(os.Getenv("EVENTS_URL"), "/"),
	}
	db, err := connect(mustEnv("DATABASE_URL"))
	if err != nil {
		slog.Error("база", "err", err)
		os.Exit(1)
	}
	a.db = db

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("POST /orders", a.createOrder)
	mux.HandleFunc("GET /orders/{id}", a.getOrder)
	mux.HandleFunc("POST /webhooks/delivery", a.deliveryConfirmed)
	mux.HandleFunc("GET /users/{id}/bonus", a.getBonus)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	slog.Info("orders: слушаю", "port", port)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		slog.Error("server", "err", err)
		os.Exit(1)
	}
}

// connect ждёт базу: она может подниматься дольше сервиса.
func connect(url string) (*pgxpool.Pool, error) {
	var lastErr error
	for range 60 {
		db, err := pgxpool.New(context.Background(), url)
		if err == nil {
			if _, err = db.Exec(context.Background(), schema); err == nil {
				return db, nil
			}
			db.Close()
		}
		lastErr = err
		slog.Info("база недоступна, жду", "err", err)
		time.Sleep(time.Second)
	}
	return nil, lastErr
}

func (a *app) createOrder(w http.ResponseWriter, r *http.Request) {
	var req OrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || !valid(req) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad_request"})
		return
	}
	id := newID()
	var amount int64
	for _, it := range req.Items {
		amount += int64(it.Qty) * it.PriceCents
	}

	if err := a.reserve(id, req.Items); err != nil {
		a.fail(w, id, err)
		return
	}
	declined, err := a.charge(id, amount)
	if err != nil {
		a.fail(w, id, err)
		return
	}
	status := "paid"
	if declined {
		a.release(id)
		status = "rejected"
	}

	items, _ := json.Marshal(req.Items)
	_, err = a.db.Exec(context.Background(),
		`INSERT INTO orders (id, user_id, status, amount_cents, items) VALUES ($1, $2, $3, $4, $5)`,
		id, req.UserID, status, amount, items)
	if err != nil {
		a.fail(w, id, err)
		return
	}
	slog.Info("order", "id", id, "status", status)
	if status == "paid" {
		a.publishPaid(id)
	}
	writeJSON(w, http.StatusCreated, map[string]string{"id": id, "status": status})
}

// publishPaid сообщает в шину, что заказ оплачен.
func (a *app) publishPaid(orderID string) {
	if a.eventsURL == "" {
		return
	}
	code, err := call(http.MethodPost, a.eventsURL+"/events", map[string]any{
		"id":          newID(),
		"type":        "order.paid",
		"order_id":    orderID,
		"occurred_at": time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil || code != http.StatusAccepted {
		slog.Warn("событие не опубликовано", "order", orderID, "code", code, "err", err)
	}
}

// deliveryConfirmed — POST /webhooks/delivery: начислить бонусы за доставку.
func (a *app) deliveryConfirmed(w http.ResponseWriter, r *http.Request) {
	var ev struct {
		ID          string `json:"id"`
		UserID      string `json:"user_id"`
		BonusPoints int64  `json:"bonus_points"`
	}
	if err := json.NewDecoder(r.Body).Decode(&ev); err != nil || ev.UserID == "" || ev.BonusPoints <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad_request"})
		return
	}
	_, err := a.db.Exec(r.Context(),
		`INSERT INTO bonuses (user_id, points) VALUES ($1, $2)
		 ON CONFLICT (user_id) DO UPDATE SET points = bonuses.points + EXCLUDED.points`,
		ev.UserID, ev.BonusPoints)
	if err != nil {
		slog.Error("бонусы", "event", ev.ID, "err", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *app) getBonus(w http.ResponseWriter, r *http.Request) {
	var points int64
	err := a.db.QueryRow(r.Context(), `SELECT points FROM bonuses WHERE user_id = $1`, r.PathValue("id")).Scan(&points)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]int64{"points": points})
}

func (a *app) fail(w http.ResponseWriter, id string, err error) {
	slog.Error("POST /orders", "id", id, "err", err)
	writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal", "message": err.Error()})
}

func (a *app) getOrder(w http.ResponseWriter, r *http.Request) {
	var (
		id, status string
		amount     int64
	)
	err := a.db.QueryRow(r.Context(), `SELECT id, status, amount_cents FROM orders WHERE id = $1`,
		r.PathValue("id")).Scan(&id, &status, &amount)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not_found"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"id": id, "status": status, "amount_cents": amount})
}

func (a *app) reserve(id string, items []Item) error {
	type resvItem struct {
		SKU string `json:"sku"`
		Qty int    `json:"qty"`
	}
	ri := make([]resvItem, 0, len(items))
	for _, it := range items {
		ri = append(ri, resvItem{it.SKU, it.Qty})
	}
	code, err := call(http.MethodPost, a.inventoryURL+"/reservations", map[string]any{"order_id": id, "items": ri})
	if err != nil {
		return err
	}
	if code != http.StatusOK && code != http.StatusCreated {
		return fmt.Errorf("inventory: %d", code)
	}
	return nil
}

func (a *app) release(id string) {
	_, _ = call(http.MethodDelete, a.inventoryURL+"/reservations/"+id, nil)
}

var errPayment = errors.New("payment")

// charge списывает деньги; declined — банк отказал.
func (a *app) charge(id string, amount int64) (declined bool, err error) {
	declined, err = a.chargeOnce(id, amount)
	if err != nil {
		slog.Warn("оплата не прошла, пробую ещё раз", "id", id, "err", err)
		declined, err = a.chargeOnce(id, amount)
	}
	return declined, err
}

func (a *app) chargeOnce(id string, amount int64) (bool, error) {
	code, err := call(http.MethodPost, a.paymentURL+"/payments", map[string]any{
		"order_id":     id,
		"amount_cents": amount,
		"currency":     "RUB",
	})
	if err != nil {
		return false, err
	}
	switch code {
	case http.StatusOK, http.StatusCreated:
		return false, nil
	case http.StatusPaymentRequired:
		return true, nil
	}
	return false, fmt.Errorf("%w: %d", errPayment, code)
}

func call(method, url string, body any) (int, error) {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, url, rd)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, nil
}

func valid(r OrderRequest) bool {
	if r.UserID == "" || len(r.Items) == 0 {
		return false
	}
	for _, it := range r.Items {
		if it.SKU == "" || it.Qty < 1 || it.PriceCents < 0 {
			return false
		}
	}
	return true
}

func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func mustEnv(k string) string {
	v := os.Getenv(k)
	if v == "" {
		slog.Error("не задана переменная окружения", "name", k)
		os.Exit(1)
	}
	return v
}
