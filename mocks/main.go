// Заглушки для Orders: Payment, Inventory и управление ими (/admin/*).
//
// Один процесс, один порт (MOCKS_ADDR, по умолчанию :8080):
//
//	POST /payments, GET /payments?order_id=      — Payment (contracts/payment.yaml)
//	POST /reservations, DELETE /reservations/{order_id},
//	GET  /reservations?order_id=                 — Inventory (contracts/inventory.yaml)
//	POST /events                                 — шина событий (contracts/events.yaml, bus.go)
//	POST /admin/bus/{deliver,redeliver}          — доставка delivery.confirmed в Orders
//	PUT  /admin/modes, POST /admin/reset,
//	GET  /admin/facts, GET /admin/events?since=  — управление, пользуются раннер
//	                                               и дашборд; с MOCKS_ADMIN_TOKEN —
//	                                               только с заголовком X-Admin-Token
//	POST /admin/orders, POST /admin/orders/check — заказы в Orders с дашборда
//	                                               (адрес — только ORDERS_URL)
//	/admin/runner/{status,run,report,params}     — прокси к сервису прогонов
//	                                               из дашборда (RUNNER_UI_URL, make ui)
//	                                               Во время прогона раннера ручные
//	                                               действия страницы — 409 (runlock.go)
//	GET  /ui                                     — дашборд (страница без токена,
//	                                               данные — из /admin/*)
package main

import (
	"crypto/subtle"
	"flag"
	"fmt"
	"log"
	"mime"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	healthcheck := flag.Bool("healthcheck", false, "проверить, что заглушки отвечают, и выйти")
	flag.Parse()

	addr := envOr("MOCKS_ADDR", ":8080")
	if *healthcheck {
		os.Exit(runHealthcheck(addr))
	}

	m := initialModes()
	if err := m.Payment.validate(); err != nil {
		log.Fatalf("mocks: режимы из окружения: %v", err)
	}
	if err := m.Bus.validate(); err != nil {
		log.Fatalf("mocks: режимы из окружения: %v", err)
	}
	st := newState(m)
	st.adminToken = os.Getenv("MOCKS_ADMIN_TOKEN")
	target, err := ordersTarget(os.Getenv("ORDERS_URL"))
	if err != nil {
		log.Fatalf("mocks: %v", err)
	}
	st.ordersURL = target
	if v := os.Getenv("RUNNER_UI_URL"); v != "" {
		if st.runnerUI, err = newRunnerProxy(v); err != nil {
			log.Fatalf("mocks: %v", err)
		}
	}
	srv := &http.Server{
		Addr:              addr,
		Handler:           st.routes(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	log.Printf("mocks: слушаю %s", addr)
	log.Fatal(srv.ListenAndServe())
}

func (s *state) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /payments", s.handleCharge)
	mux.HandleFunc("GET /payments", s.handleLookup)
	mux.HandleFunc("POST /reservations", s.handleReserve)
	mux.HandleFunc("DELETE /reservations/{order_id}", s.handleRelease)
	mux.HandleFunc("POST /events", s.handlePublish)
	mux.HandleFunc("GET /reservations", s.handleReservations)
	mux.HandleFunc("PUT /admin/modes", s.admin(s.manual(s.handleSetModes)))
	mux.HandleFunc("GET /admin/modes", s.admin(s.handleGetModes))
	mux.HandleFunc("POST /admin/reset", s.admin(s.manual(s.handleReset)))
	mux.HandleFunc("GET /admin/facts", s.admin(s.handleFacts))
	mux.HandleFunc("GET /admin/events", s.admin(s.handleEvents))
	mux.HandleFunc("POST /admin/run/start", s.admin(s.handleRunStart))
	mux.HandleFunc("GET /admin/run", s.admin(s.handleRunStatus))
	mux.HandleFunc("POST /admin/bus/deliver", s.admin(s.manual(s.handleDeliver)))
	mux.HandleFunc("POST /admin/bus/redeliver", s.admin(s.manual(s.handleRedeliver)))
	mux.HandleFunc("POST /admin/bus/balances", s.admin(s.manual(s.handleBalances)))
	mux.HandleFunc("POST /admin/orders", s.admin(s.manual(s.handleSendOrders)))
	mux.HandleFunc("POST /admin/orders/check", s.admin(s.manual(s.handleCheckOrders)))
	mux.HandleFunc("/admin/runner/", s.admin(s.handleRunner))
	mux.HandleFunc("GET /ui", handleUI)
	// /ui/ — частая опечатка (и так открывают ссылку некоторые make): фрагмент
	// с токеном браузер сохраняет при переадресации.
	mux.HandleFunc("GET /ui/{$}", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/ui", http.StatusFound)
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	return mux
}

// admin пускает к /admin/* только с токеном из MOCKS_ADMIN_TOKEN (если он
// задан): управлять заглушками и читать факты может раннер, но не Orders.
// Запросы раннера (X-Stand-Client: runner) отмечаются: идёт прогон (runlock.go).
//
// Защита от чужих страниц в браузере (важно, когда токен пуст): Host — только
// имена стенда (сайт с чужим именем, указывающим на 127.0.0.1, получит 403),
// а изменяющие запросы — только с Content-Type: application/json (такой
// запрос с чужой страницы браузер без разрешения не отправит).
func (s *state) admin(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !standHost(r.Host) {
			writeErr(w, http.StatusForbidden, "forbidden", "адрес стенда: localhost или 127.0.0.1")
			return
		}
		if s.adminToken != "" && subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Admin-Token")), []byte(s.adminToken)) != 1 {
			writeErr(w, http.StatusForbidden, "forbidden", "нужен заголовок X-Admin-Token")
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead && !jsonType(r.Header.Get("Content-Type")) {
			writeErr(w, http.StatusUnsupportedMediaType, "content_type", "нужен заголовок Content-Type: application/json")
			return
		}
		if isRunner(r) {
			s.noteRunner(r)
		}
		h(w, r)
	}
}

// standHosts — имена, под которыми к заглушкам ходят браузер (порт на хосте —
// любой: MOCKS_PORT), раннер и runner-ui.
var standHosts = map[string]bool{"localhost": true, "127.0.0.1": true, "::1": true, "mocks": true, "mocks-admin": true}

func standHost(hostport string) bool {
	host := hostport
	if h, _, err := net.SplitHostPort(hostport); err == nil {
		host = h
	}
	host = strings.TrimSuffix(strings.TrimPrefix(host, "["), "]")
	return standHosts[strings.ToLower(host)]
}

func jsonType(v string) bool {
	mt, _, err := mime.ParseMediaType(v)
	return err == nil && mt == "application/json"
}

// initialModes — режимы при старте из переменных окружения (для
// экспериментов руками); раннер выставляет режимы только через /admin/modes.
func initialModes() Modes {
	m := Modes{Seed: 1}
	if v := os.Getenv("MOCK_SEED"); v != "" {
		if n, err := strconv.ParseUint(v, 10, 64); err == nil {
			m.Seed = n
		}
	}
	m.Payment.FailRate = envFloat("MOCK_PAYMENT_FAIL_RATE")
	m.Payment.DeclineRate = envFloat("MOCK_PAYMENT_DECLINE_RATE")
	m.Payment.LatencyMS = int(envFloat("MOCK_PAYMENT_LATENCY_MS"))
	m.Payment.BlackholeS = envFloat("MOCK_PAYMENT_BLACKHOLE_S")
	m.Payment.BlackholeAfterS = envFloat("MOCK_PAYMENT_BLACKHOLE_AFTER_S")
	m.Payment.LostResponse = os.Getenv("MOCK_PAYMENT_LOST_RESPONSE")
	m.Bus.DownS = envFloat("MOCK_BUS_DOWN_S")
	m.Bus.DownAfterS = envFloat("MOCK_BUS_DOWN_AFTER_S")
	m.Bus.HangRate = envFloat("MOCK_BUS_HANG_RATE")
	return m
}

func runHealthcheck(addr string) int {
	host := addr
	if len(host) > 0 && host[0] == ':' {
		host = "127.0.0.1" + host
	}
	c := http.Client{Timeout: 2 * time.Second}
	resp, err := c.Get("http://" + host + "/healthz")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envFloat(k string) float64 {
	v, err := strconv.ParseFloat(os.Getenv(k), 64)
	if err != nil {
		return 0
	}
	return v
}
