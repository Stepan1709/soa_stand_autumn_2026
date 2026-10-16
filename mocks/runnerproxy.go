package main

// /admin/runner/* — прокси к сервису прогонов из дашборда (runner -serve,
// сервис runner-ui, make ui). Пропускаются только известные пути; токен
// проверяет и admin(), и сам сервис прогонов.

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"
)

var runnerPaths = map[string]string{
	"/admin/runner/status": http.MethodGet,
	"/admin/runner/run":    http.MethodPost,
	"/admin/runner/report": http.MethodGet,
	"/admin/runner/params": http.MethodGet,
}

// newRunnerProxy — прокси на base (http://хост:порт). Если сервис не
// поднят — 502 с подсказкой «make ui».
func newRunnerProxy(base string) (http.Handler, error) {
	u, err := url.Parse(strings.TrimRight(base, "/"))
	if err != nil || u.Scheme != "http" || u.Host == "" || u.Path != "" {
		return nil, fmt.Errorf("RUNNER_UI_URL: ожидается http://хост:порт, получено %q", base)
	}
	p := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(u)
			pr.Out.URL.Path = strings.TrimPrefix(pr.In.URL.Path, "/admin/runner")
			pr.Out.URL.RawPath = ""
			pr.Out.URL.RawQuery = runnerQuery(pr.In.URL.Query())
			pr.Out.Header.Del("Cookie")
		},
		Transport: &http.Transport{
			DialContext:           (&net.Dialer{Timeout: 2 * time.Second}).DialContext,
			ResponseHeaderTimeout: 10 * time.Second,
			MaxIdleConns:          4,
			IdleConnTimeout:       30 * time.Second,
		},
		ModifyResponse: func(resp *http.Response) error {
			h := resp.Header
			h.Set("X-Content-Type-Options", "nosniff")
			h.Set("Cache-Control", "no-store")
			// Отчёт — самодостаточная страница без скриптов.
			h.Set("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'; img-src data:; base-uri 'none'; form-action 'none'; frame-ancestors 'none'")
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, _ error) {
			writeErr(w, http.StatusBadGateway, "runner_unavailable",
				"прогоны из дашборда недоступны: запустите make ui (стенд должен быть поднят: make up)")
		},
	}
	return p, nil
}

// runnerQuery — из параметров запроса в сервис прогонов уходит только
// task=1 или task=2 (какое задание), остальное отбрасывается.
func runnerQuery(q url.Values) string {
	switch t := q.Get("task"); t {
	case "1", "2":
		return "task=" + t
	}
	return ""
}

func (s *state) handleRunner(w http.ResponseWriter, r *http.Request) {
	m, ok := runnerPaths[r.URL.Path]
	if !ok {
		writeErr(w, http.StatusNotFound, "not_found", "нет такого пути")
		return
	}
	if r.Method != m {
		writeErr(w, http.StatusMethodNotAllowed, "method_not_allowed", "ожидается "+m)
		return
	}
	if s.runnerUI == nil {
		writeErr(w, http.StatusBadGateway, "runner_unavailable", "прогоны из дашборда недоступны: заглушкам не задан RUNNER_UI_URL")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<12)
	if r.URL.Path == "/admin/runner/run" {
		// «Прогнать» со страницы во время make scenarios — тоже ручное
		// действие: два прогона испортили бы друг другу проверку.
		s.manual(s.runnerUI.ServeHTTP)(w, r)
		return
	}
	s.runnerUI.ServeHTTP(w, r)
}
