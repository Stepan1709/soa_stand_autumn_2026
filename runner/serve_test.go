package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeStand — заглушки и Orders для прогона без Docker: Orders отвечает
// rejected, заглушки — пустыми фактами.
func fakeStand(t *testing.T) (orders, mocks *httptest.Server, modes *[]map[string]any) {
	t.Helper()
	var n atomic.Int64
	orders = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/healthz":
			w.WriteHeader(200)
		case r.Method == "POST" && r.URL.Path == "/orders":
			w.WriteHeader(201)
			fmt.Fprintf(w, `{"id":"o-%d","status":"rejected"}`, n.Add(1))
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/orders/"):
			fmt.Fprint(w, `{"status":"rejected","amount_cents":0}`)
		default:
			w.WriteHeader(404)
		}
	}))
	t.Cleanup(orders.Close)
	var mu sync.Mutex
	var got []map[string]any
	mocks = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/admin/") && r.Header.Get("X-Admin-Token") != "tok" {
			w.WriteHeader(403)
			return
		}
		switch r.URL.Path {
		case "/healthz", "/admin/reset":
			w.WriteHeader(200)
		case "/admin/modes":
			var m map[string]any
			_ = json.NewDecoder(r.Body).Decode(&m)
			mu.Lock()
			got = append(got, m)
			mu.Unlock()
			w.WriteHeader(200)
		case "/admin/facts":
			fmt.Fprint(w, `{"payment":{"requests":{"by_order":{},"by_status":{}},"charges":{"by_order":{},"amount_by_order":{}},"declines":{"by_order":{}}},"inventory":{"reservations":{"by_order":{}}}}`)
		default:
			w.WriteHeader(404)
		}
	}))
	t.Cleanup(mocks.Close)
	return orders, mocks, &got
}

func apiCall(t *testing.T, h http.Handler, method, path, token, body string) (int, []byte) {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		req.Header.Set("X-Admin-Token", token)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	b, _ := io.ReadAll(rec.Body)
	return rec.Code, b
}

func TestServeRun(t *testing.T) {
	orders, mocks, modes := fakeStand(t)
	dir := t.TempDir()
	params := filepath.Join(dir, "p.json")
	_ = os.WriteFile(params, []byte(`{"scenarios":{"S1":{"orders":3,"rate_per_s":100}}}`), 0o644)
	s := &server{state: "idle", cfg: serveConfig{task: "1", paramsFile: params, results: filepath.Join(dir, "task_1.json"),
		orders: orders.URL, mocks: mocks.URL, token: "tok"}}
	h := s.routes()

	if c, _ := apiCall(t, h, "GET", "/status", "", ""); c != 403 {
		t.Fatalf("без токена: %d", c)
	}
	if c, _ := apiCall(t, h, "POST", "/run", "tok", `{"only":"S9"}`); c != 400 {
		t.Fatalf("S9: %d", c)
	}
	if c, _ := apiCall(t, h, "POST", "/run", "tok", `{"only":"S1","wait":-1}`); c != 400 {
		t.Fatalf("wait -1: %d", c)
	}
	if c, _ := apiCall(t, h, "GET", "/report", "tok", ""); c != 404 {
		t.Fatalf("отчёт до прогона: %d", c)
	}
	if c, b := apiCall(t, h, "POST", "/run", "tok", `{"only":"s1","wait":0}`); c != 202 {
		t.Fatalf("run: %d %s", c, b)
	}
	if c, _ := apiCall(t, h, "POST", "/run", "tok", `{"only":"S1","wait":0}`); c != 409 {
		t.Fatalf("второй прогон: %d", c)
	}
	var st runStatus
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		_, b := apiCall(t, h, "GET", "/status", "tok", "")
		st = runStatus{}
		if err := json.Unmarshal(b, &st); err != nil {
			t.Fatal(err)
		}
		if st.State == "done" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if st.State != "done" || st.Results == nil || len(st.Scenarios) != 1 || st.Scenarios[0].State != "fail" ||
		!strings.Contains(st.Text, "S1") || !st.Report || st.FinishedAt == nil {
		t.Fatalf("status %+v", st)
	}
	if _, err := os.Stat(filepath.Join(dir, "task_1.html")); err != nil {
		t.Fatal("results не записаны:", err)
	}
	c, b := apiCall(t, h, "GET", "/report", "tok", "")
	if c != 200 || !strings.Contains(string(b), "Отчёт раннера") {
		t.Fatalf("report %d", c)
	}
	// В конце — отказы выключены, метка «прогон завершён».
	last := (*modes)[len(*modes)-1]
	if last["finished"] != true || !strings.Contains(fmt.Sprint(last["label"]), "прогон завершён") {
		t.Fatalf("modes %+v", last)
	}
}

func TestServeParamsPublic(t *testing.T) {
	s := &server{state: "idle", cfg: serveConfig{task: "1"}}
	c, b := apiCall(t, s.routes(), "GET", "/params", "", "")
	if c != 200 {
		t.Fatalf("%d", c)
	}
	low := strings.ToLower(string(b))
	for _, w := range []string{"seed", "timeout", "key", "lookup"} {
		if strings.Contains(low, w) {
			t.Fatalf("%q в /params: %s", w, b)
		}
	}
	var p struct {
		Scenarios map[string]publicScenario `json:"scenarios"`
	}
	if err := json.Unmarshal(b, &p); err != nil || p.Scenarios["S2"].Payment.FailRate != 0.33 ||
		p.Scenarios["S4"].Payment.BlackholeS != 60 || p.Scenarios["S1"].Orders != 50 {
		t.Fatalf("%v %s", err, b)
	}
}

func TestAllowList(t *testing.T) {
	a := &allowList{name: "localhost"}
	if !a.allowed("127.0.0.1:1234") || a.allowed("10.9.9.9:1") || a.allowed("garbage") {
		t.Fatal("allowList")
	}
}

func TestDashboardBusy(t *testing.T) {
	state := "running"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/admin/runner/status" || r.Header.Get("X-Admin-Token") != "tok" {
			w.WriteHeader(404)
			return
		}
		fmt.Fprintf(w, `{"state":%q}`, state)
	}))
	defer srv.Close()
	r := newRunner(testParams(t), "http://x", srv.URL, "tok", newOutput(io.Discard))
	if !r.dashboardBusy() {
		t.Fatal("running → busy")
	}
	state = "done"
	if r.dashboardBusy() {
		t.Fatal("done → не busy")
	}
	r = newRunner(testParams(t), "http://x", "http://127.0.0.1:1", "tok", newOutput(io.Discard))
	if r.dashboardBusy() {
		t.Fatal("нет ответа → не busy")
	}
}

func waitDone(t *testing.T, h http.Handler) runStatus {
	t.Helper()
	var st runStatus
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		_, b := apiCall(t, h, "GET", "/status", "tok", "")
		st = runStatus{}
		if err := json.Unmarshal(b, &st); err != nil {
			t.Fatal(err)
		}
		if st.State == "done" {
			return st
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("прогон не закончился: %+v", st)
	return st
}

func TestServeTask2(t *testing.T) {
	orders, mocks, _ := fakeStand(t)
	dir := t.TempDir()
	params := filepath.Join(dir, "p.json")
	_ = os.WriteFile(params, []byte(`{"scenarios":{"S1":{"orders":3,"rate_per_s":100}}}`), 0o644)
	s := &server{state: "idle", cfg: serveConfig{task: "1", paramsFile: params,
		results: filepath.Join(dir, "ui_task_{task}.json"), orders: orders.URL, mocks: mocks.URL, token: "tok"}}
	h := s.routes()

	// Файл make scenarios не должен затираться.
	_ = os.WriteFile(filepath.Join(dir, "task_2.json"), []byte("make"), 0o644)

	for _, body := range []string{`{"task":"2","ids":["S7"]}`, `{"task":"2","ids":["S1","s8"]}`, `{"task":"2","only":"S7"}`, `{"ids":["S7"]}`} {
		c, b := apiCall(t, h, "POST", "/run", "tok", body)
		var e map[string]string
		_ = json.Unmarshal(b, &e)
		if c != 400 || e["error"] != "bad_request" || e["message"] != msgChaosOnly {
			t.Fatalf("%s: %d %s", body, c, b)
		}
	}
	for _, body := range []string{`{"task":"2","ids":["S2"]}`, `{"task":"3"}`, `{"task":"2","wait_s":-1}`} {
		if c, b := apiCall(t, h, "POST", "/run", "tok", body); c != 400 {
			t.Fatalf("%s: %d %s", body, c, b)
		}
	}
	if c, _ := apiCall(t, h, "GET", "/report?task=2", "tok", ""); c != 404 {
		t.Fatalf("отчёт до прогона: %d", c)
	}
	if c, _ := apiCall(t, h, "GET", "/report?task=7", "tok", ""); c != 400 {
		t.Fatalf("задание 7: %d", c)
	}
	_, b := apiCall(t, h, "GET", "/status", "tok", "")
	var st runStatus
	if json.Unmarshal(b, &st); st.Task != "1" || st.State != "idle" {
		t.Fatalf("status до прогона %s", b)
	}

	if c, b := apiCall(t, h, "POST", "/run", "tok", `{"task":"2","ids":["s1"],"wait_s":0}`); c != 202 {
		t.Fatalf("run: %d %s", c, b)
	}
	st = waitDone(t, h)
	if st.Task != "2" || len(st.Scenarios) != 1 || st.Scenarios[0].ID != "S1" || !st.Report || st.Level != "" {
		t.Fatalf("status %+v", st)
	}
	for _, f := range []string{"ui_task_2.json", "ui_task_2.txt", "ui_task_2.html"} {
		if _, err := os.Stat(filepath.Join(dir, f)); err != nil {
			t.Fatal(err)
		}
	}
	if b, _ := os.ReadFile(filepath.Join(dir, "task_2.json")); string(b) != "make" {
		t.Fatal("task_2.json затёрт")
	}
	if c, _ := apiCall(t, h, "GET", "/report", "tok", ""); c != 200 {
		t.Fatalf("отчёт последнего прогона: %d", c)
	}
	if c, _ := apiCall(t, h, "GET", "/report?task=2", "tok", ""); c != 200 {
		t.Fatalf("отчёт задания 2: %d", c)
	}
	if c, _ := apiCall(t, h, "GET", "/report?task=1", "tok", ""); c != 404 {
		t.Fatalf("отчёт задания 1 без прогона: %d", c)
	}

	// Без task — задание 1.
	if c, b := apiCall(t, h, "POST", "/run", "tok", `{"ids":["S1"],"wait_s":0}`); c != 202 {
		t.Fatalf("run: %d %s", c, b)
	}
	if st = waitDone(t, h); st.Task != "1" {
		t.Fatalf("status %+v", st)
	}
	if _, err := os.Stat(filepath.Join(dir, "ui_task_1.html")); err != nil {
		t.Fatal(err)
	}
	if c, _ := apiCall(t, h, "GET", "/report?task=2", "tok", ""); c != 200 {
		t.Fatalf("отчёт задания 2 после прогона задания 1: %d", c)
	}
}

func TestServeReportFromDisk(t *testing.T) {
	dir := t.TempDir()
	s := &server{state: "idle", cfg: serveConfig{task: "1", results: filepath.Join(dir, "ui_task_{task}.json")}}
	h := s.routes()
	if c, _ := apiCall(t, h, "GET", "/report?task=2", "", ""); c != 404 {
		t.Fatalf("нет файла: %d", c)
	}
	_ = os.WriteFile(filepath.Join(dir, "ui_task_2.html"), []byte("<p>отчёт 2</p>"), 0o644)
	c, b := apiCall(t, h, "GET", "/report?task=2", "", "")
	if c != 200 || string(b) != "<p>отчёт 2</p>" {
		t.Fatalf("с диска: %d %s", c, b)
	}
	if c, _ := apiCall(t, h, "GET", "/report", "", ""); c != 404 {
		t.Fatalf("задание 1 без файла: %d", c)
	}
}

func TestServeEmptyIDs(t *testing.T) {
	s := &server{state: "running", cfg: serveConfig{task: "1"}}
	// Пустой ids — как без ids: проверка проходит, упираемся в идущий прогон.
	if c, b := apiCall(t, s.routes(), "POST", "/run", "", `{"task":"2","ids":[]}`); c != 409 {
		t.Fatalf("%d %s", c, b)
	}
	if ids, msg := checkIDs("2", dashboardIDs["2"]); msg != "" || len(ids) != 2 {
		t.Fatal(ids, msg)
	}
}

func TestServeParamsTask2(t *testing.T) {
	s := &server{state: "idle", cfg: serveConfig{task: "1"}}
	c, b := apiCall(t, s.routes(), "GET", "/params?task=2", "", "")
	if c != 200 {
		t.Fatalf("%d %s", c, b)
	}
	low := strings.ToLower(string(b))
	for _, w := range []string{"seed", "timeout", "key", "kill", "restart", "s7", "s8"} {
		if strings.Contains(low, w) {
			t.Fatalf("%q в /params: %s", w, b)
		}
	}
	var p struct {
		Task      string                    `json:"task"`
		IDs       []string                  `json:"ids"`
		Scenarios map[string]publicScenario `json:"scenarios"`
	}
	if err := json.Unmarshal(b, &p); err != nil || p.Task != "2" || len(p.IDs) != 2 || len(p.Scenarios) != 2 ||
		p.Scenarios["S6"].Bus == nil || p.Scenarios["S6"].Bus.DownS != 60 || p.Scenarios["S6"].Bus.DownAfterS != 2 {
		t.Fatalf("%v %s", err, b)
	}
	if c, _ := apiCall(t, s.routes(), "GET", "/params?task=9", "", ""); c != 400 {
		t.Fatalf("task=9: %d", c)
	}
}

func TestResultsPath(t *testing.T) {
	s := &server{cfg: serveConfig{task: "1", results: "/r/ui_task_{task}.json"}}
	if s.resultsPath("2") != "/r/ui_task_2.json" {
		t.Fatal(s.resultsPath("2"))
	}
	s.cfg.results = "/r/ui_task_1.json"
	if s.resultsPath("1") != "/r/ui_task_1.json" || s.resultsPath("2") != "/r/ui_task_2.json" {
		t.Fatal(s.resultsPath("2"))
	}
	s.cfg.results = ""
	if s.resultsPath("2") != "" {
		t.Fatal("пусто")
	}
}
