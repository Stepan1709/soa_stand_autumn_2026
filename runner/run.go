package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type scenarioResult struct {
	ID      string   `json:"id"`
	Title   string   `json:"title"`
	Pass    bool     `json:"pass"`
	Error   string   `json:"error,omitempty"`
	Seconds float64  `json:"seconds"`
	Checks  []check  `json:"checks"`
	Journal []string `json:"journal"`
	// Facts — ключевые числа прогона (только числа: уходят в аналитику).
	Facts    map[string]int `json:"facts,omitempty"`
	coreOK   bool
	budgetOK bool
	// view — данные для HTML-отчёта (в JSON не попадают).
	view *scenarioView
}

// output — вывод раннера: в stdout и в копию для файла рядом с результатами.
// Управляющие символы и обратные кавычки заменяются: в выводе бывают id и
// тела ответов Orders, и они не должны превращаться в команды CI или разметку.
type output struct {
	mu  sync.Mutex // вывод читает GET /status, пока прогон пишет
	w   io.Writer
	buf bytes.Buffer
}

func newOutput(w io.Writer) *output { return &output{w: w} }

func (o *output) printf(format string, a ...any) {
	s := fmt.Sprintf(format, a...)
	lines := strings.Split(s, "\n")
	for i := range lines {
		lines[i] = clean(lines[i])
	}
	s = strings.Join(lines, "\n")
	o.mu.Lock()
	defer o.mu.Unlock()
	o.buf.WriteString(s)
	_, _ = io.WriteString(o.w, s)
}

// text — весь вывод на этот момент.
func (o *output) text() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.buf.String()
}

func (o *output) println(s string) { o.printf("%s\n", s) }

// writeResults пишет результаты в JSON, копию вывода в .txt и отчёт в .html
// рядом.
func writeResults(path string, v any, o *output, rep *report) {
	if path == "" {
		return
	}
	b, _ := json.MarshalIndent(v, "", "  ")
	if err := os.WriteFile(path, b, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "runner: не записал %s: %v\n", path, err)
	}
	txt := strings.TrimSuffix(path, filepath.Ext(path)) + ".txt"
	if txt == path {
		txt += ".txt"
	}
	if err := os.WriteFile(txt, []byte(o.text()), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "runner: не записал %s: %v\n", txt, err)
	}
	if rep == nil {
		return
	}
	rep.Output = o.text()
	page := strings.TrimSuffix(path, filepath.Ext(path)) + ".html"
	if page == path {
		page += ".html"
	}
	var b2 bytes.Buffer
	if err := renderReport(&b2, rep); err != nil {
		fmt.Fprintf(os.Stderr, "runner: отчёт %s: %v\n", page, err)
		return
	}
	if err := os.WriteFile(page, b2.Bytes(), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "runner: не записал %s: %v\n", page, err)
	}
}

func (r *runner) run(task string, ids []string, resultsPath string) int {
	started := time.Now()
	out := r.out
	out.printf("Задание %s, сценарии %s; пауза после заказов %.0f с, ответ не дольше %d мс\n\n",
		task, strings.Join(ids, " "), r.params.WaitS, r.params.BudgetMS)

	standErr := func(msg string) int {
		out.printf("СТЕНД: %s\n", msg)
		// Прогон начат, но не состоялся: снять блокировку заглушек сразу.
		r.endRun("прогон прерван: стенд не готов")
		r.finish(resultsPath, map[string]any{
			"task": task, "run_at": started.UTC(), "seconds": time.Since(started).Seconds(),
			"error": clean(strings.SplitN(msg, "\n", 2)[0]), "passed": 0, "total": len(ids),
			"scenarios": []scenarioResult{}, "level": "",
		}, &report{Task: task, RunAt: started, Seconds: time.Since(started).Seconds(),
			Error: clean(strings.SplitN(msg, "\n", 2)[0]), Total: len(ids)})
		return 2
	}
	if err := r.waitHealthy("заглушки (mocks)", r.mocks, 60*time.Second); err != nil {
		return standErr(err.Error())
	}
	// Метка начала прогона — сразу после проверки заглушек, до ожидания Orders:
	// с этого момента ручные кнопки дашборда и второй прогон не мешают.
	if err := r.startRun(); err != nil {
		return standErr(err.Error())
	}
	// Признак жизни для заглушек — до метки конца прогона.
	stopAlive := r.keepAlive()
	defer stopAlive()
	ordersWait := 120 * time.Second
	if r.ordersWait > 0 {
		ordersWait = r.ordersWait
	}
	if err := r.waitHealthy("Orders", r.orders, ordersWait); err != nil {
		return standErr(err.Error() + "\nСмотрите make logs: сервис orders должен слушать PORT и отвечать 200 на GET /healthz.")
	}

	var results []scenarioResult
	for _, id := range ids {
		def := scenarioByID(id)
		r.report(id, results)
		out.printf("%s  %s ...\n", id, def.Title)
		res := r.one(def)
		results = append(results, res)
		printResult(out, res)
	}
	r.report("", results)
	// Отказы выключаются, но журнал последнего сценария остаётся в заглушках:
	// его можно разобрать на дашборде (make ui) после прогона.
	r.endRun("прогон завершён")

	pass := 0
	for _, res := range results {
		if res.Pass {
			pass++
		}
	}
	out.println("Итог:")
	for _, res := range results {
		out.printf("  %s %s  %s\n", verdict(res.Pass), res.ID, res.Title)
	}
	out.printf("Прошло %d из %d за %s\n", pass, len(results), durRU(time.Since(started).Seconds()))
	lvl, line := level(task, ids, results)
	if line != "" {
		out.println(line)
	}

	r.finish(resultsPath, map[string]any{
		"task": task, "run_at": started.UTC(), "seconds": time.Since(started).Seconds(),
		"passed": pass, "total": len(results), "scenarios": results, "level": lvl,
	}, &report{Task: task, RunAt: started, Seconds: time.Since(started).Seconds(),
		Passed: pass, Total: len(results), Level: lvl, Scenarios: results})
	if pass != len(results) {
		return 1
	}
	return 0
}

// report сообщает о ходе прогона (для runner -serve): current — сценарий,
// который начинается ("" — все закончились), done — готовые.
func (r *runner) report(current string, done []scenarioResult) {
	if r.progress != nil {
		r.progress(current, append([]scenarioResult(nil), done...))
	}
}

// finish запоминает итог прогона (для runner -serve) и пишет результаты.
func (r *runner) finish(path string, v map[string]any, rep *report) {
	rep.Output = r.out.text()
	r.lastResults, r.lastReport = v, rep
	writeResults(path, v, r.out, rep)
}

func (r *runner) one(def *scenarioDef) scenarioResult {
	res := scenarioResult{ID: def.ID, Title: def.Title}
	start := time.Now()
	rs, err := r.runScenario(def)
	res.Seconds = time.Since(start).Seconds()
	if err != nil {
		res.Error = "ошибка стенда: " + clean(err.Error())
		return res
	}
	evaluate(&res, def, r.params.scenario(def.ID), rs)
	return res
}

// evaluate — проверки, журнал и сводка по состоянию после прогона.
func evaluate(res *scenarioResult, def *scenarioDef, p *ScenarioParams, rs *runState) {
	if !def.noOrders {
		res.Checks = commonChecks(rs.params, rs)
	}
	if def.extra != nil {
		res.Checks = append(def.extra(p, rs), res.Checks...)
	}
	res.Pass, res.coreOK, res.budgetOK = true, true, true
	for _, c := range res.Checks {
		if !c.OK {
			res.Pass = false
			switch c.kind {
			case kindDouble:
				res.coreOK = false
			case kindBudget:
				res.budgetOK = false
			}
		}
	}
	story := orderStory
	if def.noOrders {
		res.Journal = journalS8(rs)
		res.Facts = keyFactsS8(rs)
		story = userStory
	} else {
		res.Journal = journal(rs, def)
		res.Facts = keyFacts(rs)
	}
	// Истории проблемных заказов — в журнал: каждый заказ один раз, не
	// больше пяти на проверку.
	shown := map[string]bool{}
	for _, c := range res.Checks {
		if c.OK {
			continue
		}
		n := 0
		for _, id := range c.Orders {
			if shown[id] {
				continue
			}
			if n == 5 {
				res.Journal = append(res.Journal, fmt.Sprintf("  … и другие (%s)", c.Name))
				break
			}
			shown[id] = true
			n++
			res.Journal = append(res.Journal, fmt.Sprintf("  [%s] %s: %s", c.Name, rs.display(id), story(rs, id)))
		}
	}
	res.view = buildView(res, rs)
	// В JSON — id целиком, но без управляющих символов и не длиннее 64.
	for i := range res.Checks {
		for j, id := range res.Checks[i].Orders {
			res.Checks[i].Orders[j] = cut(clean(id), 64)
		}
	}
}

func printResult(out *output, res scenarioResult) {
	out.printf("%s %s  %s  (%.0f с)\n", verdict(res.Pass), res.ID, res.Title, res.Seconds)
	if res.Error != "" {
		out.printf("     %s\n\n", res.Error)
		return
	}
	for _, c := range res.Checks {
		mark := "ok  "
		if !c.OK {
			mark = "FAIL"
		}
		line := fmt.Sprintf("     %s %s", mark, c.Name)
		if c.Detail != "" {
			line += ": " + c.Detail
		}
		out.println(line)
	}
	out.println("   журнал фактов:")
	for _, l := range res.Journal {
		out.printf("     %s\n", l)
	}
	out.println("")
}

func verdict(ok bool) string {
	if ok {
		return "PASS"
	}
	return "FAIL"
}

// level — уровень по таблице «Уровни» задания; только при полном прогоне.
// Возвращает уровень одним словом (для JSON) и строку для вывода.
func level(task string, ids []string, results []scenarioResult) (string, string) {
	if len(results) != len(tasks[task]) || len(ids) != len(tasks[task]) {
		return "", ""
	}
	if task == "2" {
		return level2(results)
	}
	if task != "1" {
		return "", ""
	}
	all, s1, core := true, false, true
	for _, res := range results {
		all = all && res.Pass
		if res.ID == "S1" {
			s1 = res.Pass
		}
		if res.Error != "" || !res.coreOK || !res.budgetOK {
			core = false
		}
	}
	switch {
	case all:
		return "полностью", "Уровень: полностью"
	case s1 && core:
		return "засчитано", "Уровень: засчитано (S1 PASS, нет двойных списаний, все ответы уложились в предел времени)"
	}
	return "не засчитано", "Уровень: не засчитано (нужно: S1 PASS, во всех сценариях нет двойных списаний и все ответы уложились в предел времени)"
}

// short — id для вывода: без управляющих символов, длинный сокращается до
// 12 символов — начало и конец (у id с общим префиксом различаются концы).
func short(id string) string {
	r := []rune(clean(id))
	if len(r) > 12 {
		return string(r[:6]) + "…" + string(r[len(r)-5:])
	}
	return string(r)
}

func cut(s string, n int) string {
	r := []rune(s)
	if len(r) > n {
		return string(r[:n-4]) + "…"
	}
	return s
}

// level2 — уровни задания 2: «засчитано» — S1, S6 и S7; «полностью» — и S8.
func level2(results []scenarioResult) (string, string) {
	pass := map[string]bool{}
	for _, res := range results {
		pass[res.ID] = res.Pass && res.Error == ""
	}
	switch {
	case pass["S1"] && pass["S6"] && pass["S7"] && pass["S8"]:
		return "полностью", "Уровень: полностью"
	case pass["S1"] && pass["S6"] && pass["S7"]:
		return "засчитано", "Уровень: засчитано (S1, S6 и S7 PASS)"
	}
	return "не засчитано", "Уровень: не засчитано (задание необязательное; нужно: S1, S6 и S7 PASS)"
}
