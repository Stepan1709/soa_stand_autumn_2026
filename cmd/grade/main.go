// Command grade — официальная проверка стенда.
//
// Кандидат (форк студента) — только входные данные: из него берётся одна
// директория orders/. Заглушки, раннер, compose и сценарии — из baseline
// (репозиторий курса). Результаты: package-results.json (статус по каждому
// заданию), change-policy-result.json (правило файлов), task_N.log (полный
// журнал: сборка образа и вывод make), task_N/task_N.json, task_N/task_N.txt и
// task_N/task_N.html (результаты, вывод и HTML-отчёт раннера), analytics.json.
//
// --params FILE — прогон со скрытыми параметрами (перед ревью): файл лежит вне
// репозитория, копируется в рабочую копию и передаётся раннеру вместо
// открытых параметров из runner/params/.
package main

import (
	"archive/tar"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// allTasks — все задания курса; включённые перечислены в .etc/config.json.
var allTasks = []string{"task_1", "task_2"}

const reportPrefix = "soa_stand/tasks/"

type config struct {
	Stream string   `json:"stream"`
	Tasks  []string `json:"tasks"`
	// Optional — необязательные из включённых: гоняются и показываются, но
	// на итог проверки (код выхода grade) не влияют.
	Optional []string `json:"optional"`
	Diff     struct {
		Original struct {
			Repo string `json:"repo"`
			Ref  string `json:"ref"`
		} `json:"original"`
		AllowPrefix string `json:"allow_prefix"`
	} `json:"diff"`
}

type packageResult struct {
	Status   string `json:"status"`
	ExitCode int    `json:"exit_code,omitempty"`
}

type guardResult struct {
	CheckCode string         `json:"checkCode"`
	Report    map[string]any `json:"report"`
}

// runnerResults — то, что пишет runner -results (только нужные поля).
// Error непустой, если стенд не поднялся (тогда scenarios пустой).
type runnerResults struct {
	Error     string  `json:"error"`
	Level     string  `json:"level"`
	Passed    int     `json:"passed"`
	Total     int     `json:"total"`
	Seconds   float64 `json:"seconds"`
	Scenarios []struct {
		ID      string         `json:"id"`
		Pass    bool           `json:"pass"`
		Error   string         `json:"error"`
		Seconds float64        `json:"seconds"`
		Facts   map[string]int `json:"facts"`
	} `json:"scenarios"`
}

// scenarioSummary — сценарий в аналитике: status только "pass"|"fail";
// error: true — сценарий не доиграл из-за ошибки (стенд, раннер).
type scenarioSummary struct {
	Status  string         `json:"status"`
	Seconds float64        `json:"seconds"`
	Facts   map[string]int `json:"facts,omitempty"`
	Error   bool           `json:"error,omitempty"`
}

// taskOutcome — то, что прогон даёт аналитике сверх package-results.json.
type taskOutcome struct {
	scenarios map[string]scenarioSummary
	levels    map[string]string
}

func main() {
	candidateFlag := flag.String("candidate", "", "каталог кандидата (форк студента)")
	outFlag := flag.String("out", "", "каталог результатов")
	baselineFlag := flag.String("baseline", ".", "каталог репозитория курса")
	paramsFlag := flag.String("params", "", "файл параметров сценариев вместо открытых (вне репозитория)")
	flag.Parse()
	if *candidateFlag == "" || *outFlag == "" {
		fmt.Fprintln(os.Stderr, "usage: grade --baseline DIR --candidate DIR --out DIR [--params FILE]")
		os.Exit(2)
	}
	params := ""
	if *paramsFlag != "" {
		params = mustAbs(*paramsFlag)
		if err := checkParams(params); err != nil {
			fatal(fmt.Errorf("--params: %w", err))
		}
	}
	baseline, candidate, out := mustAbs(*baselineFlag), mustAbs(*candidateFlag), mustAbs(*outFlag)
	if err := os.MkdirAll(out, 0o755); err != nil {
		fatal(err)
	}
	cfg, err := readConfig(filepath.Join(baseline, ".etc", "config.json"))
	if err != nil {
		fatal(err)
	}

	report := initialReport()
	outcome := taskOutcome{scenarios: map[string]scenarioSummary{}, levels: map[string]string{}}
	guard := guardResult{CheckCode: "1", Report: map[string]any{}}
	policy, cleanup, how := policyBase(baseline, candidate)
	changed, err := checkPolicy(policy, candidate, cfg)
	cleanup()
	if err != nil {
		guard.Report["error"] = err.Error()
		guard.Report["compared_with"] = how
	} else {
		guard = guardResult{CheckCode: "0", Report: map[string]any{"changed_paths": changed, "compared_with": how}}
		report, outcome = runTasks(baseline, candidate, out, params, cfg)
	}

	writeJSON(filepath.Join(out, "package-results.json"), report)
	writeJSON(filepath.Join(out, "change-policy-result.json"), guard)
	payload := map[string]any{
		"schema": "github-actions-analytics-v2",
		"stream": cfg.Stream,
		"github": map[string]string{
			"repository": os.Getenv("CANDIDATE_REPOSITORY"), "sha": os.Getenv("CANDIDATE_SHA"),
			"actor": os.Getenv("GITHUB_ACTOR"), "run_id": os.Getenv("GITHUB_RUN_ID"), "run_attempt": os.Getenv("GITHUB_RUN_ATTEMPT"),
			"workflow_repository": os.Getenv("GITHUB_REPOSITORY"), "event": os.Getenv("GITHUB_EVENT_NAME"),
		},
		"baseline": map[string]string{"repo": cfg.Diff.Original.Repo, "ref": cfg.Diff.Original.Ref},
		"config": map[string]any{"allow_list_count": 1, "tasks_enabled": len(cfg.Tasks),
			"tasks_optional": append([]string{}, cfg.Optional...)},
		"guard":       guard,
		"test_report": report,
		"scenarios":   outcome.scenarios,
		"levels":      outcome.levels,
	}
	writeJSON(filepath.Join(out, "analytics.json"), payload)
	printSummary(guard, report, out)
	if guard.CheckCode != "0" || !allEnabledPass(report, cfg) {
		os.Exit(1)
	}
}

func readConfig(path string) (config, error) {
	var c config
	b, err := os.ReadFile(path)
	if err != nil {
		return c, err
	}
	if err := json.Unmarshal(b, &c); err != nil {
		return c, err
	}
	if c.Diff.AllowPrefix != "orders/" {
		return c, errors.New("invalid instructor allow_prefix")
	}
	for _, t := range c.Tasks {
		if !contains(allTasks, t) {
			return c, fmt.Errorf("unknown task in config: %s", t)
		}
	}
	for _, t := range c.Optional {
		if !contains(c.Tasks, t) {
			return c, fmt.Errorf("optional task not enabled: %s", t)
		}
	}
	return c, nil
}

func initialReport() map[string]packageResult {
	r := make(map[string]packageResult, len(allTasks))
	for _, t := range allTasks {
		r[reportPrefix+t] = packageResult{Status: "missing"}
	}
	return r
}

// allEnabledPass — прошли все включённые обязательные задания.
func allEnabledPass(report map[string]packageResult, cfg config) bool {
	for _, t := range cfg.Tasks {
		if contains(cfg.Optional, t) {
			continue
		}
		if report[reportPrefix+t].Status != "pass" {
			return false
		}
	}
	return true
}

func printSummary(guard guardResult, report map[string]packageResult, out string) {
	if guard.CheckCode != "0" {
		// Сообщение содержит имена файлов студента: без управляющих символов,
		// чтобы не подсунуть команду в лог Actions.
		fmt.Printf("file policy violated: %s\n", strings.Map(dropControl, fmt.Sprint(guard.Report["error"])))
	}
	for _, t := range allTasks {
		fmt.Printf("%s: %s\n", t, report[reportPrefix+t].Status)
	}
	fmt.Println("итог проверки — по обязательным заданиям; необязательные на него не влияют")
	fmt.Printf("results: %s\n", out)
}

// ---------- правило файлов ----------

// policyBase — с чем сравнивать кандидата по правилу файлов: дерево
// репозитория курса в той точке, от которой отходит кандидат (merge-base его
// HEAD и HEAD baseline). Так обновление стенда в репозитории курса не делает
// запрещёнными файлы, которые студент не трогал, а только не успел
// синхронизировать форк. Нет истории git или общей точки — сам baseline.
// how — с чем сравнили (для отчёта).
func policyBase(baseline, candidate string) (dir string, cleanup func(), how string) {
	fallback := func(why string) (string, func(), string) {
		return baseline, func() {}, "baseline (" + why + ")"
	}
	head, err := gitOut(baseline, "rev-parse", "HEAD")
	if err != nil {
		return fallback("нет истории репозитория курса")
	}
	const ref = "refs/soa-grade/upstream"
	if _, err := gitOut(candidate, "fetch", "--quiet", "--no-tags", baseline, "+HEAD:"+ref); err != nil {
		return fallback("нет истории кандидата")
	}
	defer func() { _, _ = gitOut(candidate, "update-ref", "-d", ref) }()
	mb, err := gitOut(candidate, "merge-base", "HEAD", ref)
	if err != nil {
		return fallback("нет общей точки с репозиторием курса")
	}
	if mb == head {
		return baseline, func() {}, "upstream " + short(mb)
	}
	tmp, err := os.MkdirTemp("", "soa-policy-")
	if err != nil {
		return fallback(err.Error())
	}
	if err := gitArchive(candidate, mb, tmp); err != nil {
		os.RemoveAll(tmp)
		return fallback("git archive: " + err.Error())
	}
	return tmp, func() { os.RemoveAll(tmp) }, "upstream " + short(mb) + " (merge-base)"
}

// gitArgs — без хуков и с локальными путями: кандидат — только данные.
var gitArgs = []string{"-c", "core.hooksPath=/dev/null", "-c", "protocol.file.allow=always"}

func gitOut(dir string, args ...string) (string, error) {
	cmd := exec.Command("git", append(append([]string{"-C", dir}, gitArgs...), args...)...)
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	out, err := cmd.Output()
	return strings.TrimSpace(string(out)), err
}

// gitArchive раскладывает дерево коммита rev в dir: только каталоги и
// обычные файлы.
func gitArchive(repo, rev, dir string) error {
	cmd := exec.Command("git", append(append([]string{"-C", repo}, gitArgs...), "archive", "--format=tar", rev)...)
	out, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	tr := tar.NewReader(out)
	var extractErr error
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			extractErr = err
			break
		}
		name := filepath.FromSlash(h.Name)
		if !filepath.IsLocal(name) {
			extractErr = fmt.Errorf("путь вне дерева: %q", h.Name)
			break
		}
		target := filepath.Join(dir, name)
		switch h.Typeflag {
		case tar.TypeDir:
			extractErr = os.MkdirAll(target, 0o755)
		case tar.TypeReg:
			extractErr = writeFrom(target, tr)
		case tar.TypeXGlobalHeader:
		default:
			extractErr = fmt.Errorf("не обычный файл: %q", h.Name)
		}
		if extractErr != nil {
			break
		}
	}
	_, _ = io.Copy(io.Discard, out)
	if err := cmd.Wait(); err != nil && extractErr == nil {
		extractErr = err
	}
	return extractErr
}

func writeFrom(path string, r io.Reader) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, r); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func short(sha string) string {
	if len(sha) > 12 {
		return sha[:12]
	}
	return sha
}

// checkPolicy: кандидат отличается от baseline только внутри orders/.
// .gitignore может отличаться свободно — в проверку он не попадает.
// Символические ссылки и не-обычные файлы запрещены везде.
func checkPolicy(baseline, candidate string, cfg config) ([]string, error) {
	a, err := snapshot(baseline)
	if err != nil {
		return nil, err
	}
	b, err := snapshot(candidate)
	if err != nil {
		return nil, err
	}
	paths := map[string]bool{}
	for p := range a {
		paths[p] = true
	}
	for p := range b {
		paths[p] = true
	}
	changed := make([]string, 0)
	var forbidden []string
	for p := range paths {
		if a[p] == b[p] || p == ".gitignore" {
			continue
		}
		if !strings.HasPrefix(p, cfg.Diff.AllowPrefix) {
			forbidden = append(forbidden, p)
			continue
		}
		changed = append(changed, p)
	}
	if len(forbidden) > 0 {
		return nil, forbiddenError(forbidden)
	}
	sort.Strings(changed)
	return changed, nil
}

// maxListed — сколько запрещённых путей перечислять в сообщении.
const maxListed = 20

// forbiddenError перечисляет запрещённые пути по алфавиту (первые maxListed).
func forbiddenError(paths []string) error {
	sort.Strings(paths)
	shown := paths
	if len(shown) > maxListed {
		shown = shown[:maxListed]
	}
	quoted := make([]string, len(shown))
	for i, p := range shown {
		quoted[i] = strconv.Quote(p)
	}
	msg := fmt.Sprintf("forbidden changes (%d): %s", len(paths), strings.Join(quoted, ", "))
	if rest := len(paths) - len(shown); rest > 0 {
		msg += fmt.Sprintf(" … ещё %d", rest)
	}
	return errors.New(msg)
}

func snapshot(root string) (map[string]string, error) {
	result := map[string]string{}
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if rel == ".git" || strings.HasPrefix(rel, ".git/") {
			return filepath.SkipDir
		}
		if d.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink forbidden: %q", rel)
		}
		if d.IsDir() {
			return nil
		}
		if !d.Type().IsRegular() {
			return fmt.Errorf("non-regular file: %q", rel)
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		h := sha256.New()
		if _, err := io.Copy(h, f); err != nil {
			return err
		}
		result[rel] = hex.EncodeToString(h.Sum(nil))
		return nil
	})
	return result, err
}

// ---------- прогон ----------

// runTasks собирает рабочую копию (baseline без orders/ + orders/ кандидата)
// и гоняет сценарии каждого включённого задания.
//
// scenarios — плоский блок для аналитики ({"S1": {...}, ..., "S5": {...}}):
// сервис аналитики читает scenarios.<id>.status с корня. Сценарий, который
// гоняется в нескольких заданиях (S1), берётся из первого по порядку.
// levels — уровень каждого включённого задания из результатов раннера.
func runTasks(baseline, candidate, out, params string, cfg config) (map[string]packageResult, taskOutcome) {
	report := initialReport()
	outcome := taskOutcome{scenarios: map[string]scenarioSummary{}, levels: map[string]string{}}
	for _, t := range cfg.Tasks {
		report[reportPrefix+t] = packageResult{Status: "unknown"}
		outcome.levels[t] = ""
	}

	work, err := os.MkdirTemp("", "soa-grade-")
	if err != nil {
		return report, outcome
	}
	defer os.RemoveAll(work)
	if err := copyTree(baseline, work, func(rel string) bool {
		return rel == ".git" || rel == "orders" || strings.HasPrefix(rel, ".git/")
	}); err != nil {
		fmt.Fprintln(os.Stderr, "copy baseline:", err)
		return report, outcome
	}
	if err := copyTree(filepath.Join(candidate, "orders"), filepath.Join(work, "orders"), nil); err != nil {
		fmt.Fprintln(os.Stderr, "copy orders:", err)
		return report, outcome
	}
	paramsArg := "PARAMS="
	if params != "" {
		// Копия в рабочей копии: файл не меняется посреди прогона и лежит
		// в каталоге, который Docker Desktop монтирует.
		dst := filepath.Join(work, ".grade-params.json")
		if err := copyFile(params, dst); err != nil {
			fmt.Fprintln(os.Stderr, "copy params:", err)
			return report, outcome
		}
		paramsArg = "PARAMS=" + dst
	}

	env := append(cleanEnv(os.Environ()),
		"COMPOSE_PROJECT_NAME=soa-grade",
		"ORDERS_PORT=18080", "MOCKS_PORT=18090",
		"MOCKS_ADMIN_TOKEN="+randomToken(),
		"ORDERS_DIR=", "PARAMS=", "ONLY=", "WAIT=", "RESULTS=",
	)
	defer run(work, env, 5*time.Minute, "docker", "compose", "--profile", "runner", "down", "--remove-orphans", "--volumes")

	for _, t := range cfg.Tasks {
		num := strings.TrimPrefix(t, "task_")
		logPath := filepath.Join(out, t+".log")
		resDir := filepath.Join(out, t)
		_ = os.MkdirAll(resDir, 0o777)
		_ = os.Chmod(resDir, 0o777) // раннер в контейнере пишет не от нашего uid
		// Результаты прошлого прогона в том же --out не должны подхватиться.
		removeStale(resDir)

		// Сборка образа студента отдельно: её ошибка — провал задания.
		buildOut, code := run(work, env, 20*time.Minute, "docker", "compose", "build", "orders")
		if code != 0 {
			_ = os.WriteFile(logPath, append([]byte("orders/Dockerfile: сборка не удалась\n\n"), buildOut...), 0o644)
			report[reportPrefix+t] = packageResult{Status: "fail", ExitCode: code}
			continue
		}
		output, code := run(work, env, 30*time.Minute, "make", "--no-print-directory", "scenarios",
			"TASK="+num, "RESULTS="+resDir, "ONLY=", "WAIT=", "STAND_INFO=", paramsArg)
		_ = os.WriteFile(logPath, output, 0o644)

		status, rr := taskStatus(filepath.Join(resDir, t+".json"), code)
		report[reportPrefix+t] = packageResult{Status: status, ExitCode: code}
		if rr == nil {
			continue
		}
		outcome.levels[t] = rr.Level
		for _, s := range rr.Scenarios {
			if _, seen := outcome.scenarios[s.ID]; seen {
				continue
			}
			sum := scenarioSummary{Status: "fail", Seconds: round1(s.Seconds), Facts: s.Facts, Error: s.Error != ""}
			if s.Pass && s.Error == "" {
				sum.Status = "pass"
			}
			outcome.scenarios[s.ID] = sum
		}
	}
	return report, outcome
}

// taskStatus читает результаты раннера. rr == nil — раннер не дошёл до
// записи результатов (чаще всего сервис не поднялся) или файл битый.
func taskStatus(path string, code int) (string, *runnerResults) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "fail", nil
	}
	var rr runnerResults
	if json.Unmarshal(b, &rr) != nil {
		return "fail", nil
	}
	if code != 0 || rr.Error != "" || rr.Total == 0 || rr.Passed != rr.Total {
		return "fail", &rr
	}
	for _, s := range rr.Scenarios {
		if !s.Pass || s.Error != "" {
			return "fail", &rr
		}
	}
	return "pass", &rr
}

// removeStale удаляет *.json, *.txt и *.html прошлого прогона из каталога результатов.
func removeStale(dir string) {
	for _, pat := range []string{"*.json", "*.txt", "*.html"} {
		old, _ := filepath.Glob(filepath.Join(dir, pat))
		for _, f := range old {
			_ = os.Remove(f)
		}
	}
}

// cleanEnv убирает переменные родительского make: иначе `make grade ONLY=S1`
// через MAKEFLAGS дотянется до вложенного make scenarios.
func cleanEnv(env []string) []string {
	out := make([]string, 0, len(env))
	for _, kv := range env {
		name, _, _ := strings.Cut(kv, "=")
		switch name {
		case "MAKEFLAGS", "MFLAGS", "GNUMAKEFLAGS", "MAKELEVEL", "MAKEOVERRIDES":
			continue
		}
		out = append(out, kv)
	}
	return out
}

// checkParams: обычный файл с JSON-объектом.
func checkParams(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%s: не обычный файл", path)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var v map[string]any
	if err := json.Unmarshal(b, &v); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	return nil
}

func run(dir string, env []string, timeout time.Duration, args ...string) ([]byte, int) {
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Dir = dir
	cmd.Env = env
	done := make(chan struct{})
	var output []byte
	var err error
	go func() {
		output, err = cmd.CombinedOutput()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		_ = cmd.Process.Kill()
		<-done
		return append(output, []byte("\ntimeout\n")...), 124
	}
	if err == nil {
		return output, 0
	}
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		return output, exit.ExitCode()
	}
	return append(output, []byte("\n"+err.Error())...), 124
}

// ---------- мелочи ----------

func copyTree(from, to string, skip func(rel string) bool) error {
	return filepath.WalkDir(from, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(from, path)
		rel = filepath.ToSlash(rel)
		if rel != "." && skip != nil && skip(rel) {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		target := filepath.Join(to, filepath.FromSlash(rel))
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		if !d.Type().IsRegular() {
			return fmt.Errorf("non-regular file: %s", rel)
		}
		return copyFile(path, target)
	})
}

func copyFile(from, to string) error {
	if err := os.MkdirAll(filepath.Dir(to), 0o755); err != nil {
		return err
	}
	in, err := os.Open(from)
	if err != nil {
		return err
	}
	defer in.Close()
	info, err := in.Stat()
	if err != nil {
		return err
	}
	out, err := os.OpenFile(to, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, info.Mode().Perm())
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = io.Copy(out, in)
	return err
}

func randomToken() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// dropControl — для strings.Map: убирает управляющие символы, включая перевод строки.
func dropControl(r rune) rune {
	if unicode.IsControl(r) {
		return -1
	}
	return r
}

func round1(f float64) float64 { return float64(int(f*10+0.5)) / 10 }

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func mustAbs(p string) string {
	a, err := filepath.Abs(p)
	if err != nil {
		fatal(err)
	}
	return a
}

func writeJSON(path string, v any) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		fatal(err)
	}
	if err := os.WriteFile(path, append(b, '\n'), 0o644); err != nil {
		fatal(err)
	}
}

func fatal(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(2) }
