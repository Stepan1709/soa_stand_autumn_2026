// Command checkstatus — отдельный check task_N в PR: читает результаты
// cmd/grade и падает, если задание не прошло или нарушено правило файлов.
//
// Всё, что показывается, частично под контролем студента (имена файлов,
// вывод сервиса и сборки), поэтому: в stdout — внутри ::stop-commands::,
// в $GITHUB_STEP_SUMMARY — внутри <pre> с HTML-экранированием, и везде без
// управляющих символов.
package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"html"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// Сколько показывать: отчёт раннера — целиком (с запасом), журнал — хвост.
const (
	maxReportBytes = 64 << 10
	logTailLines   = 60
	logTailBytes   = 16 << 10
)

func main() {
	results := flag.String("results", "", "каталог результатов cmd/grade")
	task := flag.String("task", "", "задание, например task_1")
	cfgPath := flag.String("config", ".etc/config.json", "конфигурация стенда: какие задания необязательные")
	checkRun := flag.String("check-run", "", "необязательное задание: записать сюда тело запроса к Checks API (success или neutral) и не падать")
	flag.Parse()
	if *results == "" || *task == "" {
		fmt.Fprintln(os.Stderr, "usage: checkstatus --results DIR --task task_1")
		os.Exit(2)
	}
	var guard struct {
		CheckCode string         `json:"checkCode"`
		Report    map[string]any `json:"report"`
	}
	mustRead(filepath.Join(*results, "change-policy-result.json"), &guard)
	var report map[string]struct {
		Status string `json:"status"`
	}
	mustRead(filepath.Join(*results, "package-results.json"), &report)

	token := randomToken()
	summary := os.Getenv("GITHUB_STEP_SUMMARY")
	opt := optional(*cfgPath, *task)
	if guard.CheckCode != "0" {
		msg := fmt.Sprintf("нарушено правило файлов: %v\n\nМенять можно только orders/ (и .gitignore): верните остальные файлы как в исходном репозитории.", guard.Report["error"])
		fmt.Printf("%s: FAIL\n", *task)
		fmt.Print(stdoutBlock(token, msg))
		appendSummary(summary, summaryBlock(*task, "FAIL", "", msg))
		if *checkRun != "" && opt {
			// Необязательное задание и здесь — серый check, job не падает:
			// красным нарушение покажет check обязательного задания.
			writeCheckRun(*checkRun, *task, "fail", summaryBlock(*task, "FAIL", "", msg))
			return
		}
		os.Exit(1)
	}
	status := report["soa_stand/tasks/"+*task].Status
	body, note := runnerOutput(*results, *task)
	if opt {
		note = strings.TrimSpace("Задание необязательное: на итог проверки и допуск не влияет; если вы его не делали, «не пройдено» здесь ожидаемо. " + note)
	}
	if *checkRun != "" && opt {
		// Необязательное задание — отдельный check: зелёный, если пройдено,
		// иначе серый neutral; job при этом не падает.
		writeCheckRun(*checkRun, *task, status, summaryBlock(*task, strings.ToUpper(status), note, body))
		fmt.Printf("%s: %s (необязательное)\n", *task, sanitize(status))
		fmt.Print(stdoutBlock(token, body))
		appendSummary(summary, summaryBlock(*task, strings.ToUpper(status), note, body))
		return
	}
	fmt.Printf("%s: %s\n", *task, sanitize(status))
	if note != "" {
		fmt.Println(note)
	}
	fmt.Print(stdoutBlock(token, body))
	appendSummary(summary, summaryBlock(*task, strings.ToUpper(status), note, body))
	if status != "pass" {
		fmt.Printf("полный журнал: артефакт soa-results, файл %s.log\n", *task)
		os.Exit(1)
	}
}

// maxCheckSummary — предел output.summary в Checks API (65535) с запасом.
const maxCheckSummary = 60000

// writeCheckRun пишет тело POST /repos/{repo}/check-runs (без head_sha — его
// добавляет workflow): success, если задание пройдено, иначе neutral.
func writeCheckRun(path, task, status, summaryText string) {
	conclusion, title := "neutral", task+": не пройдено (необязательное)"
	if status == "pass" {
		conclusion, title = "success", task+": пройдено полностью"
	}
	if len(summaryText) > maxCheckSummary {
		summaryText = strings.ToValidUTF8(summaryText[:maxCheckSummary], "") + "\n…</pre>"
	}
	b, _ := json.Marshal(map[string]any{
		"name": task + " (необязательное)", "status": "completed", "conclusion": conclusion,
		"output": map[string]string{"title": title, "summary": summaryText},
	})
	if err := os.WriteFile(path, b, 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}

// optional — задание помечено необязательным в конфигурации стенда.
func optional(path, task string) bool {
	var cfg struct {
		Optional []string `json:"optional"`
	}
	b, err := os.ReadFile(path)
	if err != nil || json.Unmarshal(b, &cfg) != nil {
		return false
	}
	for _, t := range cfg.Optional {
		if t == task {
			return true
		}
	}
	return false
}

// runnerOutput — итоговый отчёт раннера (task_N/task_N.txt). Если его нет,
// раннер не дошёл до сценариев: показываем хвост полного журнала с пометкой.
func runnerOutput(results, task string) (body, note string) {
	if b, err := os.ReadFile(filepath.Join(results, task, task+".txt")); err == nil {
		s := string(b)
		if len(s) > maxReportBytes {
			s = "…\n" + s[len(s)-maxReportBytes:]
		}
		return strings.TrimSpace(sanitize(s)), ""
	}
	note = "Раннер не дошёл до сценариев: чаще всего не собрался образ orders/ " +
		"или сервис не ответил на /healthz. Ниже — конец журнала; полный — " +
		"артефакт soa-results, файл " + task + ".log."
	b, err := os.ReadFile(filepath.Join(results, task+".log"))
	if err != nil {
		return "(журнала нет)", note
	}
	return tail(sanitize(string(b)), logTailLines, logTailBytes), note
}

func tail(s string, lines, maxBytes int) string {
	s = strings.TrimRight(s, "\n")
	if len(s) > maxBytes {
		s = s[len(s)-maxBytes:]
		if i := strings.IndexByte(s, '\n'); i >= 0 {
			s = s[i+1:]
		}
		s = strings.ToValidUTF8(s, "")
	}
	parts := strings.Split(s, "\n")
	if len(parts) > lines {
		parts = parts[len(parts)-lines:]
	}
	return strings.Join(parts, "\n")
}

var ansiRe = regexp.MustCompile(`\x1b\[[0-9;?]*[ -/]*[@-~]|\x1b[@-_]`)

// sanitize оставляет печатный текст, \n и \t: убирает ANSI-последовательности,
// прочие управляющие символы (в том числе \r) и символы смены направления текста.
func sanitize(s string) string {
	s = strings.ToValidUTF8(s, "�")
	s = ansiRe.ReplaceAllString(s, "")
	return strings.Map(func(r rune) rune {
		switch {
		case r == '\n' || r == '\t':
			return r
		case r < 0x20 || r == 0x7f || (r >= 0x80 && r < 0xa0):
			return -1
		case r == 0x200e || r == 0x200f || (r >= 0x202a && r <= 0x202e) || (r >= 0x2066 && r <= 0x2069):
			return -1
		}
		return r
	}, s)
}

// stdoutBlock — текст для лога Actions: команды workflow внутри отключены.
// Токен случайный, поэтому текст не может сам закрыть блок.
func stdoutBlock(token, text string) string {
	return fmt.Sprintf("::stop-commands::%s\n%s\n::%s::\n", token, sanitize(text), token)
}

// summaryBlock — markdown для $GITHUB_STEP_SUMMARY: текст только внутри <pre>
// и HTML-экранирован, так что разметкой он стать не может.
func summaryBlock(task, status, note, text string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "### %s: %s\n\n", html.EscapeString(task), html.EscapeString(sanitize(status)))
	if note != "" {
		fmt.Fprintf(&b, "%s\n\n", html.EscapeString(note))
	}
	fmt.Fprintf(&b, "<pre>\n%s\n</pre>\n\n", html.EscapeString(sanitize(text)))
	return b.String()
}

func randomToken() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	return hex.EncodeToString(b[:])
}

func mustRead(path string, v any) {
	b, err := os.ReadFile(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if err := json.Unmarshal(b, v); err != nil {
		fmt.Fprintln(os.Stderr, path, err)
		os.Exit(2)
	}
}

func appendSummary(path, text string) {
	if path == "" {
		return
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	_, _ = f.WriteString(text)
}
