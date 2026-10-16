package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestSanitize(t *testing.T) {
	in := "ok\r\n\x1b[31mred\x1b[0m\x00\x07 ‮evil\ttab\n"
	want := "ok\nred evil\ttab\n"
	if got := sanitize(in); got != want {
		t.Fatalf("sanitize = %q, want %q", got, want)
	}
}

func TestStdoutBlockStopsCommands(t *testing.T) {
	out := stdoutBlock("tok", "line\n::error::injected\r\n::tok2::")
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	if lines[0] != "::stop-commands::tok" || lines[len(lines)-1] != "::tok::" {
		t.Fatalf("bad framing: %q", out)
	}
	if strings.Count(out, "::tok::") != 1 {
		t.Fatalf("text closes block early: %q", out)
	}
}

func TestSummaryBlockEscapes(t *testing.T) {
	s := summaryBlock("task_1", "FAIL", "", "</pre><img src=x onerror=alert(1)>\n```\n[link](http://x)")
	if strings.Count(s, "<pre>") != 1 || strings.Count(s, "</pre>") != 1 {
		t.Fatalf("pre not closed exactly once: %s", s)
	}
	if strings.Contains(s, "<img") {
		t.Fatalf("html not escaped: %s", s)
	}
}

func TestRunnerOutput(t *testing.T) {
	dir := t.TempDir()
	var log strings.Builder
	for i := 0; i < 100; i++ {
		log.WriteString("line\n")
	}
	log.WriteString("last \x1b[1mline\x1b[0m\n")
	if err := os.WriteFile(filepath.Join(dir, "task_1.log"), []byte(log.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	body, note := runnerOutput(dir, "task_1")
	if note == "" || !strings.HasSuffix(body, "last line") || strings.Count(body, "\n") != logTailLines-1 {
		t.Fatalf("log fallback: note=%q body=%q", note, body)
	}

	if err := os.MkdirAll(filepath.Join(dir, "task_1"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "task_1", "task_1.txt"), []byte("Задание 1\nS1 PASS\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	body, note = runnerOutput(dir, "task_1")
	if note != "" || body != "Задание 1\nS1 PASS" {
		t.Fatalf("report: note=%q body=%q", note, body)
	}
}

func TestWriteCheckRun(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "config.json")
	os.WriteFile(cfg, []byte(`{"tasks":["task_1","task_2"],"optional":["task_2"]}`), 0o644)
	if !optional(cfg, "task_2") || optional(cfg, "task_1") || optional(filepath.Join(dir, "нет"), "task_2") {
		t.Fatal("optional")
	}
	for status, want := range map[string]string{"pass": "success", "fail": "neutral", "unknown": "neutral"} {
		p := filepath.Join(dir, status+".json")
		writeCheckRun(p, "task_2", status, strings.Repeat("ж", maxCheckSummary))
		var got struct {
			Name, Status, Conclusion string
			Output                   struct{ Title, Summary string }
		}
		b, _ := os.ReadFile(p)
		if err := json.Unmarshal(b, &got); err != nil {
			t.Fatal(err)
		}
		if got.Conclusion != want || got.Status != "completed" || got.Name != "task_2 (необязательное)" ||
			len(got.Output.Summary) > maxCheckSummary+16 || !utf8.ValidString(got.Output.Summary) {
			t.Fatalf("%s: %+v (summary %d байт)", status, got, len(got.Output.Summary))
		}
	}
}
