package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func tree(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for p, body := range files {
		full := filepath.Join(root, filepath.FromSlash(p))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func TestCheckPolicy(t *testing.T) {
	var cfg config
	cfg.Diff.AllowPrefix = "orders/"
	base := map[string]string{"mocks/main.go": "m", "orders/Dockerfile": "FROM x", "orders/app.py": "1", ".gitignore": "a"}
	b := tree(t, base)

	cases := []struct {
		name    string
		mutate  func(map[string]string)
		wantErr bool
	}{
		{"same", func(map[string]string) {}, false},
		{"orders changed", func(m map[string]string) { m["orders/app.py"] = "2"; m["orders/new.go"] = "x" }, false},
		{"orders file removed", func(m map[string]string) { delete(m, "orders/app.py") }, false},
		{"gitignore changed", func(m map[string]string) { m[".gitignore"] = "b" }, false},
		{"mocks changed", func(m map[string]string) { m["mocks/main.go"] = "hacked" }, true},
		{"new file outside", func(m map[string]string) { m["runner/params/task1.json"] = "{}" }, true},
		{"prefix trick", func(m map[string]string) { m["orders.yml"] = "x" }, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := map[string]string{}
			for k, v := range base {
				m[k] = v
			}
			tc.mutate(m)
			_, err := checkPolicy(b, tree(t, m), cfg)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestCheckPolicySymlink(t *testing.T) {
	var cfg config
	cfg.Diff.AllowPrefix = "orders/"
	b := tree(t, map[string]string{"orders/Dockerfile": "x"})
	c := tree(t, map[string]string{"orders/Dockerfile": "x"})
	if err := os.Symlink("/etc/passwd", filepath.Join(c, "orders", "link")); err != nil {
		t.Skip(err)
	}
	if _, err := checkPolicy(b, c, cfg); err == nil {
		t.Fatal("symlink accepted")
	}
}

func TestCheckPolicyListsAllForbidden(t *testing.T) {
	var cfg config
	cfg.Diff.AllowPrefix = "orders/"
	b := tree(t, map[string]string{"orders/Dockerfile": "x", "mocks/main.go": "m", "runner/main.go": "r"})
	c := tree(t, map[string]string{"orders/Dockerfile": "y", "mocks/main.go": "hacked", "runner/main.go": "hacked", "a.txt": "new"})
	_, err := checkPolicy(b, c, cfg)
	if err == nil {
		t.Fatal("forbidden changes accepted")
	}
	want := `forbidden changes (3): "a.txt", "mocks/main.go", "runner/main.go"`
	if err.Error() != want {
		t.Fatalf("err = %q, want %q", err, want)
	}
}

func TestForbiddenErrorTruncates(t *testing.T) {
	var paths []string
	for i := 25; i > 0; i-- {
		paths = append(paths, fmt.Sprintf("f%02d", i))
	}
	msg := forbiddenError(paths).Error()
	if !strings.HasPrefix(msg, `forbidden changes (25): "f01", "f02"`) {
		t.Fatalf("not sorted: %s", msg)
	}
	if !strings.Contains(msg, `"f20" … ещё 5`) || strings.Contains(msg, `"f21"`) {
		t.Fatalf("bad truncation: %s", msg)
	}
}

func TestForbiddenErrorQuotesControl(t *testing.T) {
	msg := forbiddenError([]string{"x\n::error::boom"}).Error()
	if strings.ContainsAny(msg, "\n\r") {
		t.Fatalf("control characters in message: %q", msg)
	}
}

func TestTaskStatus(t *testing.T) {
	dir := t.TempDir()
	write := func(body string) string {
		p := filepath.Join(dir, "task_1.json")
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	cases := []struct {
		name, body string
		code       int
		want       string
	}{
		{"pass", `{"passed":1,"total":1,"level":"полностью","scenarios":[{"id":"S1","pass":true}]}`, 0, "pass"},
		{"scenario failed", `{"passed":0,"total":1,"scenarios":[{"id":"S1","pass":false}]}`, 1, "fail"},
		{"stand error", `{"error":"orders не ответил","passed":0,"total":0,"scenarios":[]}`, 1, "fail"},
		{"stand error, exit 0", `{"error":"x","passed":1,"total":1,"scenarios":[{"id":"S1","pass":true}]}`, 0, "fail"},
		{"broken json", `{`, 0, "fail"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got, _ := taskStatus(write(tc.body), tc.code); got != tc.want {
				t.Fatalf("status = %s, want %s", got, tc.want)
			}
		})
	}
	if got, rr := taskStatus(filepath.Join(dir, "missing.json"), 0); got != "fail" || rr != nil {
		t.Fatalf("missing file: %s %v", got, rr)
	}
}

func TestRemoveStale(t *testing.T) {
	dir := tree(t, map[string]string{"task_1.json": "{}", "task_1.txt": "old", "task_1.html": "old", "keep.log": "x"})
	removeStale(dir)
	for name, want := range map[string]bool{"task_1.json": false, "task_1.txt": false, "task_1.html": false, "keep.log": true} {
		if _, err := os.Stat(filepath.Join(dir, name)); (err == nil) != want {
			t.Fatalf("%s: exists=%v, want %v", name, err == nil, want)
		}
	}
}

func TestCleanEnv(t *testing.T) {
	got := cleanEnv([]string{"PATH=/bin", "MAKEFLAGS=ONLY=S1", "MAKELEVEL=1", "HOME=/h"})
	if strings.Join(got, " ") != "PATH=/bin HOME=/h" {
		t.Fatalf("cleanEnv = %v", got)
	}
}

// gitRepo — репозиторий с файлами одним коммитом.
func gitRepo(t *testing.T, dir string, files map[string]string, msg string) {
	t.Helper()
	for p, body := range files {
		full := filepath.Join(dir, filepath.FromSlash(p))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for _, args := range [][]string{{"add", "-A"}, {"-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", msg}} {
		if out, err := exec.Command("git", append([]string{"-C", dir}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v %s", args, err, out)
		}
	}
}

func TestPolicyBaseMergeBase(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("нет git")
	}
	var cfg config
	cfg.Diff.AllowPrefix = "orders/"
	upstream := t.TempDir()
	if out, err := exec.Command("git", "init", "-q", upstream).CombinedOutput(); err != nil {
		t.Fatalf("%v %s", err, out)
	}
	gitRepo(t, upstream, map[string]string{"mocks/main.go": "v1", "orders/app.py": "naive"}, "stand v1")

	// Форк студента от v1: своё решение в orders/.
	fork := filepath.Join(t.TempDir(), "fork")
	if out, err := exec.Command("git", "clone", "-q", upstream, fork).CombinedOutput(); err != nil {
		t.Fatalf("%v %s", err, out)
	}
	gitRepo(t, fork, map[string]string{"orders/app.py": "solution"}, "решение")

	// Репозиторий курса обновил стенд, форк не синхронизирован.
	gitRepo(t, upstream, map[string]string{"mocks/main.go": "v2", "mocks/bus.go": "new"}, "stand v2")

	dir, cleanup, how := policyBase(upstream, fork)
	defer cleanup()
	if !strings.Contains(how, "merge-base") {
		t.Fatalf("сравнили с %s", how)
	}
	if changed, err := checkPolicy(dir, fork, cfg); err != nil || strings.Join(changed, ",") != "orders/app.py" {
		t.Fatalf("несинхронизированный форк: %v %v", changed, err)
	}
	// Без merge-base тот же форк был бы нарушителем.
	if _, err := checkPolicy(upstream, fork, cfg); err == nil {
		t.Fatal("сравнение с новым baseline не нашло отличий")
	}

	// Студент поменял файл стенда — нарушение и с merge-base.
	gitRepo(t, fork, map[string]string{"mocks/main.go": "hacked"}, "чужой файл")
	dir2, cleanup2, _ := policyBase(upstream, fork)
	defer cleanup2()
	if _, err := checkPolicy(dir2, fork, cfg); err == nil || !strings.Contains(err.Error(), "mocks/main.go") {
		t.Fatalf("изменение вне orders/ пропущено: %v", err)
	}
	if out, _ := exec.Command("git", "-C", fork, "show-ref").Output(); strings.Contains(string(out), "soa-grade") {
		t.Fatalf("служебная ссылка осталась: %s", out)
	}

	// Не git — сравнение с baseline, как раньше.
	plain := tree(t, map[string]string{"orders/app.py": "x"})
	if dir, cleanup, how := policyBase(plain, plain); dir != plain || !strings.HasPrefix(how, "baseline") {
		t.Fatalf("без git: %s %s", dir, how)
	} else {
		cleanup()
	}
}
