package main

// Перезапуск Orders в S7 и S8. Раннер работает в контейнере и к Docker
// доступа не имеет: он кладёт файл kill в каталог -chaos (это results/ на
// хосте), а цикл runner/chaos.sh, который make запускает рядом с раннером,
// убивает контейнер orders сигналом KILL, через restart_s секунд запускает
// его снова и отвечает файлом done.

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

var errNoChaos = errors.New("перезапуск Orders доступен только в make scenarios: S7 и S8 из дашборда не запускаются")

// chaosWait — сколько ждать, пока make перезапустит Orders, сверх restart_s.
var chaosWait = 60 * time.Second

// restartOrders убивает Orders (kill -9), через down секунд запускает снова
// и ждёт его /healthz. Возвращает момент, когда Orders убит.
func (r *runner) restartOrders(down float64) (time.Time, error) {
	if r.chaosDir == "" {
		return time.Time{}, errNoChaos
	}
	kill, done := filepath.Join(r.chaosDir, "kill"), filepath.Join(r.chaosDir, "done")
	_ = os.Remove(done)
	tmp := kill + ".tmp"
	secs := strconv.Itoa(int(down + 0.5))
	if err := os.WriteFile(tmp, []byte(secs), 0o666); err != nil {
		return time.Time{}, fmt.Errorf("перезапуск Orders: %w", err)
	}
	if err := os.Rename(tmp, kill); err != nil {
		return time.Time{}, fmt.Errorf("перезапуск Orders: %w", err)
	}
	at := time.Now()
	deadline := at.Add(seconds(down) + chaosWait)
	for {
		if _, err := os.Stat(done); err == nil {
			break
		}
		if time.Now().After(deadline) {
			_ = os.Remove(kill)
			return at, errors.New("make не перезапустил Orders (нет ответа от runner/chaos.sh)")
		}
		time.Sleep(100 * time.Millisecond)
	}
	_ = os.Remove(done)
	if err := r.waitHealthy("Orders после перезапуска", r.orders, 120*time.Second); err != nil {
		return at, err
	}
	return at, nil
}
