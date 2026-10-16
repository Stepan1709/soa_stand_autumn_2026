#!/bin/sh
# Перезапуск Orders по просьбе раннера (S7, S8 задания 2). Запускает make
# scenarios рядом с раннером: раннер в контейнере кладёт файл kill в каталог
# $1 (в нём — на сколько секунд остановить Orders), этот цикл на хосте
# убивает контейнер orders сигналом KILL, ждёт, запускает его снова и
# отвечает файлом done. Файл stop — конец работы.
# Остальные аргументы — команда compose (docker compose).
dir=$1
shift
# id контейнера — заранее: docker kill по id быстрее, чем через compose.
id=$("$@" ps -q orders 2>/dev/null)
while [ ! -e "$dir/stop" ]; do
  if [ -e "$dir/kill" ]; then
    down=$(cat "$dir/kill" 2>/dev/null)
    rm -f "$dir/kill"
    case $down in '' | *[!0-9]*) down=5 ;; esac
    [ -n "$id" ] || id=$("$@" ps -q orders 2>/dev/null)
    if [ -n "$id" ]; then
      docker kill -s KILL "$id" >/dev/null 2>&1
    else
      "$@" kill -s KILL orders >/dev/null 2>&1
    fi
    sleep "$down"
    "$@" start orders >/dev/null 2>&1
    : >"$dir/done"
  fi
  sleep 0.2
done
