# Учебный стенд. Нужно: Docker с Compose v2, make. Список команд — make help.

TASK   ?= 1
ONLY   ?=
WAIT   ?=
PARAMS ?=
# RESULTS — каталог результатов раннера: task_N.json (для cmd/grade), task_N.txt
# (вывод) и task_N.html (отчёт). Пустой (RESULTS=) — не писать.
RESULTS ?= results
# Порты на хосте: заглушки и дашборд, документация контрактов.
MOCKS_PORT ?= 8090
DOCS_PORT ?= 8091
# STAND_INFO — печатать после make up / make scenarios подсказку про дашборд и
# токен; пустой (STAND_INFO=, так делает cmd/grade) — не печатать.
STAND_INFO ?= 1

# Адрес репозитория стенда — в одном месте: .etc/config.json.
UPSTREAM_REPO := $(shell sed -n 's/.*"repo": *"\([^"]*\)".*/\1/p' .etc/config.json)

# Токен к /admin/* заглушек: его знают mocks, runner и дашборд (make ui), ваш
# сервис — нет. Если не задан, берётся из файла .mocks-token (создаётся один раз
# случайным, права 600, в git не попадает): ссылка на дашборд живёт между
# запусками make. Заданный извне (cmd/grade задаёт свой) уважается, пустой
# (make up MOCKS_ADMIN_TOKEN=) — /admin/* открыт, например чтобы крутить режимы
# заглушек руками.
TOKEN_FILE := .mocks-token
ifeq ($(origin MOCKS_ADMIN_TOKEN),undefined)
MOCKS_ADMIN_TOKEN := $(shell [ -s $(TOKEN_FILE) ] || (umask 077; od -An -N16 -tx1 /dev/urandom | tr -d ' \n' >$(TOKEN_FILE)); cat $(TOKEN_FILE) 2>/dev/null)
ifeq ($(strip $(MOCKS_ADMIN_TOKEN)),)
$(error не удалось создать токен в $(TOKEN_FILE) (нужны od и /dev/urandom); удалите файл и запустите снова или задайте MOCKS_ADMIN_TOKEN=...)
endif
endif
export MOCKS_ADMIN_TOKEN

# \# внутри $(if …) make ≥ 4.3 оставляет с обратной косой чертой: решётка —
# через переменную.
H := \#
UI_URL := http://localhost:$(MOCKS_PORT)/ui$(if $(MOCKS_ADMIN_TOKEN),$(H)token=$(MOCKS_ADMIN_TOKEN))
DOCS_URL := http://localhost:$(DOCS_PORT)

# open_url — открыть ссылку в браузере, если получится; ошибки не валят make.
# macOS — open; WSL — wslview или explorer.exe; Linux — xdg-open; иначе только
# печать ссылки.
open_url = if [ "$$(uname -s)" = Darwin ] && command -v open >/dev/null 2>&1; then open '$(1)' >/dev/null 2>&1 || true; \
	elif grep -qi microsoft /proc/version 2>/dev/null; then \
	  if command -v wslview >/dev/null 2>&1; then wslview '$(1)' >/dev/null 2>&1 || true; \
	  elif command -v explorer.exe >/dev/null 2>&1; then explorer.exe '$(1)' >/dev/null 2>&1 || true; fi; \
	elif command -v xdg-open >/dev/null 2>&1; then xdg-open '$(1)' >/dev/null 2>&1 </dev/null || true; fi

# stand_info — после make up и make scenarios: дашборд и токен (STAND_INFO=
# — не печатать).
stand_info = $(if $(STAND_INFO),echo ""; echo "Дашборд и кнопки управления стендом: make ui"; \
	echo "$(if $(MOCKS_ADMIN_TOKEN),Для своих экспериментов с /admin/modes — токен: make token,/admin/* открыт без токена: стенд поднят с пустым MOCKS_ADMIN_TOKEN)",true)

# Вывод cmd/grade должен лежать вне репозитория: грейдер снимает слепок дерева.
GRADE_OUT ?= $(or $(TMPDIR),/tmp)/soa-grade

COMPOSE := docker compose

RUNNER_ARGS := -task $(TASK)
RUNNER_MOUNT :=
ifneq ($(strip $(ONLY)),)
RUNNER_ARGS += -only $(ONLY)
endif
ifneq ($(strip $(WAIT)),)
RUNNER_ARGS += -wait $(WAIT)
endif
ifneq ($(strip $(PARAMS)),)
RUNNER_MOUNT += -v $(abspath $(PARAMS)):/params.json:ro
RUNNER_ARGS += -params /params.json
endif
# CHAOS — каталог, через который раннер просит перезапустить Orders (S7, S8
# задания 2): runner/chaos.sh на хосте убивает контейнер orders и запускает
# снова. Только вместе с RESULTS: каталог смонтирован в контейнер раннера.
CHAOS_START :=
CHAOS_STOP :=
ifneq ($(strip $(RESULTS)),)
RUNNER_MOUNT += -v $(abspath $(RESULTS)):/results
RUNNER_ARGS += -results /results/task_$(TASK).json -chaos /results/.chaos
CHAOS_START = chaos='$(abspath $(RESULTS))/.chaos'; rm -rf "$$chaos"; mkdir -p "$$chaos"; chmod 777 "$$chaos"; \
	sh runner/chaos.sh "$$chaos" $(COMPOSE) & chaos_pid=$$!;
CHAOS_STOP = : >"$$chaos/stop"; wait $$chaos_pid 2>/dev/null; rm -rf "$$chaos";
endif

.PHONY: help up down logs ps scenarios dashboard-idle build grade sync ui token docs

help:
	@echo "make up                   поднять стенд вместе с вашим сервисом (база пересоздаётся)"
	@echo "make scenarios TASK=1     прогнать сценарии задания 1 (TASK=2 — необязательное задание 2: S1, S6–S8)"
	@echo "make scenarios ONLY=S3    один сценарий (или несколько: ONLY=S2,S5)"
	@echo "    WAIT=5                пауза после последнего заказа, с (по умолчанию 30) — для отладки"
	@echo "    PARAMS=file.json      подменить параметры сценариев"
	@echo "    RESULTS=dir           куда писать результаты (по умолчанию results/: .json, .txt, .html; S7 и S8 без RESULTS не идут)"
	@echo "    ORDERS_DIR=./examples/go  собрать Orders из другого каталога вместо orders/ (проверка всегда берёт orders/)"
	@echo "make ui                   дашборд заглушек с кнопками: режимы, заказы, прогоны сценариев"
	@echo "make token                токен к /admin/* заглушек одной строкой: T=\$$(make -s token)"
	@echo "make docs                 контракты в Swagger UI: http://localhost:$(DOCS_PORT)"
	@echo "make logs                 логи всех контейнеров"
	@echo "make down                 остановить стенд"
	@echo "make sync                 добавить remote upstream и сделать git fetch upstream"
	@echo "make grade                то же, что CI, локально: сверка файлов со своим деревом и оба задания (нужен Go 1.26)"
	@echo "make ps / make build      состояние контейнеров / пересобрать образы"

# postgres и orders пересоздаются: пустая база и свежий процесс на каждый прогон.
up: dashboard-idle
	$(COMPOSE) up -d --build --wait mocks
	$(COMPOSE) up -d --build --force-recreate --wait postgres
	$(COMPOSE) up -d --build --force-recreate --no-deps orders
	@$(if $(filter scenarios,$(MAKECMDGOALS)),true,$(stand_info))

build:
	$(COMPOSE) --profile runner build

# Прогон из дашборда (make ui) и make scenarios ходят в одни заглушки; кроме
# того, make up пересоздал бы Orders посреди чужого прогона. Проверка — до up:
# статус прогона из дашборда и признак прогона у заглушек (GET /admin/run —
# любой прогон, в том числе другой make scenarios).
ADMIN_CURL = curl -sf -m 3 $(if $(MOCKS_ADMIN_TOKEN),-H 'X-Admin-Token: $(MOCKS_ADMIN_TOKEN)')
dashboard-idle:
	@if command -v curl >/dev/null 2>&1 && { $(ADMIN_CURL) 'http://localhost:$(MOCKS_PORT)/admin/runner/status' 2>/dev/null | grep -q '"state":"running"' || \
	  $(ADMIN_CURL) 'http://localhost:$(MOCKS_PORT)/admin/run' 2>/dev/null | grep -q '"runner_active":true'; }; then \
	  echo "Сейчас идёт прогон сценариев (из дашборда или другой make scenarios): прогоны ходят в одни и те же заглушки."; \
	  echo "Дождитесь его конца и запустите команду снова."; exit 2; fi

scenarios: dashboard-idle up
	$(COMPOSE) --profile runner build runner
# Раннер в контейнере работает не от вашего uid: каталогу RESULTS нужна запись для всех.
ifneq ($(strip $(RESULTS)),)
	mkdir -p '$(abspath $(RESULTS))'
	chmod 777 '$(abspath $(RESULTS))'
	rm -f '$(abspath $(RESULTS))/task_$(TASK).json' '$(abspath $(RESULTS))/task_$(TASK).txt' '$(abspath $(RESULTS))/task_$(TASK).html'
endif
	@echo '$(COMPOSE) run --rm --no-deps $(RUNNER_MOUNT) runner $(RUNNER_ARGS)'
	@status=0; $(CHAOS_START) $(COMPOSE) run --rm --no-deps $(RUNNER_MOUNT) runner $(RUNNER_ARGS) || status=$$?; $(CHAOS_STOP) \
	$(if $(strip $(RESULTS)),if [ -f '$(RESULTS)/task_$(TASK).html' ]; then echo ""; echo 'Отчёт: $(RESULTS)/task_$(TASK).html'; fi;) \
	$(stand_info); exit $$status

logs:
	$(COMPOSE) logs -f --tail=200

ps:
	$(COMPOSE) ps

down:
	$(COMPOSE) --profile runner --profile ui --profile docs down --remove-orphans

# make ui поднимает и сервис прогонов из дашборда (runner-ui): кнопки
# «Прогнать» на дашборде. Результаты — в results/, как у make scenarios.
ui:
	mkdir -p results
	chmod 777 results
	@# Заглушки пересоздаются, только если запущены с другим токеном (например, из другой копии стенда).
	$(COMPOSE) up -d --build --wait mocks
	$(COMPOSE) --profile ui up -d --build runner-ui
	@echo ""
	@echo "Дашборд: $(UI_URL)"
	@echo "(стенд должен быть поднят: make up; пока идёт прогон из дашборда, make scenarios не запустится)"
	@$(call open_url,$(UI_URL))

token:
	@echo '$(MOCKS_ADMIN_TOKEN)'

docs:
	$(COMPOSE) --profile docs up -d --wait docs
	@echo "Контракты: $(DOCS_URL)"
	@$(call open_url,$(DOCS_URL))

grade:
	go run ./cmd/grade --baseline . --candidate . --out $(GRADE_OUT)

sync:
	@git remote get-url upstream >/dev/null 2>&1 || git remote add upstream https://github.com/$(UPSTREAM_REPO).git
	git fetch upstream
