"""Orders — оформление заказов, устойчивое к отказам Payment.

Идея:
* Заказ сразу записывается в Postgres со статусом `pending`; вся дальнейшая
  работа идёт по состоянию из базы, поэтому её можно повторять и подхватывать
  после падения сервиса.
* Резерв в Inventory и оплата в Payment идемпотентны: резерв определяется
  `order_id`, оплата — заголовком `Idempotency-Key: <order_id>`. Повтор после
  таймаута, 500 или потерянного ответа не может списать деньги второй раз.
* `POST /orders` ждёт результат не дольше REPLY_BUDGET_S и отвечает тем, что
  известно (`pending` допустим); обработка продолжается в фоне.
* Фоновый воркер добирает `pending`-заказы с экспоненциальным backoff.
* Circuit breaker и лимит одновременных вызовов не дают заваливать
  недоступный Payment запросами.
"""

import asyncio
import collections
import json
import logging
import os
import random
import time
import uuid
from contextlib import asynccontextmanager

import httpx
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

PORT = int(os.environ.get("PORT", "8080"))
DATABASE_URL = os.environ["DATABASE_URL"]
PAYMENT_URL = os.environ["PAYMENT_URL"].rstrip("/")
INVENTORY_URL = os.environ["INVENTORY_URL"].rstrip("/")

REPLY_BUDGET_S = 0.6        # сколько POST /orders ждёт обработку (лимит задания — 1 с)
PAYMENT_TIMEOUT_S = 6.0     # таймаут вызова Payment (фон, на ответ клиенту не влияет)
INVENTORY_TIMEOUT_S = 3.0
CONCURRENCY_MIN = 1         # одновременных вызовов Payment: после таймаута
CONCURRENCY_START = 4       # ... на старте
CONCURRENCY_MAX = 16        # ... потолок; растёт на 1 за каждый ответ
LOCK_S = 30                 # на сколько заказ закрепляется за обработчиком
WORKER_TICK_S = 0.25
WORKER_BATCH = 10

BREAKER_HARD_THRESHOLD = 3  # подряд таймаутов/отказов соединения до размыкания
BREAKER_WINDOW = 20         # окно последних ответов для оценки доли ошибок
BREAKER_MIN_SAMPLES = 10
BREAKER_ERROR_RATE = 0.9    # доля ошибок в окне, после которой размыкаем
BREAKER_COOLDOWN_S = 3.0    # пауза до пробного запроса, удваивается при неудаче пробы
BREAKER_COOLDOWN_MAX_S = 15.0

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("orders")

SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
    id              TEXT PRIMARY KEY,
    user_id         TEXT NOT NULL,
    status          TEXT NOT NULL,
    amount_cents    BIGINT NOT NULL,
    items           JSONB NOT NULL,
    reserved        BOOLEAN NOT NULL DEFAULT false,
    payment_state   TEXT,
    attempts        INT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_until    TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS orders_pending_idx
    ON orders (next_attempt_at) WHERE status = 'pending';
"""

pool: AsyncConnectionPool
http: httpx.AsyncClient
background: set[asyncio.Task] = set()


# --- circuit breaker -------------------------------------------------------

class Breaker:
    """closed — звоним; open — не звоним до open_until; half_open — идёт один пробный вызов.

    Размыкается в двух случаях: подряд несколько вызовов остались без ответа
    (таймаут, нет соединения — Payment «лежит») или почти все ответы в окне —
    ошибки. Редкие 500 (треть, половина запросов) его не размыкают: там
    помогает повтор с backoff, а Payment отвечает быстро.
    """

    def __init__(self):
        self.state = "closed"
        self.hard_failures = 0
        self.window = collections.deque(maxlen=BREAKER_WINDOW)
        self.cooldown = BREAKER_COOLDOWN_S
        self.open_until = 0.0

    def claim_limit(self):
        """Сколько заказов воркеру имеет смысл брать сейчас."""
        if self.state == "closed":
            return WORKER_BATCH
        if self.state == "open" and time.monotonic() >= self.open_until:
            return 1
        return 0

    def allow(self):
        if self.state == "closed":
            return True
        if self.state == "open" and time.monotonic() >= self.open_until:
            self.state = "half_open"
            return True
        return False

    def success(self):
        if self.state != "closed":
            log.info("breaker: closed")
        self.state = "closed"
        self.hard_failures = 0
        self.cooldown = BREAKER_COOLDOWN_S
        self.window.append(True)

    def failure(self, hard):
        """hard — ответа не было вовсе (таймаут, нет соединения)."""
        if self.state == "half_open":
            self.cooldown = min(self.cooldown * 2, BREAKER_COOLDOWN_MAX_S)
            self._open()
            return
        if self.state != "closed":
            return
        self.window.append(False)
        self.hard_failures = self.hard_failures + 1 if hard else 0
        errors = self.window.count(False)
        if (self.hard_failures >= BREAKER_HARD_THRESHOLD
                or (len(self.window) >= BREAKER_MIN_SAMPLES
                    and errors / len(self.window) >= BREAKER_ERROR_RATE)):
            self._open()

    def _open(self):
        self.state = "open"
        self.open_until = time.monotonic() + self.cooldown
        self.hard_failures = 0
        self.window.clear()
        log.warning("breaker: open на %.1f с", self.cooldown)


breaker = Breaker()
payments_inflight = 0
cwnd = float(CONCURRENCY_START)  # разрешённое число одновременных вызовов Payment


# --- Inventory и Payment ---------------------------------------------------

async def reserve(order_id, items):
    r = await http.post(
        f"{INVENTORY_URL}/reservations",
        json={"order_id": order_id,
              "items": [{"sku": i["sku"], "qty": i["qty"]} for i in items]},
        timeout=INVENTORY_TIMEOUT_S,
    )
    if r.status_code not in (200, 201):
        raise RuntimeError(f"inventory reserve: {r.status_code}")


async def release(order_id):
    r = await http.delete(f"{INVENTORY_URL}/reservations/{order_id}", timeout=INVENTORY_TIMEOUT_S)
    if r.status_code not in (200, 204):
        raise RuntimeError(f"inventory release: {r.status_code}")


async def charge(order_id, amount):
    """Один вызов Payment. 'captured' | 'declined' | 'error' | 'skip' (не звонили)."""
    global payments_inflight, cwnd
    if payments_inflight >= int(cwnd) or not breaker.allow():
        return "skip"
    payments_inflight += 1
    try:
        async with asyncio.timeout(PAYMENT_TIMEOUT_S):
            r = await http.post(
                f"{PAYMENT_URL}/payments",
                json={"order_id": order_id, "amount_cents": amount, "currency": "RUB"},
                headers={"Idempotency-Key": order_id},
                timeout=PAYMENT_TIMEOUT_S,
            )
    except (httpx.TimeoutException, httpx.ConnectError, TimeoutError) as e:
        breaker.failure(hard=True)
        cwnd = CONCURRENCY_MIN
        log.warning("order %s: payment не ответил (%s)", order_id, type(e).__name__)
        return "error"
    except httpx.HTTPError as e:  # соединение оборвано: ответ потерян
        breaker.failure(hard=False)
        log.warning("order %s: payment оборвал соединение (%s)", order_id, type(e).__name__)
        return "error"
    finally:
        payments_inflight -= 1
    cwnd = min(cwnd + 1, CONCURRENCY_MAX)
    if r.status_code in (200, 201):
        breaker.success()
        return "captured"
    if r.status_code == 402:
        breaker.success()
        return "declined"
    breaker.failure(hard=False)
    log.warning("order %s: payment %s", order_id, r.status_code)
    return "error"


# --- обработка заказа ------------------------------------------------------

async def sql(query, *params):
    async with pool.connection() as conn:
        await conn.execute(query, params)


def backoff(attempts):
    return min(0.3 * 2 ** attempts, 4.0) * random.uniform(0.5, 1.5)


async def defer(order_id, delay, failed=False):
    """Отпустить заказ и назначить следующую попытку."""
    await sql(
        "UPDATE orders SET next_attempt_at = now() + make_interval(secs => %s), "
        "locked_until = now(), attempts = attempts + %s WHERE id = %s",
        delay, 1 if failed else 0, order_id,
    )


async def advance(o):
    oid = o["id"]
    state = o["payment_state"]
    if state is None:
        if not o["reserved"]:
            await reserve(oid, o["items"])
            await sql("UPDATE orders SET reserved = true WHERE id = %s", oid)
        if o["amount_cents"] == 0:
            state = "captured"  # платить нечего (Payment принимает от 1)
        else:
            state = await charge(oid, o["amount_cents"])
            if state == "skip":
                return await defer(oid, random.uniform(0.5, 1.0))
            if state == "error":
                return await defer(oid, backoff(o["attempts"]), failed=True)
        await sql("UPDATE orders SET payment_state = %s WHERE id = %s", state, oid)
    if state == "captured":
        await sql("UPDATE orders SET status = 'paid', locked_until = now() WHERE id = %s", oid)
    else:  # declined: резерв снимаем, только потом rejected
        await release(oid)
        await sql(
            "UPDATE orders SET status = 'rejected', reserved = false, locked_until = now() "
            "WHERE id = %s", oid)
    log.info("order %s: %s", oid, "paid" if state == "captured" else "rejected")


async def process(o):
    try:
        await advance(o)
    except Exception as e:
        log.warning("order %s: шаг не удался (%s: %s), повторю", o["id"], type(e).__name__, e)
        try:
            await defer(o["id"], backoff(o["attempts"]), failed=True)
        except Exception:
            log.exception("order %s: не удалось отложить", o["id"])


def spawn(coro):
    task = asyncio.create_task(coro)
    background.add(task)
    task.add_done_callback(background.discard)
    return task


async def claim(limit):
    async with pool.connection() as conn:
        cur = await conn.execute(
            "UPDATE orders SET locked_until = now() + make_interval(secs => %s) "
            "WHERE id IN (SELECT id FROM orders WHERE status = 'pending' "
            "AND next_attempt_at <= now() AND locked_until <= now() "
            "ORDER BY next_attempt_at LIMIT %s FOR UPDATE SKIP LOCKED) "
            "RETURNING id, amount_cents, items, reserved, payment_state, attempts",
            (LOCK_S, limit),
        )
        return await cur.fetchall()


async def worker():
    while True:
        try:
            limit = min(breaker.claim_limit(), int(cwnd) - payments_inflight)
            if limit > 0:
                for row in await claim(limit):
                    spawn(process(row))
        except Exception:
            log.exception("worker")
        await asyncio.sleep(WORKER_TICK_S)


# --- HTTP ------------------------------------------------------------------

def valid(body):
    if not isinstance(body, dict):
        return False
    if not isinstance(body.get("user_id"), str) or not body["user_id"]:
        return False
    items = body.get("items")
    if not isinstance(items, list) or not items:
        return False
    for i in items:
        if not isinstance(i, dict):
            return False
        if not isinstance(i.get("sku"), str) or not i["sku"]:
            return False
        for k, minimum in (("qty", 1), ("price_cents", 0)):
            v = i.get(k)
            if not isinstance(v, int) or isinstance(v, bool) or v < minimum:
                return False
    return True


@asynccontextmanager
async def lifespan(app):
    global pool, http
    pool = AsyncConnectionPool(
        DATABASE_URL, min_size=2, max_size=30, open=False,
        kwargs={"autocommit": True, "row_factory": dict_row},
    )
    for _ in range(60):  # база может подниматься дольше сервиса
        try:
            await pool.open(wait=True, timeout=5)
            await sql(SCHEMA)
            break
        except Exception as e:
            log.info("база недоступна (%s), жду", e)
            await pool.close()
            await asyncio.sleep(1)
    else:
        raise SystemExit("не дождался базы")
    http = httpx.AsyncClient(limits=httpx.Limits(max_connections=100))
    worker_task = asyncio.create_task(worker())
    yield
    worker_task.cancel()
    await http.aclose()
    await pool.close()


app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


@app.post("/orders")
async def create_order(request: Request):
    started = time.monotonic()
    try:
        body = json.loads(await request.body() or b"null")
    except ValueError:
        body = None
    if not valid(body):
        return JSONResponse({"error": "bad_request"}, status_code=400)

    order_id = str(uuid.uuid4())
    amount = sum(i["qty"] * i["price_cents"] for i in body["items"])
    async with pool.connection() as conn:
        # Заказ сразу закреплён за этим запросом (locked_until), воркер его не тронет.
        cur = await conn.execute(
            "INSERT INTO orders (id, user_id, status, amount_cents, items, locked_until) "
            "VALUES (%s, %s, 'pending', %s, %s, now() + make_interval(secs => %s)) "
            "RETURNING id, amount_cents, items, reserved, payment_state, attempts",
            (order_id, body["user_id"], amount, Jsonb(body["items"]), LOCK_S),
        )
        row = await cur.fetchone()

    task = spawn(process(row))  # не отменяется, когда ответ уже ушёл
    await asyncio.wait({task}, timeout=max(0.05, REPLY_BUDGET_S - (time.monotonic() - started)))

    status = "pending"
    if task.done():
        try:
            async with pool.connection() as conn:
                cur = await conn.execute("SELECT status FROM orders WHERE id = %s", (order_id,))
                status = (await cur.fetchone())["status"]
        except Exception:
            log.exception("чтение статуса %s", order_id)
    return JSONResponse({"id": order_id, "status": status}, status_code=201)


@app.get("/orders/{order_id}")
async def get_order(order_id: str):
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT id, status, amount_cents FROM orders WHERE id = %s", (order_id,))
        row = await cur.fetchone()
    if row is None:
        return JSONResponse({"error": "not_found"}, status_code=404)
    return row


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT, access_log=False, log_level="warning")
