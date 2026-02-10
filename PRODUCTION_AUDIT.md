# Production Readiness Audit — Cube Demper
**Дата:** 2026-02-09

## Контекст
Полный аудит готовности к продакшену. Фокус: бэкенд, нагрузка, инфраструктура Railway. Текущий масштаб: ~10-20 пользователей, цель — устойчивость при росте до 100+.

---

## Общая оценка: 6.5/10

**Хорошо:** rate limiting, circuit breaker, connection pooling, security headers, parameterized SQL, health check, dependency pinning.
**Плохо:** фоновые задачи без рестарта, дефолтные секреты в коде, Docker root, нет rate limit на auth, DB pool min=2, нет structured logging.

---

## КРИТИЧЕСКИЕ ПРОБЛЕМЫ (нужно фиксить сейчас)

### 1. Background tasks умирают без рестарта
**Файл:** `new-backend/app/main.py:78,85,90`

`asyncio.create_task()` для Playwright verify, legal docs loader и orders sync — если task крашится, он просто исчезает. Orders sync ломается → заказы не синхронизируются, никто не узнает.

**Фикс:** Обернуть каждый background task в retry-обёртку:
```python
async def _safe_background_task(coro_func, *args, name="task", restart_delay=60):
    """Wrapper that restarts background tasks on crash."""
    while True:
        try:
            await coro_func(*args)
        except asyncio.CancelledError:
            logger.info(f"[BG] {name} cancelled")
            break
        except Exception as e:
            logger.error(f"[BG] {name} crashed: {e}, restarting in {restart_delay}s")
            await asyncio.sleep(restart_delay)
```
Применить к `periodic_orders_sync`, `load_legal_docs_background`, `verify_playwright_background`.

### 2. Дефолтные секреты в config.py
**Файл:** `new-backend/app/config.py:55-56`
```python
secret_key: str = "your-secret-key-min-32-characters-change-in-production"
encryption_key: str = "your-encryption-key-must-be-32-bytes-fernet-compatible"
```
Если env vars не установлены — JWT подписывается дефолтным ключом, шифрование сессий скомпрометировано.

**Фикс:** Валидация при старте — если значения содержат "change-in-production", бросать RuntimeError. Или дефолт `None` + проверка при запуске.

### 3. Docker контейнеры работают от root
**Файл:** `new-backend/Dockerfile:47`, `Dockerfile.worker`

Нет `USER` директивы. Если контейнер скомпрометирован — полный доступ ко всему.

**Фикс:** Добавить в конец Dockerfile:
```dockerfile
RUN useradd -m -r appuser && chown -R appuser:appuser /app
USER appuser
```

### 4. DB pool min_size=2 — bottleneck при холодном старте
**Файл:** `new-backend/app/config.py:32`

Только 2 соединения при старте. Первые 3+ одновременных запроса будут ждать создания новых коннектов.

**Фикс:** `db_pool_min_size: int = 10` (в Railway env var `DB_POOL_MIN_SIZE=10`)

---

## ВЫСОКИЙ ПРИОРИТЕТ

### 5. Нет rate limiting на auth endpoints
**Файл:** `new-backend/app/routers/auth.py:30,84`

`/auth/register` и `/auth/login` без ограничений. Brute-force атаки, user enumeration, спам-регистрации.

**Фикс:** Добавить простой in-memory rate limiter (IP-based) или Redis-based:
- Login: max 5 попыток/мин на IP
- Register: max 3/мин на IP

### 6. Gemini API без circuit breaker и timeout
**Файл:** `new-backend/app/services/ai_lawyer_service.py`

`genai.GenerativeModel().generate_content()` вызывается без таймаута. Если Gemini зависнет — запрос клиента будет ждать вечно.

**Фикс:** Обернуть в `asyncio.wait_for(coro, timeout=30)` + добавить circuit breaker (уже есть инфра в `core/circuit_breaker.py`).

### 7. Orders sync последовательный — не масштабируется
**Файл:** `new-backend/app/services/orders_sync_service.py:81-114`

Каждый магазин обрабатывается последовательно с задержкой 2с. При 100 магазинах: 200с + fetch time ≈ 10-15 мин. При 500: час+, перекрывает следующий цикл.

**Фикс:** Параллельная обработка с лимитом (5-10 магазинов одновременно):
```python
sem = asyncio.Semaphore(5)
async def process_with_limit(store):
    async with sem:
        await _sync_store(store)
        await asyncio.sleep(STORE_DELAY)
await asyncio.gather(*[process_with_limit(s) for s in stores])
```

### 8. Hardcoded production URL во фронтенде
**Файл:** `frontend/src/lib/unit-economics.ts`

Fallback URL `'https://cube-demper-backend-production.up.railway.app'` захардкожен. Если `NEXT_PUBLIC_API_URL` не установлен — запросы идут на прод вместо localhost.

**Фикс:** Заменить на `'http://localhost:8010'` как fallback (или убрать fallback и кидать ошибку).

---

## СРЕДНИЙ ПРИОРИТЕТ

### 9. command_timeout=60s слишком высокий
**Файл:** `new-backend/app/core/database.py:31`

Медленные запросы висят целую минуту. Лучше 20-30 секунд — fail fast.

**Фикс:** `command_timeout=30`

### 10. Browser farm: только 2 шарда
**Файл:** `new-backend/app/config.py:73`

2 Playwright инстанса = ~15 параллельных операций. При 50+ магазинов, нуждающихся в одновременном refresh сессии — очередь.

**Фикс:** `browser_shards: int = 4` (через env `BROWSER_SHARDS=4`)

### 11. Redis ключи без TTL
**Файлы:** `new-backend/app/core/redis.py`

User activity, rate limit, proxy lock ключи создаются без expiration. Со временем Redis забивается.

**Фикс:** Везде использовать `setex()` или `expire()` с разумным TTL (1-24 часа).

### 12. Нет structured logging
**Файл:** `new-backend/app/core/logger.py`

Логи в plain text — сложно агрегировать, искать, алертить. Нет correlation ID для трейсинга запросов.

**Фикс:** Переход на JSON logging (добавить `python-json-logger`) — на будущее, не блокер.

### 13. Partner auth без Pydantic валидации
**Файл:** `new-backend/app/routers/partner_auth.py`

Credentials приходят как dict без schema валидации. Должна быть Pydantic модель.

---

## НИЗКИЙ ПРИОРИТЕТ (рекомендации)

### 14. N+1 в admin stats
`/admin/users` делает subqueries на каждого юзера. При 50 юзерах — ок, при 500 — тормоза.

### 15. CORS allow_headers=["*"]
`main.py:128` — стандартно для API, но можно ограничить до `["Authorization", "Content-Type"]`.

### 16. Миграция GUID формата
Старый формат (plain string) и новый (encrypted dict) сосуществуют. Нужна one-time миграция.

### 17. No SSL mode для DB соединения
`database.py` — нет `ssl='require'`. Railway Postgres может работать и без, но лучше с.

---

## МАСШТАБИРУЕМОСТЬ — Прогноз

| Пользователей | Продуктов | Воркеров | DB Pool max | Browser Shards | Статус |
|---|---|---|---|---|---|
| 10 | 1K | 2 | 50 | 2 | **Готово** |
| 50 | 5K | 3 | 50 | 4 | **Нужны #1,#4,#7** |
| 100 | 10K | 5 | 100 | 4 | **Нужны все HIGH** |
| 500 | 50K | 10 | 200 | 8 | **Нужна архитектура** |

---

## ПОЛОЖИТЕЛЬНОЕ (уже хорошо)

- Per-endpoint rate limiting для Kaspi API (offers 8 RPS, pricefeed 1.5 RPS per merchant)
- Circuit breaker для Kaspi auth
- Security headers (HSTS, X-Frame-Options, XSS Protection)
- Параметризованные SQL запросы — нет SQL injection
- Health check endpoint с проверкой DB и Redis
- HTTP client с connection pooling (100 max, HTTP/2)
- JWT с проверкой `is_blocked`
- Пароли хешируются bcrypt
- Все зависимости пинёны до конкретных версий
- Frontend Dockerfile — отличный (multi-stage, non-root user)
- Worker concurrency ограничен семафором (100)
- Graceful shutdown для DB pool, Redis, HTTP client

---

## ПЛАН ДЕЙСТВИЙ

### Фаза 1 — Критические фиксы (сейчас)
1. **Background task restart wrapper** — `main.py` + новая утилита
2. **Дефолтные секреты** — валидация в `config.py`
3. **Docker USER** — `Dockerfile`, `Dockerfile.worker`, `Dockerfile.orders`
4. **DB pool min_size** — env var `DB_POOL_MIN_SIZE=10`

### Фаза 2 — Безопасность (эта неделя)
5. **Auth rate limiting** — `routers/auth.py`
6. **Gemini timeout + circuit breaker** — `ai_lawyer_service.py`
7. **Hardcoded URL** — `frontend/src/lib/unit-economics.ts`
8. **Partner auth validation** — `routers/partner_auth.py`

### Фаза 3 — Масштаб (при росте до 50+ юзеров)
9. **Orders sync параллельный** — `orders_sync_service.py`
10. **Browser shards=4** — env var
11. **Redis TTL** — `core/redis.py`
12. **command_timeout=30** — `core/database.py`

---

## Верификация
1. `cd new-backend && python -c "from app.config import settings; print(settings.secret_key)"` — проверить что дефолтный ключ больше не работает
2. Запустить бэкенд, убить orders sync task → убедиться что рестартуется
3. `docker build -t test . && docker run --rm test whoami` → должен вывести `appuser`, не `root`
4. Нагрузочный тест: 10 одновременных запросов на `/health` → все должны ответить < 1с
5. Задеплоить, проверить логи Railway на отсутствие ошибок
