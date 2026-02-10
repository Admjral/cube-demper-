# Claude Code Instructions for Cube Demper Project

## Project Overview
Cube Demper - сервис для автоматического демпинга цен на Kaspi.kz маркетплейсе.

## Repository Structure

### Main Monorepo (Development)
- **Path**: `/Users/adilhamitov/Desktop/cube-demper-full`
- **GitHub**: https://github.com/Admjral/cube-demper-full
- **Contains**: frontend + new-backend + sync scripts

### Separate Deployment Repos
После разработки изменения синхронизируются в отдельные репозитории:

#### Frontend
- **Path**: `/Users/adilhamitov/Desktop/Cube Demper/frontend`
- **GitHub**: https://github.com/Admjral/Demper_front
- **Branch**: `master`
- **Deploy**: Railway (service: `frontend`)

#### Backend
- **Path**: `/Users/adilhamitov/Desktop/Cube Demper/new-backend`
- **GitHub**: https://github.com/Admjral/cube-demper-
- **Branch**: `main`
- **Deploy**: Railway (service: `backemd`)

## Tech Stack

### Frontend
- Next.js 14 (App Router)
- TypeScript
- Tailwind CSS
- shadcn/ui components
- Zustand (state management)

### Backend
- FastAPI
- PostgreSQL (asyncpg)
- Redis
- Alembic (migrations)
- Playwright (web scraping)

## Deployment (Railway)

### Project: `proud-vision`
Services:
- `frontend` - Next.js app
- `backemd` - FastAPI backend (typo in name, don't change)
- `Postgres` - PostgreSQL database
- `Redis` - Redis cache
- `worker-1`, `worker-2` - Demping workers (2 шарда достаточно для 10-20 юзеров)

### Backend Dockerfile
```dockerfile
# Main backend uses: Dockerfile
# Workers use: Dockerfile.worker
```

### Backend Start Command
```bash
sh -c 'alembic upgrade head && exec uvicorn app.main:app --host 0.0.0.0 --port $PORT'
```

### Worker Environment Variables
Each worker needs:
- `INSTANCE_INDEX` - 0 or 1
- `INSTANCE_COUNT` - 2

> Сократили с 4 до 2 воркеров для экономии. 2 воркера достаточно для ~2000 товаров.

## Git Workflow

### Committing to Monorepo
```bash
cd /Users/adilhamitov/Desktop/cube-demper-full
git add <files>
git commit -m "feat: description"
git push origin main
```

### Syncing to Separate Repos
Use `sync.sh` script or manual rsync:

```bash
# Frontend sync
rsync -av --exclude='.git' --exclude='node_modules' --exclude='.next' \
  /Users/adilhamitov/Desktop/cube-demper-full/frontend/ \
  "/Users/adilhamitov/Desktop/Cube Demper/frontend/"

# Backend sync
rsync -av --exclude='.git' --exclude='__pycache__' --exclude='.venv' --exclude='venv' \
  /Users/adilhamitov/Desktop/cube-demper-full/new-backend/ \
  "/Users/adilhamitov/Desktop/Cube Demper/new-backend/"
```

### After Sync - Push to Deployment Repos
```bash
# Frontend
cd "/Users/adilhamitov/Desktop/Cube Demper/frontend"
git add -A
git commit -m "feat: description"
git push origin master

# Backend
cd "/Users/adilhamitov/Desktop/Cube Demper/new-backend"
git add -A
git commit -m "feat: description"
git push origin main
```

## Database Migrations

### Creating New Migration
```bash
cd /Users/adilhamitov/Desktop/cube-demper-full/new-backend
alembic revision -m "description"
```

### Migration Naming Convention
`YYYYMMDDHHMMSS_description.py` (e.g., `20260121100000_add_product_city_prices.py`)

### Important: Migration Chain
Always check `down_revision` points to the latest existing migration. Use:
```bash
alembic heads
alembic history
```

## Key Files

### Backend
- `app/main.py` - FastAPI app entry point
- `app/config.py` - Configuration and env vars
- `app/routers/` - API endpoints
- `app/routers/ai.py` - AI chat + AI Salesman endpoints
- `app/routers/lawyer.py` - AI Lawyer (чат, документы, калькуляторы)
- `app/services/ai_lawyer_service.py` - Сервис юриста (RAG, анализ договоров)
- `app/services/ai_salesman_service.py` - Сервис продажника (upsell, отзывы)
- `app/services/legal_docs_loader.py` - Автозагрузка PDF в RAG при старте
- `app/services/api_parser.py` - Kaspi API integration (sync_orders_to_db, parse_order_details)
- `app/services/kaspi_mc_service.py` - MC GraphQL: заказы, телефоны покупателей
- `app/services/orders_sync_service.py` - Периодический sync заказов (background, каждые 60 мин)
- `app/workers/demper_instance.py` - Worker for price demping
- `migrations/versions/` - Alembic migrations
- `legal_docs/` - PDF документы для RAG (загружаются автоматически при старте)

### Frontend
- `src/app/(dashboard)/` - Dashboard pages (App Router)
- `src/components/` - React components
- `src/hooks/api/` - API hooks (React Query)
- `src/store/` - Zustand stores
- `src/types/api.ts` - TypeScript types

## Common Issues & Fixes

### Multiple Alembic Heads
Fix by updating `down_revision` in the problematic migration to point to correct parent.

### Railway CLI in Non-TTY Mode
Cannot use interactive commands. Use Railway Dashboard for worker configuration.

### Kaspi API Returns Null Fields
Always use fallback values, e.g.:
```python
kaspi_product_id = raw_offer.get("offerId") or raw_offer.get("sku")
```

### Demper Worker не видит товары ("Fetched 0 active products")
Проверь по порядку:
1. `bot_active = TRUE` у товаров? (включён демпинг)
2. `kaspi_stores.is_active = TRUE`?
3. `kaspi_stores.needs_reauth = FALSE`? ← **Частая проблема!**
4. `kaspi_stores.guid IS NOT NULL`? (есть сессия)
5. `products.external_kaspi_id IS NOT NULL`?
6. Текущее время в рабочих часах (`demping_settings.work_hours_*`)?

**Быстрый фикс needs_reauth:**
```sql
UPDATE kaspi_stores SET needs_reauth = FALSE WHERE guid IS NOT NULL;
```

## Git Config
- Email: `hamitov.adil04@gmail.com`
- Username: `Admjral`

## Railway CLI
- Path: `/opt/homebrew/bin/railway`
- To check status: `railway status`
- To view logs: `railway logs -s <service>`

## Direct Database Access
Для прямого доступа к БД Railway без CLI (Claude может использовать это):

```bash
# Получить публичный DATABASE_URL:
/opt/homebrew/bin/railway variables -s Postgres --json 2>&1 | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('DATABASE_PUBLIC_URL',''))"

# Запустить SQL через Python:
python3 -c "
import psycopg2
conn = psycopg2.connect('DATABASE_URL_HERE')
cur = conn.cursor()
cur.execute('SELECT ...')
print(cur.fetchall())
conn.close()
"
```

### Полезные диагностические запросы
```sql
-- Проверить почему воркер не видит товары
SELECT
    COUNT(*) as total_with_demping,
    COUNT(*) FILTER (WHERE ks.is_active = TRUE) as store_active,
    COUNT(*) FILTER (WHERE ks.needs_reauth = FALSE OR ks.needs_reauth IS NULL) as no_reauth_needed,
    COUNT(*) FILTER (WHERE ks.guid IS NOT NULL) as has_session,
    COUNT(*) FILTER (WHERE p.external_kaspi_id IS NOT NULL) as has_external_id
FROM products p
JOIN kaspi_stores ks ON ks.id = p.store_id
WHERE p.bot_active = TRUE;

-- Сбросить needs_reauth если сессия валидна
UPDATE kaspi_stores SET needs_reauth = FALSE WHERE guid IS NOT NULL;

-- Проверить историю цен
SELECT * FROM price_history ORDER BY created_at DESC LIMIT 20;
```

## Important Notes
1. **Never force push to main/master branches**
2. **Always sync both frontend and backend after changes**
3. **Check Railway deploy logs after pushing**
4. **Migrations run automatically on backend deploy**
5. **Workers need manual configuration in Railway Dashboard**

---

## Claude Instructions

### После успешного фикса
Когда пользователь подтверждает что правка заработала — добавь инсайт в секцию "Learned Insights" ниже.

### Управление размером файла
Если CLAUDE.md превышает ~300 строк — сократи устаревшую информацию, оставь только актуальное.

---

## Learned Insights

### Railway Deployment
- **Healthcheck timeout**: Railway Network healthcheck может занимать 2-5 минут даже если приложение стартует быстро. Это нормально — инфраструктура Railway (DNS, load balancer).
- **railway.toml**: Может конфликтовать с UI настройками. Лучше использовать дефолты Railway или настраивать через UI.
- **Playwright при старте**: Блокирует startup на 30-60 секунд. Решение — запускать проверку в background через `asyncio.create_task()`.

### Admin Panel (2026-01-28)
- Добавлена админ-панель из форка `hasabasa/cube-demper-full` branch `admin-panel`
- Новые роутеры: `admin.py`, `partner_auth.py`
- Новые таблицы: `partners`, колонка `is_blocked` в `users`
- **Важно**: При добавлении роутера не забыть подключить в `main.py`

### Authentication
- Login endpoint должен проверять `is_blocked` перед выдачей токена

### Google Gemini API (2026-02-07)
- **API ключ**: Google Cloud Console (не AI Studio), включить "Generative Language API" в restrictions
- **Модели**: `gemini-2.5-flash` (основная), `text-embedding-004` (эмбеддинги для RAG)
- **Ошибка "API key expired"**: Ключ невалиден, проверять через curl перед деплоем

### Legal Docs RAG Loader (2026-02-07)
- **Автозагрузка**: PDF из `legal_docs/` загружаются при старте как background task, идемпотентно (по `title`)
- **pgvector опционален**: На Railway нет pgvector → text search fallback
- **Таблицы**: `legal_documents` (`document_type`, NOT `category`), `legal_articles` (NO `keywords` column)

### Kaspi API Rate Limits (2026-02-08)
- **Offers**: 8 RPS safe (бан 403, 10с, per IP). **Pricefeed**: 1.5 RPS safe (бан 429, **30 мин**, per account!). **Catalog**: без ограничений.
- **Реализация**: Per-endpoint limiters в `rate_limiter.py`, per-merchant для pricefeed, cooldown/pause механизмы
- **Конфиг**: `offers_rps`, `pricefeed_rps`, `pricefeed_cooldown_seconds`, `offers_ban_pause_seconds`

### Bugs Fixed (2026-02-08, consolidated)
- **Demper worker**: TypeError из-за несовпадения сигнатур + единый rate limiter → per-endpoint лимитеры
- **AI Salesman**: category/sales_count не заполнялись, лимит сообщений игнорировался, sent_at=NULL, нет валидации телефона
- **Парсинг телефонов**: dead code в `format_chat_id()`, двойной +7 в MC (10 vs 11 цифр)
- **Deploy repo contamination**: rsync не удаляет лишние файлы → всегда `git status` после rsync
- **Alembic idempotency**: `inspector.get_table_names()` перед `CREATE TABLE` при нескольких таблицах в одной миграции
- **AI Lawyer docs**: KeyError в шаблонах (setdefault), asyncpg JSONB = `json.dumps()`, отсутствующие шаблоны
- **Playwright race condition**: Per-merchant `asyncio.Lock` — параллельные логины в один аккаунт = гарантированный сбой
- **Secret scanning**: НИКОГДА не хардкодить API ключи в публичных репо. Использовать env vars + `None` дефолт

### Frontend Patterns (2026-02-09)
- **Hydration #418**: Использовать `'\u00B7'` вместо `&middot;` в Next.js
- **Mobile-first**: `grid-cols-1 sm:grid-cols-2 lg:grid-cols-4`, таблицы → `sm:hidden` карточки + `hidden sm:block` таблица
- **Фиксированные ширины**: `w-full sm:w-[140px]`, `h-[250px] sm:h-[400px]`

### Kaspi MC GraphQL (2026-02-09)
- **URL**: `mc.shop.kaspi.kz/mc/facade/graphql`
- **Auth**: cookies как httpx dict (НЕ Cookie header строкой) + `x-auth-version: 3` + `Origin: https://kaspi.kz`
- **НЕ Relay-схема**: нет `edges/node`, нет `first`/`states`/`totalCount`
- **Список заказов**: `merchant.orders.orders(input: { presetFilter: TAB })` → `{total, orders: [...]}`
- **Табы**: `NEW`, `DELIVERY`, `PICKUP`, `SIGN_REQUIRED` — только активные, **нет COMPLETED/ARCHIVE**
- **Поля Order**: `code`, `totalPrice`, `status` (НЕ `state`!), `customer` (объект), `entries` (объект)
- **`creationDate` НЕ существует** в типе Order (есть только в orderDetail)
- **`orderDetail(code: "xxx")`**: работает для получения деталей (customer.phoneNumber, entries, deliveryAddress)
- **`get_active_session(merchant_id)`**: принимает 1 аргумент! Не (user_id, store_id, pool)

### Orders Sync Architecture (2026-02-09)
- MC GraphQL показывает только **активные** заказы → БД накапливает историю со временем
- **Отдельная фоновая задача в бэкенде** (`orders_sync_service.py`), НЕ в воркерах (они уже нагружены offers+pricefeed)
- Цикл каждые 60 мин, `asyncio.create_task()` в `main.py`
- Последовательная обработка магазинов с паузами (2с между магазинами)
- 400 магазинов × 24 запроса = 9,600 req/cycle → ~53 мин при 3 RPS
- `fetch_orders_for_sync()`: шаг 1 = коды из 4 табов, шаг 2 = `orderDetail` для каждого
- Формат конвертируется в Open API совместимый → `sync_orders_to_db()` без изменений

### AI Lawyer Chat (2026-02-09, исправлено)
- **Gemini role mapping**: `"assistant"` → `"model"` (Gemini API не принимает "assistant")
- **Duplicate message**: User message нужно сохранять в history **ПОСЛЕ** `chat()`, не до (иначе Gemini видит его дважды → краш на втором сообщении)

### WAHA Plus на Railway (2026-02-09)
- **Темплейт**: Использовать официальный WAHA темплейт на Railway marketplace (Docker Hub credentials через CLI не работают)
- **Движок NOWEB**: `WHATSAPP_DEFAULT_ENGINE=NOWEB` (без Chromium)
- **API Key**: WAHA генерирует `WAHA_API_KEY` при первом старте → добавить в backend env vars
- **Internal URL**: `http://waha-plus.railway.internal:3000`
- **Public URL**: `waha-plus-production-ccb5.up.railway.app` (dashboard + API)
- **Проверка сессий**: `curl -H "X-Api-Key: KEY" https://PUBLIC_URL/api/sessions`
- **OTP сессия**: `config.py:waha_otp_session` = `"default"` — единственная активная сессия (77027410732, Cube Development). Если поменяется — обновить в config.py или env var `WAHA_OTP_SESSION`

### WhatsApp Templates (2026-02-09)
- **JSONB response parsing**: asyncpg возвращает JSONB как raw string. Всегда `json.loads()` при чтении, `json.dumps()` + `::jsonb` при записи
- **Правило**: При изменении Create/Update схемы — **всегда** проверять Response схему и все SQL запросы на согласованность

### Subscription & Billing (2026-02-09)
- **Цены API**: `/billing/plans-v2` уже возвращает тенге (`price_tiyns / 100`). Фронтенд НЕ должен делить повторно — `plan.price.toLocaleString()`, не `(plan.price / 100)`
- **Free plan при регистрации**: `auth.py` создаёт subscription с `plan='free'`, `plan_id=NULL`. Значит `features.plan_code === null` но `has_active_subscription === true`
- **Trial flow**: `POST /billing/activate-trial` — проверяет 3 условия: 1) нет plan_id, 2) merchant_id не использован другим аккаунтом для триала, 3) plan с trial_days > 0
- **Anti-abuse**: Проверка по `kaspi_stores.merchant_id` через JOIN между аккаунтами — один магазин = один триал, вне зависимости от аккаунта
- **SubscriptionGate vs FeatureGate**: `SubscriptionGate` проверяет `has_active_subscription` (есть ли вообще подписка), `FeatureGate` проверяет конкретную фичу в `features[]`
- **Админ endpoints**: `POST /admin/users/{id}/subscription/cancel`, `ends_at` param в AssignSubscriptionRequest (ISO datetime, приоритет над `days`)

### WhatsApp OTP верификация телефона (2026-02-09)
- **Флоу**: Регистрация (email+пароль+телефон) → бэкенд создаёт юзера + отправляет 6-значный OTP в WhatsApp → фронтенд перенаправляет на `/verify-phone` → юзер вводит код → доступ к дашборду
- **Регистрация теперь возвращает JWT**: Раньше возвращала `UserResponse`, теперь `Token` — автологин сразу после регистрации (чтобы можно было вызвать `/auth/send-otp` и `/auth/verify-otp` с токеном)
- **signUp() тоже возвращает AuthResponse**: Фронтенд `auth.ts:signUp()` теперь делает `register` → получает токен → `GET /me` → возвращает `AuthResponse` (как `signIn`), а не просто `User`
- **OTP rate limit**: Максимум 1 код в 60 секунд на юзера (проверка по `created_at` в `phone_verifications`). 5 попыток на код, истекает через 5 минут
- **Обратная совместимость для старых юзеров**: Гард в dashboard layout: `user.phone && !user.phone_verified` → редирект. Юзеры с `phone=NULL` (легаси) проходят свободно
- **Middleware**: `/verify-phone` требует auth_token но НЕ редиректит на дашборд (в отличие от `/login`/`/register` которые редиректят залогиненных)
- **dependencies.py**: `get_current_user()` SQL запрос **должен** включать `phone, phone_verified` — иначе `current_user.get('phone')` всегда `None` и OTP endpoints не работают
- **WAHA отправка**: `get_waha_service().send_text(phone, text, session=settings.waha_otp_session)` — session param обязателен для OTP

### DB Audit (2026-02-10)
- **`products` таблица**: колонка `kaspi_sku`, НЕ `sku`. AI Salesman использовал `p.sku` → крашился
- **`kaspi_stores` ON CONFLICT**: `ON CONFLICT (merchant_id)` не обновляет `user_id` → User B может перезаписать credentials User A. Фикс: проверка ownership перед INSERT
- **FK без CASCADE**: `phone_verifications.user_id` и `subscriptions.assigned_by` блокировали удаление юзеров. Фикс: миграция `20260210120000`
- **WhatsApp status нормализация**: WAHA возвращает `WORKING`/`SCAN_QR_CODE`, а приложение хранит `connected`/`qr_pending`. Фикс: `normalize_waha_status()` в `whatsapp.py`
- **Legacy endpoints**: `/session/create` (singular) — мёртвый код, INSERT без `waha_api_key` (NOT NULL) = crash. Фронтенд использует `/sessions` (plural)
- **Alembic chain**: `ba10cb14a230` (initial) и `6ce6a0fa5853` оба имеют `down_revision = None` — pre-existing multiple heads, не трогать
