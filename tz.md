# Cube Demper - Полное техническое описание платформы

## Оглавление

1. [Обзор платформы](#1-обзор-платформы)
2. [Архитектура и технологии](#2-архитектура-и-технологии)
3. [Система демпинга цен (Price Bot)](#3-система-демпинга-цен-price-bot)
4. [Интеграция с Kaspi.kz](#4-интеграция-с-kaspikz)
5. [Система заказов](#5-система-заказов)
6. [WhatsApp интеграция](#6-whatsapp-интеграция)
7. [AI-функции](#7-ai-функции)
8. [Биллинг и подписки](#8-биллинг-и-подписки)
9. [Партнерская программа](#9-партнерская-программа)
10. [Админ-панель](#10-админ-панель)
11. [Техническая поддержка](#11-техническая-поддержка)
12. [База данных](#12-база-данных)
13. [Подводные камни и Edge Cases](#13-подводные-камни-и-edge-cases)
14. [Deployment и инфраструктура](#14-deployment-и-инфраструктура)

---

## 1. Обзор платформы

**Cube Demper** — это комплексная SaaS-платформа для автоматизации продаж на маркетплейсе Kaspi.kz.

### Основные функции:

| Модуль | Описание |
|--------|----------|
| **Price Bot** | Автоматический демпинг цен для удержания топ-позиций |
| **Kaspi Integration** | Синхронизация товаров, цен и заказов с Kaspi.kz |
| **WhatsApp Notifications** | Уведомления клиентам о статусах заказов |
| **AI Lawyer** | Юридические консультации с RAG-системой |
| **AI Salesman** | Автоматические допродажи через WhatsApp |
| **Unit Economics** | Калькулятор маржинальности и налогов |
| **Partner Program** | Реферальная система для партнёров |
| **Support Chat** | Real-time чат поддержки |

### Приложения в монорепозитории:

```
cube-demper-full/
├── frontend/          # Next.js 14 - основное приложение пользователей
├── new-backend/       # FastAPI - REST API + workers
├── admin-panel/       # Next.js 14 - панель администратора
├── partner-cabinet/   # Next.js 14 - кабинет партнёра
└── tech-support/      # Next.js 14 - CRM для поддержки
```

---

## 2. Архитектура и технологии

### Frontend Stack

| Технология | Версия | Назначение |
|------------|--------|------------|
| Next.js | 16.1.1 | App Router, SSR |
| React | 19.2.3 | UI библиотека |
| TypeScript | 5.x | Типизация |
| Tailwind CSS | 4.x | Стилизация |
| Zustand | 5.0.9 | State management |
| React Query | 5.90.12 | Data fetching и кеширование |
| shadcn/ui | - | UI компоненты (Radix + Tailwind) |

### Backend Stack

| Технология | Версия | Назначение |
|------------|--------|------------|
| FastAPI | 0.115+ | Async REST API |
| Python | 3.11+ | Язык бэкенда |
| asyncpg | - | Async PostgreSQL драйвер |
| PostgreSQL | 15+ | Основная БД |
| Redis | 7+ | Кеширование и сессии |
| Alembic | - | Миграции БД |
| Playwright | - | Браузерная автоматизация |
| OpenAI/Gemini | - | AI модели |

### Инфраструктура (Railway)

| Сервис | Описание |
|--------|----------|
| `frontend` | Next.js приложение |
| `backemd` | FastAPI backend (опечатка в имени!) |
| `Postgres` | PostgreSQL база данных |
| `Redis` | Redis кеш |
| `worker-1`, `worker-2` | Демпинг воркеры (2 шарда) |

---

## 3. Система демпинга цен (Price Bot)

### 3.1 Архитектура

Система использует **шардированную архитектуру** с распределением товаров между воркерами:

```
Товар → hash(product_id) % INSTANCE_COUNT = INSTANCE_INDEX

Worker-0: обрабатывает товары с hash % 2 = 0
Worker-1: обрабатывает товары с hash % 2 = 1
```

### 3.2 Цикл обработки товара

```
1. fetch_products_for_instance()
   ├── bot_active = TRUE
   ├── kaspi_stores.is_active = TRUE
   ├── kaspi_stores.needs_reauth = FALSE
   ├── external_kaspi_id IS NOT NULL
   ├── Текущее время в work_hours
   └── last_check_time + interval < NOW()

2. Для каждого товара:
   ├── Получить сессию магазина (skip_validation=True)
   ├── parse_product_by_sku() → получить цены конкурентов
   ├── Рассчитать целевую цену по стратегии
   ├── Применить ограничения (min_price, max_price)
   ├── sync_product() → обновить на Kaspi
   └── Записать в price_history
```

### 3.3 Стратегии демпинга

| Стратегия | Описание | Формула |
|-----------|----------|---------|
| `standard` | Быть на шаг дешевле | `target = competitor - step` |
| `always_first` | Всегда быть первым | `target = competitor - step` |
| `stay_top_n` | Оставаться в топ-N | `target = nth_position - step` |

### 3.4 Настройки демпинга

**Уровень магазина (demping_settings):**
- `price_step` — шаг цены (по умолчанию 1 тенге)
- `check_interval_minutes` — интервал проверки (15 мин)
- `work_hours_start/end` — рабочие часы (09:00-21:00)
- `min_profit` — минимальная прибыль
- `excluded_merchant_ids` — исключённые конкуренты

**Уровень товара (products):**
- `min_price`, `max_price` — пределы цены
- `price_step_override` — переопределение шага
- `demping_strategy` — стратегия
- `strategy_params` — параметры стратегии (JSON)

### 3.5 Защита от блокировок

| Механизм | Описание |
|----------|----------|
| **Rate Limiter** | Token Bucket, 60 RPS глобально |
| **Circuit Breaker** | 5 ошибок → OPEN, 60 сек timeout |
| **Proxy Rotation** | Пул прокси с ротацией по ошибкам |
| **Random Delays** | 10-300 мс между запросами |
| **Headers Randomization** | Случайные User-Agent, Accept и т.д. |

---

## 4. Интеграция с Kaspi.kz

### 4.1 Аутентификация

**Двухэтапная авторизация:**
```
1. authenticate_kaspi(email, password)
   ├── Открыть браузер через BrowserFarm
   ├── Заполнить форму логина
   └── Если требуется SMS → вернуть partial_session

2. verify_sms_code(merchant_id, code, partial_session)
   ├── Ввести SMS код
   ├── Получить cookies
   ├── Сохранить GUID (зашифрованные cookies)
   └── Установить needs_reauth = FALSE
```

**Структура GUID (сессия):**
```json
{
  "cookies": [...],
  "email": "...",
  "password": "encrypted",
  "merchant_uid": "M123456789",
  "authenticated_at": "2026-01-31T..."
}
```

### 4.2 API Endpoints Kaspi

| Endpoint | Auth | Описание |
|----------|------|----------|
| `mc.shop.kaspi.kz/bff/offer-view/list` | Yes | Список товаров магазина |
| `mc.shop.kaspi.kz/pricefeed/upload/merchant/process` | Yes | Обновление цены |
| `kaspi.kz/yml/offer-view/offers/{id}` | No | Цены конкурентов |
| `kaspi.kz/shop/api/v2/orders` | Yes | Список заказов |
| `mc.shop.kaspi.kz/mc/facade/graphql` | Yes | GraphQL для деталей заказа |

### 4.3 Флаг needs_reauth

**КРИТИЧЕСКИ ВАЖНО!** Если `needs_reauth = TRUE`, воркер не обрабатывает товары магазина.

**Причины:**
- `sms_required` — требуется SMS при refresh
- `invalid_credentials` — пароль изменился
- `credentials_missing` — нет сохранённого пароля

**Быстрый фикс:**
```sql
UPDATE kaspi_stores SET needs_reauth = FALSE WHERE guid IS NOT NULL;
```

---

## 5. Система заказов

### 5.1 Orders Worker

**Интервал опроса:** 600 секунд (10 минут)

**Процесс:**
```
1. Получить магазины с orders_polling_enabled = TRUE
2. Для каждого магазина:
   ├── Получить активную сессию
   ├── Запросить заказы (последние 7 дней)
   ├── UPSERT в таблицу orders
   └── При изменении статуса → триггер WhatsApp
```

### 5.2 Статусы заказов

```
APPROVED → ACCEPTED_BY_MERCHANT → DELIVERY → DELIVERED → COMPLETED
                                      ↓
                              CANCELLED / RETURNED
```

### 5.3 Данные заказа

**Таблица orders:**
- `kaspi_order_id`, `kaspi_order_code` — ID заказа
- `status`, `previous_status` — текущий и предыдущий статус
- `total_price`, `delivery_cost` — суммы в тиынах
- `customer_name`, `customer_phone` — данные покупателя
- `delivery_address`, `delivery_mode`, `payment_mode`
- `wa_*_sent_at` — флаги отправки WhatsApp уведомлений

**Таблица order_items:**
- Позиции заказа с ценами и количеством

---

## 6. WhatsApp интеграция

### 6.1 Архитектура

**Компоненты:**
- **WAHA** — Docker контейнер с WhatsApp Web API
- **Backend** — управление сессиями и отправка
- **OrderEventProcessor** — триггеры по статусам заказов

### 6.2 Авторизация по QR-коду

```
1. POST /whatsapp/session/create → создать сессию в WAHA
2. GET /whatsapp/session/qr → получить PNG с QR-кодом
3. Пользователь сканирует QR мобильным WhatsApp
4. Webhook session.status = WORKING → сессия активна
```

### 6.3 Шаблоны сообщений

**Переменные:**
```
{customer_name}        → "Иван Петров"
{order_code}           → "790686780"
{order_total}          → "15 000 тг"
{items_list}           → "• iPhone 15\n• Чехол"
{delivery_address}     → "Алматы, ул. Абая, 1"
{store_name}           → "My Store"
```

**События (trigger_event):**
- `order_approved` — заказ оплачен
- `order_shipped` — отправлен
- `order_delivered` — доставлен
- `order_completed` — завершён
- `review_request` — запрос отзыва (отложенный)

### 6.4 Методы отправки

```python
waha.send_text(phone, text, session)
waha.send_image(phone, image_url, caption, session)
waha.send_file(phone, file_url, filename, session)
waha.send_location(phone, lat, lng, name, session)
```

---

## 7. AI-функции

### 7.1 Общая архитектура AI

Единая схема для всех AI агентов:
```
Webhook событие
  → Сбор данных
  → Валидация магазина
  → Сбор контекста
  → AI Agent (Gemini)
  → Результат
  → Логирование
```

### 7.2 AI Lawyer (ИИ-Юрист)

**Модель:** Google Gemini 1.5 Pro

**RAG-система:**
- Правовые документы РК в `legal_documents`
- Статьи с embeddings в `legal_articles` (pgvector)
- Векторный поиск релевантных статей

**Функции:**
| Функция | Описание |
|---------|----------|
| Консультации | Ответы на вопросы с цитированием законов РК |
| Генерация документов | Договоры, претензии, заявления |
| Анализ договоров | Выявление рисков (критический/высокий/средний/низкий) |
| Калькулятор пени | Расчёт по ст. 353 ГК РК |
| Калькулятор налогов | ИП упрощёнка, ИП общий, ТОО (КПН) |
| Калькулятор госпошлин | Регистрация ИП/ТОО, иски |

**Документы юриста:**
- Договоры: поставки, купли-продажи, услуг, аренды, трудовые
- Претензии: поставщику, покупателю
- Заявления: регистрация ИП/ТОО, лицензия

**Триггеры для автоматического анализа:**
- Блокировка товара
- Штраф от Kaspi
- Возврат товара
- Спор с клиентом
- Ручной запрос

### 7.3 AI Salesman (ИИ-Продавец)

**Модель:** Google Gemini 1.5 Flash

**Концепция:**
- Не саппорт, не просто чат
- Автоматический менеджер допродаж и удержания
- Работает на событиях, а не по запросу

**Триггеры:**
| Триггер | Действие |
|---------|----------|
| `NEW_ORDER` | Допродажа сопутствующих товаров |
| `REPEAT_CUSTOMER` | Благодарность + персональное предложение |
| `REVIEW_REQUEST` | Запрос отзыва + бонус |

**Процесс:**
```
1. Новый заказ → получить данные (товары, клиент)
2. Загрузить каталог магазина (топ 50 товаров)
3. Получить историю покупок клиента
4. Сформировать контекст с настройками магазина
5. Gemini генерирует персонализированное сообщение
6. Отправить через WhatsApp
7. Если клиент отвечает → продолжить диалог
```

**Настройки магазина:**
- `ai_tone` — тон общения
- `ai_discount_percent` — макс скидка (2-15%)
- `ai_promo_code` — промокод
- `ai_review_bonus` — бонус за отзыв
- `ai_send_delay_minutes` — задержка отправки
- `ai_max_messages_per_day` — лимит сообщений

**Ограничения AI:**
- Не обещает то, чего нет в наличии
- Не нарушает правила Kaspi
- Не пишет ночью
- Не пишет чаще N раз

### 7.4 AI Accountant (ИИ-Бухгалтер) — планируется

**Триггеры:**
- Новая продажа
- Закрытие месяца
- Ручной запрос

**Задачи:**
- Расчёт прибыли и налогов
- Поиск убыточных товаров
- Рекомендации по ценообразованию

### 7.5 Unit Economics Calculator

**Возможности:**
- Расчёт маржинальности товара
- Комиссии Kaspi по категориям
- Налоги (ИП упрощёнка 3%, ИП общий 10%, ТОО КПН 20%)
- Сравнение сценариев доставки
- Сохранение расчётов в избранное

---

## 8. Биллинг и подписки

### 8.1 Тарифные планы

| План | Цена (KZT) | Лимит товаров | Функции |
|------|------------|---------------|---------|
| FREE | 0 | 100 | Базовый мониторинг, email поддержка |
| BASIC | 9,999 | 500 | Демпинг, WhatsApp, приоритетная поддержка |
| PRO | 29,999 | 5,000 | Аналитика, AI ассистенты, кастомные интеграции |

**Цены хранятся в тиынах** (1 KZT = 100 тиынов)

### 8.2 Процесс подписки

```
1. POST /billing/subscribe { plan: "basic" }
2. Деактивировать старую подписку (status='cancelled')
3. Создать новую (status='active', +30 дней)
4. Создать запись платежа
5. Выделить 100 прокси пользователю
```

### 8.3 TipTopPay интеграция

**Статус:** В разработке (TODO)

Планируется:
- Генерация платёжной ссылки
- Обработка webhook-ов
- Автоматическое продление подписки

---

## 9. Партнерская программа

### 9.1 Два типа рефералов

**A) Партнёрская программа (partners):**
- Отдельная таблица `partners`
- Вход через `/partner/login`
- Комиссия 20% от подписок приведённых клиентов
- Запрос выплаты (минимум 5000 тенге)

**B) Пользовательские рефералы:**
- Поле `referred_by` в таблице `users`
- Реферальный код = первые 8 символов UUID
- Транзакции в `referral_transactions`

### 9.2 Функции партнёра

| Функция | Описание |
|---------|----------|
| Статистика | Клики, регистрации, оплаты, заработок |
| Leads | Список привлечённых клиентов со статусами |
| Транзакции | История начислений и выплат |
| Payout | Запрос выплаты на банковские реквизиты |

---

## 10. Админ-панель

### 10.1 Функции администратора

| Функция | Описание |
|---------|----------|
| Статистика | Онлайн пользователи, доход, товары |
| Управление юзерами | Блокировка, продление подписки, смена роли |
| Управление партнёрами | Создание, удаление, статистика |
| Просмотр магазинов | Все подключённые магазины |
| История платежей | Все транзакции в системе |

### 10.2 Действия над пользователями

```
POST /admin/users/{id}/block — заблокировать
POST /admin/users/{id}/unblock — разблокировать
PATCH /admin/users/role — изменить роль (user/admin/support)
POST /admin/subscriptions/{id}/extend — продлить подписку на N дней
DELETE /admin/users/{id} — удалить
```

---

## 11. Техническая поддержка

### 11.1 Архитектура

**Компоненты:**
- `SupportChatWidget` — плавающая кнопка в дашборде пользователя
- `tech-support` — CRM приложение для сотрудников (порт 3002)
- WebSocket — real-time сообщения

### 11.2 Статусы чата

```
pending (новый) → open (в работе) → closed (завершён)
                                        ↓
                                    reopened
```

### 11.3 Функции CRM

- Список всех чатов с фильтрацией по статусу
- Real-time получение новых сообщений
- Назначение чата на себя ("Взять в работу")
- Закрытие/переоткрытие чата
- Счётчик непрочитанных сообщений

---

## 12. База данных

### 12.1 Основные таблицы (32 таблицы)

**Ядро:**
| Таблица | Описание |
|---------|----------|
| `users` | Пользователи (email, password, role, is_blocked) |
| `kaspi_stores` | Магазины Kaspi (guid, needs_reauth, ai_settings) |
| `products` | Товары (price, bot_active, demping_strategy) |
| `demping_settings` | Настройки демпинга (price_step, work_hours) |
| `price_history` | История изменений цен |

**Заказы:**
| Таблица | Описание |
|---------|----------|
| `orders` | Заказы (status, customer_phone, wa_*_sent_at) |
| `order_items` | Позиции заказов |
| `order_status_history` | История статусов |

**WhatsApp:**
| Таблица | Описание |
|---------|----------|
| `whatsapp_sessions` | Сессии WAHA |
| `whatsapp_templates` | Шаблоны сообщений |
| `whatsapp_messages` | История сообщений |
| `scheduled_messages` | Отложенные сообщения |

**AI:**
| Таблица | Описание |
|---------|----------|
| `ai_chat_history` | История чатов AI |
| `legal_documents` | Правовые документы РК |
| `legal_articles` | Статьи с embeddings (pgvector) |
| `lawyer_documents` | Сгенерированные документы |
| `ai_salesman_messages` | Сообщения AI продавца |

**Платежи:**
| Таблица | Описание |
|---------|----------|
| `subscriptions` | Подписки (plan, status, period) |
| `payments` | Платежи |
| `partners` | Партнёры |
| `partner_transactions` | Транзакции партнёров |

**Поддержка:**
| Таблица | Описание |
|---------|----------|
| `support_chats` | Чаты поддержки |
| `support_messages` | Сообщения в чатах |

### 12.2 Ключевые особенности

- **UUID Primary Keys** везде
- **Цены в тиынах** (1 KZT = 100 тиынов)
- **JSONB** для гибких данных (availabilities, guid, strategy_params)
- **pgvector** для RAG (опционально)
- **Триггер update_updated_at_column()** для автообновления

---

## 13. Подводные камни и Edge Cases

### 13.1 Демпинг

| Проблема | Симптом | Решение |
|----------|---------|---------|
| `needs_reauth = TRUE` | Воркер не видит товары | `UPDATE kaspi_stores SET needs_reauth = FALSE WHERE guid IS NOT NULL` |
| Нет external_kaspi_id | Товар не обрабатывается | Пересинхронизировать товары |
| Вне рабочих часов | Товар пропускается | Проверить work_hours в demping_settings |
| Rate limit 429 | Ошибки от Kaspi | Exponential backoff (1-7 сек) |
| Конкурент ниже min_price | Цена не меняется | Ожидать повышения конкурента |

### 13.2 Kaspi API

| Проблема | Решение |
|----------|---------|
| Null поля в ответе | Всегда `.get()` с default value |
| offerId = null | Fallback на `raw_offer.get("sku")` |
| Разные цены по городам | Использовать city_id (750000000 = Алматы) |
| Session expired (401) | auto_refresh с kaspi_email/password |

### 13.3 WhatsApp

| Проблема | Решение |
|----------|---------|
| QR код устарел | Запросить новый через `/session/qr` |
| Номер не в WhatsApp | `check_number_exists()` перед отправкой |
| Бан за массовые рассылки | Интервал 30 сек, лимит 100 сообщений/день |
| WAHA контейнер недоступен | Health check, перезапуск контейнера |

### 13.4 Миграции Alembic

| Проблема | Решение |
|----------|---------|
| Multiple heads | Обновить `down_revision` в проблемной миграции |
| Конфликт при merge | `alembic heads`, `alembic merge -m "merge"` |

### 13.5 Railway Deployment

| Проблема | Решение |
|----------|---------|
| Healthcheck timeout 2-5 мин | Это нормально — инфраструктура Railway |
| Playwright блокирует startup | Запускать в background через `asyncio.create_task()` |
| railway.toml конфликтует | Использовать дефолты Railway или настраивать через UI |

---

## 14. Deployment и инфраструктура

### 14.1 Railway конфигурация

**Backend:**
```bash
sh -c 'alembic upgrade head && exec uvicorn app.main:app --host 0.0.0.0 --port $PORT'
```

**Workers:**
```bash
INSTANCE_INDEX=0 INSTANCE_COUNT=2  # Worker-1
INSTANCE_INDEX=1 INSTANCE_COUNT=2  # Worker-2
```

### 14.2 Синхронизация репозиториев

```bash
# Frontend → Demper_front (master)
rsync -av --exclude='.git' --exclude='node_modules' frontend/ "../Cube Demper/frontend/"
cd "../Cube Demper/frontend" && git push origin master

# Backend → cube-demper- (main)
rsync -av --exclude='.git' --exclude='__pycache__' new-backend/ "../Cube Demper/new-backend/"
cd "../Cube Demper/new-backend" && git push origin main
```

### 14.3 Переменные окружения (Backend)

```bash
# Database
DATABASE_URL=postgresql://...
DATABASE_PUBLIC_URL=postgresql://...
REDIS_URL=redis://...

# Kaspi
KASPI_MIN_PRICE=10

# AI
GEMINI_API_KEY=...
GEMINI_MODEL=gemini-1.5-flash
GEMINI_LAWYER_MODEL=gemini-1.5-pro

# WhatsApp
WAHA_URL=http://waha:3000
WAHA_PASSWORD=...

# Billing
TIPTOPPAY_PUBLIC_ID=...
TIPTOPPAY_API_SECRET=...

# Workers
INSTANCE_INDEX=0
INSTANCE_COUNT=2
ORDERS_POLLING_INTERVAL=600
```

---

## Приложение: Диагностические SQL-запросы

### Проверка демпера

```sql
-- Почему воркер не видит товары?
SELECT
    COUNT(*) as total_with_demping,
    COUNT(*) FILTER (WHERE ks.is_active = TRUE) as store_active,
    COUNT(*) FILTER (WHERE ks.needs_reauth = FALSE) as no_reauth,
    COUNT(*) FILTER (WHERE ks.guid IS NOT NULL) as has_session,
    COUNT(*) FILTER (WHERE p.external_kaspi_id IS NOT NULL) as has_external_id
FROM products p
JOIN kaspi_stores ks ON ks.id = p.store_id
WHERE p.bot_active = TRUE;

-- Сбросить needs_reauth
UPDATE kaspi_stores SET needs_reauth = FALSE WHERE guid IS NOT NULL;

-- История цен
SELECT * FROM price_history ORDER BY created_at DESC LIMIT 20;
```

### Проверка заказов

```sql
-- Последние заказы
SELECT o.kaspi_order_code, o.status, o.total_price, o.customer_name
FROM orders o
ORDER BY o.created_at DESC LIMIT 10;

-- WhatsApp уведомления
SELECT * FROM whatsapp_messages WHERE order_code IS NOT NULL
ORDER BY created_at DESC LIMIT 20;
```

### Проверка подписок

```sql
-- Активные подписки
SELECT u.email, s.plan, s.status, s.current_period_end
FROM subscriptions s
JOIN users u ON u.id = s.user_id
WHERE s.status = 'active';

-- Истекающие подписки (в течение 7 дней)
SELECT u.email, s.plan, s.current_period_end
FROM subscriptions s
JOIN users u ON u.id = s.user_id
WHERE s.status = 'active'
  AND s.current_period_end < NOW() + INTERVAL '7 days';
```

---

*Документ создан: 2026-01-31*
*Версия: 1.0*
