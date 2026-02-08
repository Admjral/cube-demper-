математика прокси

1️⃣ Демпер (70 прокси) - БЕЗОПАСНО для 24/7:

500 товаров × 2 запроса (parse + sync) = 1000 запросов/цикл
1000 / 249 = 5 прокси на цикл (не 3!)
70 / 5 = 14 циклов
14 × 3 минуты = 42 минуты отдыха первого прокси

✅ 42 мин > 40 мин (минимум) → БЕЗОПАСНО!
2️⃣ Мои продажи (25 прокси) - КРИТИЧНО увеличил!
Вы правы - 15 было недостаточно для 200-500 заказов!


Worst case: 500 заказов × 2 запроса = 1000 запросов
1000 / 249 = 5 прокси на цикл
25 / 5 = 5 циклов
5 × 10 минут = 50 минут отдыха

✅ 50 мин > 40 мин → БЕЗОПАСНО!
✅ Теперь даже большие магазины не получат блокировку!
3️⃣ Новая оптимизированная разбивка:

Демпер:   70 прокси (1000 req/3 мин)   ← без изменений
Orders:   25 прокси (до 1000 req/10мин) ← увеличил с 15!
Catalog:   5 прокси (20 req редко)     ← уменьшил с 10
Reserve:   0 прокси                     ← убрал, используем catalog

ИТОГО: 100 прокси = $11/месяц ✅
🚀 Главное преимущество - параллельность БЕЗ ОЧЕРЕДЕЙ:

Демпер работает с прокси 1-70
  └─ 1000 запросов каждые 3 минуты

Orders работает с прокси 71-95  ← ОДНОВРЕМЕННО!
  └─ до 1000 запросов каждые 10 минут

Catalog работает с прокси 96-100
  └─ ~20 запросов по требованию

❌ БЕЗ разделения: Orders ждет пока Демпер освободит прокси
✅ С разделением: Orders имеет свои 25 прокси, не ждет никого!
План обновлен с:

✅ Правильной математикой отдыха (42 мин для демпера, 50 мин для orders)
✅ 25 прокси для orders (вместо 15) для больших магазинов
✅ Per-module интеграцией во всех воркерах
✅ ORDERS_POLLING_INTERVAL = 600 сек (10 минут)
✅ Тестами параллельной работы модулей
Claude’s Plan
План: Per-User Proxy Pool для обхода Kaspi API лимитов
Проблема
Критическое превышение лимитов Kaspi API:

Математика проблемы:
Kaspi лимит: 250 запросов / 30 минут = 8.33 запросов/минуту (0.139 RPS)
Текущее потребление:
500 товаров × 2 запроса (parse + sync) = 1000 запросов
Демпинг каждые 3 минуты = 1000 запросов / 3 мин = 333 запроса/минуту
Плюс: предзаказы, продажи, синхронизация = еще ~50 запросов/минуту
ИТОГО: ~383 запроса/минуту
Превышение лимита:

383 запросов/мин / 8.33 лимит = 46x ПРЕВЫШЕНИЕ! 🔴
Последствия:

Kaspi банит аккаунт (429 Too Many Requests)
Демпинг не работает
Предзаказы не обновляются
Circuit breaker открывается → весь функционал падает
Решение конкурентов (Алгатоп, Деметра)
Они используют прокси-ротацию:

✅ Демпинг ВСЕХ товаров одновременно (не постепенно)
✅ Ротация прокси каждые 250 запросов
✅ Прокси "отдыхают" 40 минут после использования
✅ Это позволяет обходить rate limiting Kaspi
Наше решение: Еще лучше - персональный пул прокси для каждого юзера!

Исследование текущей реализации
Файлы для изменения:
Rate Limiter: new-backend/app/core/rate_limiter.py

Текущий: 60 RPS глобальный лимит (для ВСЕХ запросов, не только Kaspi)
Проблема: Не учитывает лимит 250/30min для Kaspi
Демпинг Worker: new-backend/app/workers/demper_instance.py

Обрабатывает 500 товаров за цикл
2 запроса на товар: parse_product_by_sku + sync_product
Нет приоритизации, нет батчинга
API Parser: new-backend/app/services/api_parser.py

parse_product_by_sku() (строка 341) - читает цены конкурентов
sync_product() (строка 459) - обновляет цену
Нет кеширования, нет батчинга
Config: new-backend/app/config.py

global_rps: int = 60 (строка 77)
Архитектура решения: Per-User Proxy Pool
Концепция
Каждому платному юзеру = 100 персональных прокси, разделенных по модулям


┌─────────────────────────────────────────────────────┐
│  User оплачивает подписку (Premium)                 │
└─────────────────────────────────────────────────────┘
                      ↓
┌─────────────────────────────────────────────────────┐
│  Автоматически выделяем 100 прокси из пула          │
│  proxies.user_id = user.id                          │
│  proxies.status = 'allocated'                       │
│  proxies.module = 'demper' | 'orders' | 'catalog' | 'reserve' │
└─────────────────────────────────────────────────────┘
                      ↓
┌─────────────────────────────────────────────────────┐
│  РАЗДЕЛЕНИЕ ПО МОДУЛЯМ (per-module pools):          │
│                                                      │
│  ┌─ 70 прокси → Демпер (demper_instance.py)        │
│  │   - Ротация каждые 249 запросов                 │
│  │   - 1000 запросов/цикл (500 товаров × 2)        │
│  │   - 5 прокси на цикл, 14 циклов = 42 мин отдыха │
│  │   - 24/7 работа БЕЗОПАСНА ✅ (42 > 40 мин)      │
│  │                                                  │
│  ┌─ 25 прокси → Мои продажи (orders_worker.py)     │
│  │   - Каждые 10 минут                             │
│  │   - 200-500 заказов = до 1000 запросов! 🔥      │
│  │   - 5 прокси на цикл, 5 циклов = 50 мин отдыха  │
│  │   - Критично для больших магазинов! ✅          │
│  │                                                  │
│  ┌─ 5 прокси → Синхронизация каталога              │
│  │   - По запросу пользователя (редко)             │
│  │   - ~20 запросов = 1 прокси                     │
│  │   - 5 прокси = запас на параллельные синхр.     │
│  │                                                  │
│  └─ 0 прокси → Резерв (используем catalog прокси)  │
│      - При необходимости берем из catalog пула     │
│      - Экономим место для важных модулей           │
└─────────────────────────────────────────────────────┘
                      ↓
┌─────────────────────────────────────────────────────┐
│  ✅ ПРЕИМУЩЕСТВА:                                   │
│  - Нет конкуренции между модулями                  │
│  - Демпер + orders работают ПАРАЛЛЕЛЬНО            │
│  - 1000 запросов одновременно с разных прокси      │
│  - Изоляция: если orders прокси умер, демпер OK    │
└─────────────────────────────────────────────────────┘
Детальная разбивка запросов по модулям:
1️⃣ Демпер (demper_instance.py)
Эндпоинты:

GET https://kaspi.kz/yml/offer-view/offers/{product_id} - цены конкурентов
POST https://mc.shop.kaspi.kz/pricefeed/upload/merchant/process - обновление цены
Математика для 500 товаров:

Фаза 1: parse_product_by_sku → 500 запросов
Фаза 2: sync_product (обновление цены) → 500 запросов
ИТОГО: ~1000 запросов на цикл (каждые 3 минуты)
С 70 прокси:

1000 / 249 = 4.02 → 5 прокси на цикл
70 / 5 = 14 циклов без повтора
14 × 3 минуты = 42 минуты отдыха для первого прокси
✅ БЕЗОПАСНО для 24/7: 42 мин > 40 мин (минимальный отдых)
2️⃣ Мои продажи (orders_worker.py)
Эндпоинт:

GET https://kaspi.kz/shop/api/v2/orders - список заказов
Математика (WORST CASE - большой магазин):

На 1 магазин: 200-500 заказов (по данным юзера!)
Каждый заказ: ~2 запроса (fetch + details с пагинацией)
Worst case: 500 заказов × 2 = 1000 запросов
Интервал: каждые 10 минут (настройка ORDERS_POLLING_INTERVAL)
С 25 прокси:

1000 / 249 = 4.02 → 5 прокси на цикл
25 / 5 = 5 циклов без повтора
5 × 10 минут = 50 минут отдыха для первого прокси
✅ БЕЗОПАСНО: 50 мин > 40 мин (минимальный отдых)
✅ Критично для больших магазинов! (без прокси = блокировка)
3️⃣ Синхронизация каталога (kaspi.py → get_products)
Эндпоинт:

GET https://mc.shop.kaspi.kz/bff/offer-view/list - список товаров
Математика:

На магазин с 1000 товаров: ~10-20 запросов (пагинация по 50-100)
Вызывается: по запросу пользователя (кнопка "Синхронизировать")
ИТОГО: ~20 запросов за раз, редко (1-2 раза в день)
С 5 прокси:

20 / 249 = 0.08 → 1 прокси хватит
5 прокси = можно синхронизировать до 5 магазинов параллельно
✅ Достаточно! (синхронизация редкая операция)
ИТОГО за полный цикл демпинга (3 минуты):

Демпер:                    1000 запросов (70 прокси, 5 активных)
Orders (если совпадает):   до 1000 запросов (25 прокси, 5 активных) 🔥
Каталог (если нажали):     ~20 запросов (5 прокси, 1 активный)

ОДНОВРЕМЕННО:              до 2020 запросов с РАЗНЫХ прокси!
                           Kaspi видит это как трафик от 100 разных юзеров ✅
                           НЕТ БЛОКИРОВОК, НЕТ ОЧЕРЕДЕЙ! 🚀
Почему 100 прокси хватает на 24/7:

Демпер: 70 прокси, 5 за цикл, 14 циклов × 3 мин = 42 мин отдыха ✅
Orders: 25 прокси, 5 за цикл, 5 циклов × 10 мин = 50 мин отдыха ✅
Catalog: 5 прокси, редкое использование, всегда доступны ✅
Критическое преимущество per-module pools:


БЕЗ разделения (старый подход):
  Демпер использует прокси 1-5 → Orders ждет → ОЧЕРЕДЬ! ❌

С разделением (новый подход):
  Демпер использует прокси 1-70
  Orders использует прокси 71-95   ← ПАРАЛЛЕЛЬНО! ✅
  Catalog использует прокси 96-100
Первые прокси "отдыхают" 48 минут → безопасная ротация ✅
Даже при 1000 товаров:

1000 × 2 / 249 = 8 прокси на цикл
100 / 8 = 12 циклов = 36 минут → прокси все еще не повторяются ✅
Решение: Per-User Proxy Pool Architecture
Компоненты системы:
Proxy Provider Integration - закупка прокси
Proxy Pool Manager - управление пулом прокси
User Proxy Allocator - выделение прокси юзерам
Proxy Rotator - ротация каждые 249 запросов
Billing Integration - автоматизация после оплаты
Стратегия 1: Database Schema (P0 - Критично)
Цель: Хранить прокси, привязывать к юзерам, трекать использование

SQL Schema:


-- Таблица прокси
CREATE TABLE proxies (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Proxy connection details
    host VARCHAR(255) NOT NULL,
    port INTEGER NOT NULL,
    protocol VARCHAR(10) DEFAULT 'http',  -- http, socks5
    username VARCHAR(255),
    password VARCHAR(255),

    -- User allocation
    user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    allocated_at TIMESTAMP,  -- Когда выделен юзеру
    status VARCHAR(20) DEFAULT 'available',  -- available, allocated, resting, dead

    -- ✨ NEW: Module assignment (per-module proxy pools)
    module VARCHAR(20) DEFAULT NULL,  -- 'demper', 'orders', 'catalog', 'reserve', NULL
    -- NULL = не выделен никакому модулю (available в общем пуле)

    -- Rotation tracking (в памяти worker'а, но backup в БД)
    requests_count INTEGER DEFAULT 0,
    max_requests INTEGER DEFAULT 249,  -- ⚠️ 249, не 250!
    last_used_at TIMESTAMP,
    available_at TIMESTAMP,  -- Когда снова можно использовать (после resting)

    -- Health monitoring
    success_count INTEGER DEFAULT 0,
    failure_count INTEGER DEFAULT 0,
    last_error TEXT,
    last_health_check TIMESTAMP,

    -- Metadata
    country VARCHAR(10) DEFAULT 'NL',  -- Netherlands IPv6
    provider VARCHAR(50),
    cost_usd DECIMAL(10,4),  -- Для биллинга
    is_residential BOOLEAN DEFAULT FALSE,

    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),

    -- Indexes
    INDEX idx_proxies_user_status (user_id, status, available_at),
    INDEX idx_proxies_user_module (user_id, module, status, available_at),  -- ✨ NEW: для модульного доступа
    INDEX idx_proxies_available (status, user_id) WHERE status = 'available',
    UNIQUE (host, port)
);

-- Таблица истории использования (для аналитики)
CREATE TABLE proxy_usage_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    proxy_id UUID REFERENCES proxies(id) ON DELETE CASCADE,
    user_id UUID REFERENCES users(id) ON DELETE CASCADE,

    requests_made INTEGER,
    success_count INTEGER,
    failure_count INTEGER,

    started_at TIMESTAMP,
    finished_at TIMESTAMP,

    INDEX idx_usage_proxy_time (proxy_id, started_at DESC),
    INDEX idx_usage_user_time (user_id, started_at DESC)
);
Миграция:

Создать: new-backend/migrations/versions/20260131_add_proxy_pool.py
Стратегия 2: Proxy Allocator - Выделение после оплаты (P0 - Критично)
Цель: Автоматически выделить 100 прокси юзеру после оплаты подписки

Логика:


# new-backend/app/services/proxy_allocator.py

class ProxyAllocator:
    """Выделение прокси юзерам после оплаты"""

    async def allocate_proxies_to_user(
        self,
        user_id: UUID,
        count: int = 100,
        distribution: dict = None  # ✨ NEW: разбивка по модулям
    ) -> Dict[str, List[Proxy]]:
        """
        Выделить N прокси юзеру из available пула с разбивкой по модулям

        По умолчанию:
        - 70 прокси → демпер
        - 15 прокси → orders
        - 10 прокси → catalog
        - 5 прокси → reserve

        Steps:
        1. Найти available прокси (status='available', user_id IS NULL)
        2. Если недостаточно → закупить новые (см. proxy_provider)
        3. Присвоить юзеру с указанием module
        4. Записать в лог
        """

        if distribution is None:
            distribution = {
                'demper': 70,    # Основная нагрузка (500 товаров × 2 = 1000 req)
                'orders': 25,    # Мои продажи (200-500 заказов = до 1000 req!)
                'catalog': 5,    # Синхронизация каталога (редко)
                'reserve': 0     # Резерв (используем catalog прокси при необходимости)
            }

        total_needed = sum(distribution.values())
        if total_needed != count:
            raise ValueError(f"Distribution sum {total_needed} != requested {count}")

        result = {}
        async with pool.acquire() as conn:
            for module, module_count in distribution.items():
                # Взять N прокси для модуля
                query = """
                    UPDATE proxies
                    SET user_id = $1,
                        status = 'allocated',
                        module = $2,
                        allocated_at = NOW()
                    WHERE id IN (
                        SELECT id FROM proxies
                        WHERE status = 'available'
                          AND user_id IS NULL
                        ORDER BY created_at ASC
                        LIMIT $3
                        FOR UPDATE SKIP LOCKED
                    )
                    RETURNING *
                """
                proxies = await conn.fetch(query, user_id, module, module_count)

                if len(proxies) < module_count:
                    # Недостаточно прокси в пуле → нужно закупить
                    shortage = module_count - len(proxies)
                    logger.warning(f"Proxy pool shortage for {module}: {shortage} proxies needed")

                    # TODO: Интеграция с Proxy Provider API
                    # await proxy_provider.purchase_proxies(shortage)

                    raise InsufficientProxiesError(
                        f"Only {len(proxies)}/{module_count} proxies available for {module}"
                    )

                result[module] = [Proxy(**p) for p in proxies]
                logger.info(f"Allocated {len(proxies)} proxies to user {user_id} for module {module}")

            return result

    async def deallocate_proxies_from_user(self, user_id: UUID):
        """
        Освободить все прокси юзера (когда подписка кончилась)
        """
        query = """
            UPDATE proxies
            SET user_id = NULL,
                status = 'available',
                allocated_at = NULL,
                requests_count = 0,
                available_at = NULL
            WHERE user_id = $1
            RETURNING COUNT(*)
        """
Интеграция с биллингом:


# new-backend/app/routers/billing.py

@router.post("/subscribe")
async def subscribe_user(
    plan: SubscriptionPlan,
    current_user: dict = Depends(get_current_user),
    pool: asyncpg.Pool = Depends(get_db_pool)
):
    # ... payment processing ...

    if payment_successful:
        # Создать подписку
        subscription = await create_subscription(user_id, plan)

        # ✅ ВЫДЕЛИТЬ ПРОКСИ АВТОМАТИЧЕСКИ
        if plan in ['premium', 'ultra']:  # Только платные планы
            try:
                proxies = await proxy_allocator.allocate_proxies_to_user(
                    user_id=current_user['id'],
                    count=100
                )
                logger.info(f"Allocated {len(proxies)} proxies to user {current_user['id']}")
            except InsufficientProxiesError as e:
                logger.error(f"Failed to allocate proxies: {e}")
                # Отправить алерт админу
                await send_admin_alert(f"Proxy pool depleted! User {current_user['id']}")

        return {"subscription": subscription, "proxies_allocated": len(proxies)}
Webhook для истечения подписки:


# new-backend/app/workers/subscription_cleanup.py

async def cleanup_expired_subscriptions():
    """
    Крон-джоба: каждые 1 час проверяет истекшие подписки
    и освобождает их прокси обратно в пул
    """
    query = """
        SELECT user_id FROM subscriptions
        WHERE status = 'active'
          AND current_period_end < NOW()
    """

    expired_users = await conn.fetch(query)

    for user in expired_users:
        # Деактивировать подписку
        await deactivate_subscription(user['user_id'])

        # Освободить прокси
        await proxy_allocator.deallocate_proxies_from_user(user['user_id'])

        logger.info(f"Freed proxies from expired user {user['user_id']}")
Файлы:

Создать: new-backend/app/services/proxy_allocator.py (~200 строк)
Изменить: new-backend/app/routers/billing.py (добавить allocate после оплаты)
Создать: new-backend/app/workers/subscription_cleanup.py (~100 строк)
Стратегия 3: Proxy Rotator - Ротация каждые 249 запросов (P0 - Критично)
Цель: Автоматически менять прокси после 249 запросов (не 250!)

Ключевая логика:


# new-backend/app/core/proxy_rotator.py

class ProxyRotator:
    """
    Управляет ротацией прокси для юзера

    Логика:
    - Начинает с первого прокси
    - Трекает requests_count в памяти (быстро!)
    - После 249 запросов → берет следующий прокси
    - Использованный прокси → resting на 40 минут
    """

    def __init__(self, user_id: UUID, module: str = 'demper'):  # ✨ NEW: module параметр
        self.user_id = user_id
        self.module = module  # ✨ NEW: 'demper', 'orders', 'catalog', 'reserve'
        self.current_proxy: Optional[Proxy] = None
        self.current_requests_count = 0
        self.max_requests_per_proxy = 249  # ⚠️ Не 250!

        # Кеш прокси юзера в памяти (только для этого модуля!)
        self.user_proxies: List[Proxy] = []
        self.proxy_index = 0

    async def initialize(self):
        """
        Загрузить прокси юзера для ЭТОГО МОДУЛЯ из БД в память
        """
        query = """
            SELECT * FROM proxies
            WHERE user_id = $1
              AND module = $2
              AND status IN ('allocated', 'resting')
            ORDER BY
                CASE WHEN status = 'allocated' THEN 0 ELSE 1 END,
                available_at ASC NULLS FIRST,
                last_used_at ASC NULLS FIRST
        """
        proxies = await conn.fetch(query, self.user_id, self.module)
        self.user_proxies = [Proxy(**p) for p in proxies]

        if not self.user_proxies:
            raise NoProxiesAllocatedError(
                f"User {self.user_id} has no proxies for module '{self.module}'"
            )

        # Взять первый доступный
        self.current_proxy = await self._get_next_available_proxy()

    async def _get_next_available_proxy(self) -> Proxy:
        """
        Получить следующий доступный прокси

        Приоритет:
        1. status='allocated' (еще не использовался)
        2. status='resting' но available_at <= NOW() (отдохнул)
        3. Если все resting → взять с наименьшим available_at
        """
        now = datetime.now(timezone.utc)

        # Сортируем: allocated первыми, потом resting по available_at
        available = [
            p for p in self.user_proxies
            if p.status == 'allocated' or (
                p.status == 'resting' and
                (p.available_at is None or p.available_at <= now)
            )
        ]

        if available:
            return available[0]

        # Все еще resting → берем ближайший
        resting = [p for p in self.user_proxies if p.status == 'resting']
        if resting:
            # Ждем до available_at ближайшего
            next_proxy = min(resting, key=lambda p: p.available_at or now)
            wait_seconds = (next_proxy.available_at - now).total_seconds()

            if wait_seconds > 0:
                logger.warning(
                    f"All proxies resting, waiting {wait_seconds:.1f}s "
                    f"for proxy {next_proxy.id}"
                )
                await asyncio.sleep(wait_seconds)

            return next_proxy

        raise NoProxiesAvailableError("All proxies are dead or unavailable")

    async def get_current_proxy(self) -> Proxy:
        """
        Получить текущий прокси для использования

        Автоматически ротирует после 249 запросов!
        """
        if self.current_requests_count >= self.max_requests_per_proxy:
            # Достигли лимита → ротация
            await self._rotate_proxy()

        return self.current_proxy

    async def _rotate_proxy(self):
        """
        Сменить прокси на следующий

        1. Текущий прокси → resting (40 минут)
        2. Взять следующий available
        3. Сбросить счетчик
        """
        if self.current_proxy:
            # Отправить на отдых
            await self._set_proxy_resting(
                self.current_proxy.id,
                duration_minutes=40
            )

            logger.info(
                f"Proxy {self.current_proxy.id} rotated after "
                f"{self.current_requests_count} requests (resting 40min)"
            )

        # Взять следующий
        self.current_proxy = await self._get_next_available_proxy()
        self.current_requests_count = 0

        logger.info(f"Switched to proxy {self.current_proxy.id}")

    async def _set_proxy_resting(self, proxy_id: UUID, duration_minutes: int):
        """
        Отправить прокси на отдых
        """
        query = """
            UPDATE proxies
            SET status = 'resting',
                available_at = NOW() + INTERVAL '%s minutes',
                requests_count = 0  -- Сброс для следующего использования
            WHERE id = $1
        """
        await conn.execute(query % duration_minutes, proxy_id)

        # Обновить в кеше
        for p in self.user_proxies:
            if p.id == proxy_id:
                p.status = 'resting'
                p.available_at = datetime.now(timezone.utc) + timedelta(
                    minutes=duration_minutes
                )

    async def record_request(self, success: bool):
        """
        Записать использование прокси

        Вызывается ПОСЛЕ каждого запроса к Kaspi API
        """
        self.current_requests_count += 1

        if success:
            self.current_proxy.success_count += 1
        else:
            self.current_proxy.failure_count += 1

            # Если слишком много ошибок → mark as dead
            failure_rate = (
                self.current_proxy.failure_count /
                (self.current_proxy.success_count + self.current_proxy.failure_count)
            )

            if failure_rate > 0.5 and self.current_proxy.failure_count > 10:
                logger.error(
                    f"Proxy {self.current_proxy.id} has high failure rate "
                    f"({failure_rate:.1%}), marking as dead"
                )
                await self._mark_proxy_dead(self.current_proxy.id)

                # Ротировать немедленно
                await self._rotate_proxy()

    async def _mark_proxy_dead(self, proxy_id: UUID):
        """Пометить прокси как мертвый"""
        query = "UPDATE proxies SET status = 'dead' WHERE id = $1"
        await conn.execute(query, proxy_id)

        # Удалить из кеша
        self.user_proxies = [p for p in self.user_proxies if p.id != proxy_id]

        # Алерт админу
        await send_admin_alert(f"Proxy {proxy_id} marked as dead")
Интеграция с API Parser:


# new-backend/app/services/api_parser.py

# Добавить в начале файла:
# ✨ NEW: Per-module rotators
proxy_rotators: Dict[tuple[UUID, str], ProxyRotator] = {}  # (user_id, module) → ProxyRotator

async def get_user_proxy_rotator(user_id: UUID, module: str = 'demper') -> ProxyRotator:
    """
    Получить ProxyRotator для юзера и модуля (singleton в памяти)

    Каждая комбинация (user_id, module) имеет свой rotator!
    """
    cache_key = (user_id, module)

    if cache_key not in proxy_rotators:
        rotator = ProxyRotator(user_id, module=module)
        await rotator.initialize()
        proxy_rotators[cache_key] = rotator

    return proxy_rotators[cache_key]


async def parse_product_by_sku(
    product_sku: str,
    user_id: UUID,  # ← Новый параметр!
    use_proxy: bool = True,
    ...
):
    """
    Получить цены конкурентов с прокси-ротацией
    """

    if use_proxy:
        # Получить текущий прокси юзера
        rotator = await get_user_proxy_rotator(user_id)
        proxy = await rotator.get_current_proxy()  # Авто-ротация!

        # Создать клиента с прокси
        proxy_url = f"{proxy.protocol}://{proxy.username}:{proxy.password}@{proxy.host}:{proxy.port}"

        client = httpx.AsyncClient(
            proxies={"http://": proxy_url, "https://": proxy_url},
            timeout=httpx.Timeout(30.0),
        )
    else:
        client = await get_http_client()

    try:
        # Сделать запрос
        response = await client.post(kaspi_url, json=payload)

        # ✅ Записать успешный запрос
        if use_proxy:
            await rotator.record_request(success=True)

        return parse_response(response)

    except Exception as e:
        # ❌ Записать ошибку
        if use_proxy:
            await rotator.record_request(success=False)

        raise
Интеграция с demper_instance.py:


# new-backend/app/workers/demper_instance.py

async def process_products(products: List[Product], user_id: UUID):
    """
    Обработать товары для демпинга

    ✨ Использует module='demper' прокси пул (70 прокси)
    """
    for product in products:
        # Парсим цены конкурентов (с прокси ротацией для демпера)
        competitors = await parse_product_by_sku(
            product_sku=product.sku,
            user_id=user_id,  # ✨ Передаем user_id
            use_proxy=True    # ✨ Включаем прокси
        )
        # get_user_proxy_rotator автоматически использует module='demper' (default)

        # Синхронизируем цену если нужно
        if should_update_price(product, competitors):
            await sync_product(
                product_id=product.id,
                new_price=calculate_new_price(competitors),
                user_id=user_id,  # ✨ Передаем user_id для прокси ротации
                use_proxy=True
            )
Интеграция с orders_worker.py:


# new-backend/app/workers/orders_worker.py

# ✨ NEW: Изменить интервал опроса с 60 на 600 секунд
ORDERS_POLLING_INTERVAL = 600  # 10 минут (было 60 секунд)

async def fetch_orders_for_store(store_id: UUID, user_id: UUID):
    """
    Получить заказы из Kaspi для магазина

    ✨ Использует module='orders' прокси пул (15 прокси)
    """
    # Получить rotator для модуля 'orders'
    rotator = await get_user_proxy_rotator(user_id, module='orders')  # ✨ module='orders'!
    proxy = await rotator.get_current_proxy()

    # Создать клиента с прокси
    proxy_url = f"{proxy.protocol}://{proxy.username}:{proxy.password}@{proxy.host}:{proxy.port}"

    try:
        # Запрос к Kaspi orders API
        response = await http_client.get(
            "https://kaspi.kz/shop/api/v2/orders",
            proxies={"http://": proxy_url, "https://": proxy_url}
        )

        # ✅ Успех
        await rotator.record_request(success=True)

        return response.json()

    except Exception as e:
        # ❌ Ошибка
        await rotator.record_request(success=False)
        raise
Интеграция с catalog sync (kaspi.py):


# new-backend/app/services/kaspi.py

async def get_products(store_id: UUID, user_id: UUID) -> List[Product]:
    """
    Синхронизация каталога товаров

    ✨ Использует module='catalog' прокси пул (10 прокси)
    """
    rotator = await get_user_proxy_rotator(user_id, module='catalog')  # ✨ module='catalog'!
    proxy = await rotator.get_current_proxy()

    proxy_url = f"{proxy.protocol}://{proxy.username}:{proxy.password}@{proxy.host}:{proxy.port}"

    try:
        response = await http_client.get(
            "https://mc.shop.kaspi.kz/bff/offer-view/list",
            proxies={"http://": proxy_url, "https://": proxy_url}
        )

        await rotator.record_request(success=True)
        return parse_products(response.json())

    except Exception as e:
        await rotator.record_request(success=False)
        raise
Файлы:

Создать: new-backend/app/core/proxy_rotator.py (~350 строк)
Изменить: new-backend/app/services/api_parser.py (добавить use_proxy, rotator, per-module support)
Изменить: new-backend/app/workers/demper_instance.py (передавать user_id, module='demper')
Изменить: new-backend/app/workers/orders_worker.py (использовать module='orders', ORDERS_POLLING_INTERVAL=600)
Изменить: new-backend/app/services/kaspi.py (использовать module='catalog')
Стратегия 4: Интеллектуальная приоритизация товаров (P1 - Опционально)
Цель: Не демпить все 500 товаров одновременно, а только приоритетные

Критерии приоритета:

Hot products (изменение цены за последние 24ч)
High margin (большая разница между мин. ценой и конкурентами)
Recent activity (недавние заказы/просмотры)
Low priority (стабильные цены, низкий спрос)
Алгоритм:


# В demper_instance.py, fetch_products_for_instance():

SELECT
    p.*,
    CASE
        WHEN ph.price_changed_at > NOW() - INTERVAL '24 hours' THEN 1  -- Hot
        WHEN p.current_price - p.min_price > 1000 THEN 2                -- High margin
        WHEN p.last_order_at > NOW() - INTERVAL '7 days' THEN 3         -- Active
        ELSE 4                                                           -- Low priority
    END as priority
FROM products p
LEFT JOIN (
    SELECT product_id, MAX(created_at) as price_changed_at
    FROM price_history
    WHERE created_at > NOW() - INTERVAL '24 hours'
    GROUP BY product_id
) ph ON ph.product_id = p.id
WHERE p.bot_active = TRUE
ORDER BY priority ASC, p.last_check_time ASC NULLS FIRST
LIMIT 100  -- Уменьшить с 500 до 100 за цикл
Адаптивный интервал:

Priority 1 (Hot): каждые 3 минуты
Priority 2 (High margin): каждые 10 минут
Priority 3 (Active): каждые 30 минут
Priority 4 (Low): каждые 2 часа
Файлы:

Изменить: new-backend/app/workers/demper_instance.py (строки 303-350)
Добавить: new-backend/migrations/versions/20260131_add_product_priority.py
Стратегия 5: Proxy Provider Integration (P0 - Критично)
Цель: Автоматическая закупка прокси когда пул истощается

Провайдеры (на выбор):

Proxy6.net - IPv6 прокси из Нидерландов

Цена: ~$11 за 100 прокси
API: https://proxy6.net/developers
Поддержка: HTTP, SOCKS5
Альтернативы (если нидерландские не работают):

Soax.com - Казахстанские residential (дороже, ~$100/100 прокси)
Brightdata - Премиум, но надежные
API Integration:


# new-backend/app/services/proxy_provider.py

import httpx
from typing import List

class ProxyProviderClient:
    """
    Интеграция с Proxy6.net API
    """

    def __init__(self):
        self.api_key = settings.proxy6_api_key
        self.base_url = "https://proxy6.net/api"

    async def purchase_proxies(
        self,
        count: int,
        period_days: int = 30,
        country: str = "nl",  # Netherlands
        version: int = 6  # IPv6
    ) -> List[ProxyCredentials]:
        """
        Закупить прокси через API

        Docs: https://proxy6.net/developers/buy
        """
        url = f"{self.base_url}/{self.api_key}/buy/{count}/{period_days}/{country}/{version}"

        async with httpx.AsyncClient() as client:
            response = await client.get(url)
            data = response.json()

            if data['status'] != 'yes':
                raise ProxyPurchaseError(f"Failed to buy proxies: {data}")

            # Парсим купленные прокси
            proxies = []
            for proxy_id, proxy_data in data['list'].items():
                proxies.append(ProxyCredentials(
                    host=proxy_data['host'],
                    port=proxy_data['port_http'],
                    username=proxy_data['user'],
                    password=proxy_data['pass'],
                    protocol='http',
                    country=country
                ))

            return proxies

    async def check_balance(self) -> float:
        """
        Проверить баланс на Proxy6.net
        """
        url = f"{self.base_url}/{self.api_key}/getbalance"

        async with httpx.AsyncClient() as client:
            response = await client.get(url)
            data = response.json()

            if data['status'] == 'yes':
                return float(data['balance'])

            raise ProxyProviderError(f"Failed to get balance: {data}")


# Интеграция с ProxyAllocator:

async def ensure_proxy_pool_sufficient(required_count: int = 500):
    """
    Проверить, достаточно ли прокси в пуле
    Если нет → закупить автоматически
    """
    query = "SELECT COUNT(*) FROM proxies WHERE status = 'available'"
    available_count = await conn.fetchval(query)

    if available_count < required_count:
        shortage = required_count - available_count

        logger.warning(f"Proxy pool low: {available_count}/{required_count}, buying {shortage}")

        # Проверить баланс
        provider = ProxyProviderClient()
        balance = await provider.check_balance()

        estimated_cost = (shortage / 100) * 11  # $11 per 100 proxies

        if balance < estimated_cost:
            await send_admin_alert(
                f"⚠️ Low proxy provider balance: ${balance:.2f}, need ${estimated_cost:.2f}"
            )
            raise InsufficientFundsError("Not enough funds to buy proxies")

        # Закупить
        new_proxies = await provider.purchase_proxies(count=shortage)

        # Добавить в БД
        for proxy in new_proxies:
            await conn.execute(
                """
                INSERT INTO proxies (host, port, username, password, protocol, country, status)
                VALUES ($1, $2, $3, $4, $5, $6, 'available')
                """,
                proxy.host, proxy.port, proxy.username,
                proxy.password, proxy.protocol, proxy.country
            )

        logger.info(f"✅ Purchased {len(new_proxies)} new proxies")

        return new_proxies
Крон-джоба для автоматической закупки:


# new-backend/app/workers/proxy_pool_monitor.py

async def monitor_proxy_pool():
    """
    Каждые 6 часов проверяет уровень прокси в пуле
    Если < 500 available → автоматически закупает
    """
    while True:
        try:
            await ensure_proxy_pool_sufficient(required_count=500)
        except Exception as e:
            logger.error(f"Proxy pool monitoring error: {e}")

        await asyncio.sleep(6 * 3600)  # 6 часов
Environment Variables:


# .env
PROXY6_API_KEY=your_proxy6_api_key_here
PROXY_POOL_MIN_SIZE=500  # Минимум прокси в пуле
PROXY_AUTO_PURCHASE=true  # Автоматическая закупка
Файлы:

Создать: new-backend/app/services/proxy_provider.py (~200 строк)
Создать: new-backend/app/workers/proxy_pool_monitor.py (~100 строк)
Изменить: new-backend/app/config.py (добавить PROXY6_API_KEY)
Стратегия 6: Батчинг обновлений цен (P2 - Опционально)
Цель: Вместо 500 individual запросов → 10-20 batch запросов

Проблема: Kaspi API pricefeed/upload/merchant/process принимает:

Либо один товар (SKU + цена)
Либо XML файл со списком товаров
Решение: Использовать XML batch upload

Реализация:

Создать batch_sync_products() в api_parser.py:


async def batch_sync_products(
    products: List[ProductUpdate],
    session: dict,
    batch_size: int = 50
) -> BatchResult:
    """
    Обновляет цены батчем через XML upload
    1 запрос вместо 50!
    """
    xml = generate_pricefeed_xml(products)  # YML формат Kaspi

    await kaspi_rate_limiter.acquire()  # Один запрос на батч

    response = await http_client.post(
        "https://mc.shop.kaspi.kz/pricefeed/upload/xml",
        files={"file": ("pricefeed.xml", xml, "application/xml")},
        headers=get_auth_headers(session)
    )
Изменить демпинг цикл:

Собрать все обновления в список
Группировать по 50 товаров
Отправить батчем вместо по одному
Экономия:

Было: 500 товаров × 1 запрос = 500 запросов
Стало: 500 товаров / 50 батч = 10 запросов
Экономия: 50x!
Файлы:

Изменить: new-backend/app/services/api_parser.py (+150 строк)
Изменить: new-backend/app/workers/demper_instance.py (изменить логику sync)
Стратегия 4: Кеширование цен конкурентов (P1 - Высокий)
Цель: Не запрашивать цены конкурентов каждые 3 минуты, если они не изменились

Решение:

Redis кеш для competitor prices:


# В parse_product_by_sku():

cache_key = f"kaspi:competitors:{product_sku}"
cached = await redis.get(cache_key)

if cached and not force_refresh:
    return json.loads(cached)

# Fetch from Kaspi API
await kaspi_rate_limiter.acquire()
response = await http_client.post(kaspi_url, ...)

# Кешировать на 5 минут
await redis.setex(cache_key, 300, json.dumps(result))
Smart refresh strategy:

Если кеш есть и < 5 мин → использовать кеш
Если товар Hot (priority 1) → force_refresh=True
Если товар Low priority → кеш на 30 минут
Экономия:

Priority 1 (10% товаров): refresh каждые 3 мин
Priority 2-3 (40%): кеш 10 мин → экономия 70%
Priority 4 (50%): кеш 30 мин → экономия 90%
Средняя экономия: ~60% запросов!
Файлы:

Изменить: new-backend/app/services/api_parser.py (строки 341-450)
Использовать существующий Redis client
Стратегия 5: Очередь с приоритетами (P2 - Средний)
Цель: Urgent запросы (продажи, предзаказы) имеют приоритет над демпингом

Реализация:

Priority Queue в Redis:


# Приоритеты:
# 1 - Срочные (orders, preorders)
# 2 - Демпинг hot товаров
# 3 - Демпинг обычных товаров
# 4 - Синхронизация каталога

await redis.zadd(
    "kaspi:request_queue",
    {
        json.dumps(request): priority_score
    }
)
Обработчик очереди:

Worker читает из очереди по приоритету
Уважает Kaspi rate limit
Если лимит исчерпан → откладывает в очередь
Файлы:

Создать: new-backend/app/core/priority_queue.py (новый файл, ~200 строк)
Изменить: new-backend/app/workers/demper_instance.py (использовать очередь)
Стратегия 6: Адаптивный backoff при 429 (P1 - Высокий)
Цель: При получении 429 от Kaspi → умная задержка

Текущая проблема:


# api_parser.py, строка 297:
if response.status_code == 429:
    await asyncio.sleep(random.uniform(0.5, 2.0))  # ❌ Слишком короткая!
Решение - Exponential Backoff:


async def smart_backoff_on_429(attempt: int, max_attempts: int = 5):
    """
    Экспоненциальная задержка с jitter

    Attempt 1: 5-10 сек
    Attempt 2: 30-60 сек
    Attempt 3: 2-5 мин
    Attempt 4: 10-20 мин
    Attempt 5: Fail with CircuitOpenError
    """
    base_delay = 5 * (2 ** attempt)  # Exponential
    jitter = random.uniform(0.5, 1.5)  # +/- 50%
    delay = min(base_delay * jitter, 20 * 60)  # Max 20 мин

    logger.warning(f"429 detected, backing off for {delay:.1f}s (attempt {attempt})")
    await asyncio.sleep(delay)
Дополнительно:

После 429 → снизить Kaspi rate limit до 50% на 30 минут
После 3 подряд 429 → Circuit Breaker → OPEN state
Оповестить администратора через webhook
Файлы:

Изменить: new-backend/app/services/api_parser.py (строки 269-310)
Изменить: new-backend/app/core/circuit_breaker.py (добавить 429 handling)
Итоговая оптимизация
До оптимизации:

500 товаров × 2 запроса (parse + sync) = 1000 запросов / 3 мин
= 333 запроса/мин
Превышение: 333 / 8.33 = 40x ❌
После оптимизации:
С приоритизацией (100 товаров/цикл):

Priority 1 (10 товаров): 10 × 1 parse = 10 запросов
Priority 2-4 (90 товаров): Кеш, parse только при необходимости = ~30 запросов
ИТОГО parse: ~40 запросов
С батчингом (50 товаров/батч):

100 товаров / 50 = 2 batch sync запроса
Итого за цикл:


40 parse + 2 batch sync = 42 запроса / 3 мин = 14 запросов/мин
Соотношение: 14 / 8.33 = 1.68x ⚠️ (все еще превышение, но приемлемое)
С кешированием (60% экономия):


14 × 0.4 = 5.6 запросов/мин
Соотношение: 5.6 / 8.33 = 0.67x ✅ (в пределах лимита!)
Плюс buffer для предзаказов/продаж:

Остается ~2.7 запросов/мин для других операций ✅
Итоговая эффективность
Без прокси (текущая ситуация):

500 товаров × 2 запроса = 1000 запросов / 3 мин = 333 запросов/мин
Kaspi лимит: 8.33 запросов/мин
Превышение: 40x ❌ → БАН
С per-user proxy pool (решение):

Юзер использует 6 своих прокси за цикл:
- Прокси 1: 249 запросов (parse товары 1-249)
- Прокси 2: 249 запросов (parse товары 250-498)
- Прокси 3: 2 запроса (parse товары 499-500)
- Прокси 4: 249 запросов (sync товары 1-249)
- Прокси 5: 249 запросов (sync товары 250-498)
- Прокси 6: 2 запроса (sync товары 499-500)

ИТОГО: 1000 запросов за ~2 минуты ✅
Каждый прокси: 249 < 250 лимита ✅
Никаких банов ✅
Преимущества:

✅ Мгновенный демпинг - все 500 товаров за 2 минуты
✅ Конкурентное преимущество - как у Алгатоп и Деметра
✅ Масштабируемость - до 1000+ товаров на юзера
✅ Изоляция - прокси юзера не влияют на других
✅ Автоматизация - выделение после оплаты
План реализации
Phase 0: Тестирование прокси (День 1)
Цель: Убедиться что нидерландские IPv6 работают с Kaspi

Купить 10 тестовых прокси (1 час)

Proxy6.net: 10 прокси IPv6 NL за ~$1.10
Получить credentials
Тестовый скрипт (2 часа)


# scripts/test_proxies.py
async def test_kaspi_with_proxy(proxy):
    # Тест 1: Доступ к kaspi.kz
    # Тест 2: Запрос к mc.shop.kaspi.kz (merchant center)
    # Тест 3: Parse product API
    # Тест 4: Sync product API
Анализ результатов (1 час)

Если работают ✅ → продолжаем с NL IPv6
Если НЕ работают ❌ → нужны KZ residential (дороже)
Итого Phase 0: ~4 часа (0.5 дня)

КРИТИЧНО: Не начинать Phase 1 пока не подтверждено что прокси работают!

Phase 1: Database & Core Infrastructure (День 2-3)
Миграция БД - Proxy Tables (3 часа)

Создать: migrations/20260131_add_proxy_pool.py
Таблицы: proxies, proxy_usage_log
Индексы
Proxy Models (2 часа)

Создать: app/models/proxy.py
Pydantic schemas
ProxyRotator класс (6 часов)

Создать: app/core/proxy_rotator.py
Логика ротации каждые 249 запросов
In-memory tracking
Тесты (unit tests)
ProxyAllocator сервис (4 часа)

Создать: app/services/proxy_allocator.py
allocate_proxies_to_user()
deallocate_proxies_from_user()
Итого Phase 1: ~15 часов (2 дня)

Phase 2: Proxy Provider Integration (День 4)
Proxy6.net API Client (4 часа)

Создать: app/services/proxy_provider.py
purchase_proxies()
check_balance()
Тесты с реальным API (staging)
Автоматическая закупка (3 часа)

ensure_proxy_pool_sufficient()
Крон-джоба: proxy_pool_monitor.py
Первичное наполнение пула (1 час)

Скрипт: scripts/initial_proxy_purchase.py
Закупить 500 прокси для старта
Добавить в БД
Итого Phase 2: ~8 часов (1 день)

Phase 3: Integration с демпингом (День 5-6)
Изменить api_parser.py (6 часов)

Добавить use_proxy параметр
Интеграция с ProxyRotator (per-module support)
get_user_proxy_rotator(user_id, module='demper')
record_request() после каждого вызова
Поддержка разных модулей через cache_key=(user_id, module)
Изменить demper_instance.py (4 часов)

Передавать user_id в parse_product_by_sku
Передавать user_id в sync_product
Автоматически использует module='demper' (70 прокси)
Логирование прокси-ротации
Изменить orders_worker.py (3 часа)

Интегрировать get_user_proxy_rotator(user_id, module='orders')
Изменить ORDERS_POLLING_INTERVAL с 60 на 600 секунд (10 минут)
Использует свои 15 прокси (не конкурирует с демпером!)
Изменить kaspi.py (catalog sync) (2 часа)

Интегрировать get_user_proxy_rotator(user_id, module='catalog')
Использует свои 10 прокси для синхронизации каталога
Playwright интеграция (6 часов)

Изменить: app/core/browser_farm.py
launch_browser_with_proxy()
Использует module='reserve' прокси (5 штук)
Передавать прокси в kaspi_auth_service
Итого Phase 3: ~21 часов (2.5 дня)

Phase 4: Billing Integration (День 7)
Автоматическое выделение при оплате (3 часа)

Изменить: app/routers/billing.py
После успешной оплаты → allocate_proxies_to_user()
Обработка ошибок (InsufficientProxiesError)
Subscription cleanup worker (3 часа)

Создать: app/workers/subscription_cleanup.py
Крон каждые 1 час
Освобождает прокси от истекших подписок
Admin панель для прокси (4 часа)

Dashboard: сколько прокси в пуле
Сколько выделено юзерам
Health check статус
Кнопка "Закупить прокси вручную"
Итого Phase 4: ~10 часов (1.5 дня)

Phase 5: Мониторинг и алерты (День 8)
Метрики (4 часа)

Endpoint: /health/proxies
Metrics: available_count, allocated_count, dead_count
Per-user: proxies_used, current_proxy, requests_on_current
Алерты (2 часа)

Webhook если proxy pool < 100
Webhook если прокси юзера все dead
Email админу при закупке прокси
Логирование (2 часа)

Structured logging для proxy rotation
Grafana/Loki интеграция (опционально)
Итого Phase 5: ~8 часов (1 день)

Phase 6: Тестирование (День 9-10)
Unit тесты (6 часов)

ProxyRotator tests
ProxyAllocator tests
Mock Proxy6.net API
Integration тесты (6 часов)

Полный цикл: оплата → выделение → демпинг
Тест ротации на 500 товарах
Тест resting и reuse
Load тестирование (4 часов)

10 юзеров × 500 товаров одновременно
Проверка isolation (не конкурируют за прокси)
Memory leaks check
Итого Phase 6: ~16 часов (2 дня)

Общий timeline
Phase	Дни	Часы	Описание
Phase 0	0.5	4	Тестирование прокси с Kaspi
Phase 1	2	15	Database & ProxyRotator
Phase 2	1	8	Proxy Provider интеграция
Phase 3	2.5	21	Демпинг + per-module интеграция
Phase 4	1.5	10	Billing интеграция
Phase 5	1	8	Мониторинг
Phase 6	2	16	Тестирование
ИТОГО	10.5 дней	82 часа	Full implementation
С 1 разработчиком: 10 рабочих дней (2 недели)
С 2 разработчиками: 5-6 рабочих дней (1 неделя)

Критические файлы для создания/изменения
Новые файлы (создать):
Файл	Назначение	Строки	Приоритет
migrations/versions/20260131_add_proxy_pool.py	DB schema для прокси	~100	P0
app/models/proxy.py	Proxy models	~80	P0
app/core/proxy_rotator.py	Ротация каждые 249 запросов	~350	P0
app/services/proxy_allocator.py	Выделение прокси юзерам	~200	P0
app/services/proxy_provider.py	Proxy6.net API client	~200	P0
app/workers/proxy_pool_monitor.py	Крон для автозакупки	~100	P0
app/workers/subscription_cleanup.py	Освобождение прокси	~100	P1
scripts/test_proxies.py	Тестирование прокси	~150	P0
scripts/initial_proxy_purchase.py	Первичная закупка	~50	P1
Изменяемые файлы:
Файл	Изменения	Строки	Приоритет
app/services/api_parser.py	Интеграция ProxyRotator, use_proxy параметр, per-module support	+200	P0
app/workers/demper_instance.py	Передача user_id, module='demper'	+50	P0
app/workers/orders_worker.py	module='orders', ORDERS_POLLING_INTERVAL=600 (10 мин)	+60	P0
app/services/kaspi.py	module='catalog' для синхронизации	+40	P0
app/routers/billing.py	Выделение прокси с per-module distribution	+30	P0
app/config.py	PROXY6_API_KEY, proxy settings	+15	P0
app/core/browser_farm.py	launch_browser_with_proxy()	+50	P1
app/services/kaspi_auth_service.py	Использование прокси для auth	+30	P1
Верификация
Phase 0: Тестирование прокси

# 1. Запустить тестовый скрипт
python scripts/test_proxies.py

# Ожидаемый вывод:
# ✅ Proxy 123.45.67.89:8080 - kaspi.kz accessible
# ✅ Proxy 123.45.67.89:8080 - merchant center accessible
# ✅ Proxy 123.45.67.89:8080 - parse product API works
# ✅ Proxy 123.45.67.89:8080 - sync product API works
# Total: 9/10 proxies working (90% success rate)

# Если < 80% success → нужны другие прокси (KZ residential)
Phase 1-4: Интеграционная проверка

-- 1. Проверить прокси в БД
SELECT
    status,
    COUNT(*) as count,
    COUNT(DISTINCT user_id) as users
FROM proxies
GROUP BY status;

-- Ожидается:
-- available  | 400 | 0      (свободные)
-- allocated  | 100 | 1      (выделены юзеру)
-- resting    | 0   | 0      (отдыхают)
-- dead       | 0   | 0      (мертвые)

# 2. Тест оплаты и выделения прокси
curl -X POST http://backend:8010/billing/subscribe \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"plan": "premium"}'

# Проверить в БД:
psql -c "SELECT COUNT(*) FROM proxies WHERE user_id = 'USER_UUID'"
# Должно быть: 100

# ✨ Проверить разбивку по модулям:
psql -c "
SELECT
    module,
    COUNT(*) as proxy_count
FROM proxies
WHERE user_id = 'USER_UUID'
GROUP BY module
ORDER BY module;
"

# Ожидаемый результат:
#  module   | proxy_count
# ----------+-------------
#  demper   | 70
#  orders   | 25
#  catalog  | 5

# 3. Тест ротации прокси (per-module)
async def test_rotation():
    # Тест демпер модуля
    demper_rotator = ProxyRotator(user_id=test_user_id, module='demper')
    await demper_rotator.initialize()

    # Сделать 500 запросов
    for i in range(500):
        proxy = await demper_rotator.get_current_proxy()
        await demper_rotator.record_request(success=True)

        if i == 248:
            assert proxy.id == first_proxy_id
        if i == 249:
            # Должна произойти ротация!
            assert proxy.id != first_proxy_id
            print(f"✅ Demper proxy rotated at request #{i}")

# Запустить:
python -m pytest tests/test_proxy_rotation.py -v

# 4. ✨ Тест параллельной работы модулей (КРИТИЧНО!)
async def test_concurrent_modules():
    """
    Проверить что демпер и orders не конкурируют за прокси
    """
    user_id = test_user_id

    # Одновременно запустить:
    # - демпер (650 запросов)
    # - orders (2 запроса)

    async def demper_task():
        for i in range(650):
            rotator = await get_user_proxy_rotator(user_id, module='demper')
            proxy = await rotator.get_current_proxy()
            await rotator.record_request(success=True)
            print(f"Demper using proxy: {proxy.id}")

    async def orders_task():
        for i in range(2):
            rotator = await get_user_proxy_rotator(user_id, module='orders')
            proxy = await rotator.get_current_proxy()
            await rotator.record_request(success=True)
            print(f"Orders using proxy: {proxy.id}")

    # Запустить параллельно
    start = time.time()
    await asyncio.gather(demper_task(), orders_task())
    elapsed = time.time() - start

    print(f"✅ Concurrent execution completed in {elapsed:.1f}s")
    print("✅ Демпер и orders НЕ ждали друг друга (разные прокси пулы)")

# Запустить:
python -m pytest tests/test_concurrent_modules.py -v
Phase 5: Мониторинг

# 1. Health endpoint
curl http://backend:8010/health/proxies

# Ожидаемый JSON:
{
  "proxy_pool": {
    "total": 500,
    "available": 400,
    "allocated": 100,
    "resting": 0,
    "dead": 0
  },
  "users_with_proxies": 1,
  "average_proxies_per_user": 100,
  "per_module_allocation": {
    "demper": 70,
    "orders": 25,
    "catalog": 5
  }
}

# 2. Логи прокси-ротации
railway logs -s backemd | grep "Proxy rotated"

# Ожидается:
# [Worker-0] Proxy 12345 rotated after 249 requests (resting 40min)
# [Worker-0] Switched to proxy 67890

-- 3. Проверить историю использования
SELECT
    p.host,
    u.requests_made,
    u.success_count,
    u.failure_count,
    (u.success_count::float / u.requests_made * 100) as success_rate
FROM proxy_usage_log u
JOIN proxies p ON p.id = u.proxy_id
WHERE u.started_at > NOW() - INTERVAL '1 hour'
ORDER BY u.started_at DESC
LIMIT 10;

-- Ожидается: success_rate > 90% для большинства
Phase 6: Production тест

# Запустить демпинг для юзера с 500 товарами
# Отследить:

import time

start = time.time()

# Триггер демпинга
await demper_worker.run_cycle()

elapsed = time.time() - start

print(f"✅ Демпинг 500 товаров завершен за {elapsed:.1f}s")

# Ожидается: 60-120 секунд (2 минуты)
# БЕЗ прокси было бы: 500 товаров / 8.33 req/min = 60 минут!

# Проверить, что нет 429 ошибок
railway logs -s backemd | grep "429"
# Должно быть пусто!

railway logs -s backemd | grep "Too Many Requests"
# Должно быть пусто!

-- Проверить, что цены обновились
SELECT
    COUNT(*) as products_updated,
    MAX(updated_at) as last_update
FROM products
WHERE user_id = 'TEST_USER_ID'
  AND updated_at > NOW() - INTERVAL '5 minutes';

-- Должно быть: products_updated = 500, last_update ~ NOW()

# ✨ Тест параллельной работы демпера и orders (КРИТИЧНЫЙ!)
async def test_concurrent_demper_and_orders():
    """
    Проверить что демпер (650 запросов) и orders (2 запроса)
    работают ОДНОВРЕМЕННО с разными прокси без ожидания
    """
    user_id = test_user_id

    async def run_demper():
        """Симуляция демпера - 650 запросов"""
        start = time.time()
        for i in range(650):
            await parse_product_by_sku(
                product_sku=f"test-sku-{i}",
                user_id=user_id,
                use_proxy=True
            )
        elapsed = time.time() - start
        print(f"Demper completed 650 requests in {elapsed:.1f}s")
        return elapsed

    async def run_orders():
        """Симуляция orders worker - 2 запроса"""
        start = time.time()
        await asyncio.sleep(0.5)  # Запустить немного позже
        for i in range(2):
            rotator = await get_user_proxy_rotator(user_id, module='orders')
            proxy = await rotator.get_current_proxy()
            # Симуляция запроса к orders API
            await http_client.get(
                "https://kaspi.kz/shop/api/v2/orders",
                proxies={f"{proxy.protocol}://": f"{proxy.username}:{proxy.password}@{proxy.host}:{proxy.port}"}
            )
            await rotator.record_request(success=True)
        elapsed = time.time() - start
        print(f"Orders completed 2 requests in {elapsed:.1f}s")
        return elapsed

    # Запустить параллельно
    global_start = time.time()
    demper_time, orders_time = await asyncio.gather(run_demper(), run_orders())
    global_elapsed = time.time() - global_start

    # Проверки:
    assert orders_time < 5, "Orders должен завершиться за < 5 сек (не ждал демпера!)"
    assert demper_time < 120, "Demper должен завершиться за < 2 минуты"
    assert global_elapsed < max(demper_time, orders_time) + 5, "Работали параллельно"

    print(f"""
    ✅ Тест прошел успешно!
    - Demper: {demper_time:.1f}s (650 запросов)
    - Orders: {orders_time:.1f}s (2 запроса)
    - Total: {global_elapsed:.1f}s
    - Orders НЕ ждал демпера! (разные прокси пулы)
    """)

# Запустить:
python -m pytest tests/test_production_concurrent.py -v

-- ✨ Проверить что прокси использовались из разных модулей
SELECT
    p.module,
    COUNT(DISTINCT p.id) as unique_proxies_used,
    SUM(p.requests_count) as total_requests
FROM proxies p
WHERE p.user_id = 'TEST_USER_ID'
  AND p.last_used_at > NOW() - INTERVAL '10 minutes'
GROUP BY p.module
ORDER BY p.module;

-- Ожидаемый результат:
--  module  | unique_proxies_used | total_requests
-- ---------+---------------------+----------------
--  demper  | 3                   | 650            (использовал 3 прокси)
--  orders  | 1                   | 2              (использовал 1 прокси)
--
-- ✅ Прокси НЕ пересекались между модулями!
Критические риски и митигация
🔴 КРИТИЧНЫЕ РИСКИ
Риск	Вероятность	Влияние	Митигация
Нидерландские IPv6 НЕ работают с Kaspi	60%	БЛОКЕР	Phase 0: тестирование! Если не работают → KZ residential
Kaspi детектирует прокси по fingerprint	40%	Высокое	Использовать Playwright с real browser fingerprint
Kaspi банит весь subnet прокси	30%	Высокое	Использовать residential (ротируют IP), не datacenter
Proxy6.net прокси нестабильные	50%	Среднее	Health checks, auto-replace dead proxies, резервный провайдер
🟠 ВЫСОКИЕ РИСКИ
Риск	Вероятность	Влияние	Митигация
Юридические (нарушение ToS Kaspi)	90%	Среднее	Disclaimer в UI: "На ваш риск". Не афишировать метод.
Стоимость прокси растет	40%	Среднее	Мониторинг цен, автоматические алерты при росте > 20%
Proxy pool истощается быстрее	30%	Среднее	Автоматическая закупка, алерты при < 100 available
Прокси юзера все умерли	20%	Высокое	Auto-replacement: если dead > 20% → закупить новые
🟡 СРЕДНИЕ РИСКИ
Риск	Вероятность	Влияние	Митигация
Kaspi изменит API	30%	Высокое	Версионирование, fallback
Латентность через прокси	60%	Низкое	Использовать быстрые прокси (< 300ms ping)
Memory leak в ProxyRotator	20%	Среднее	Периодический restart воркеров (каждые 24ч)
Важные замечания
1. ⚠️ Выбор провайдера прокси
Нидерландские IPv6 ($11/100 прокси):

✅ Дешево
✅ IPv6 = больше IP адресов
❌ Kaspi может блокировать не-KZ геолокацию
❌ IPv6 adoption низкий в KZ (~5-10%)
Риск: 60% что НЕ БУДУТ работать
Казахстанские Residential ($100-150/100 прокси):

✅ Казахстанская геолокация (Kaspi не заподозрит)
✅ Residential IP (выглядят как обычные юзеры)
✅ IPv4 (стандарт в KZ)
❌ Дорого (10x дороже)
Рекомендация: Использовать если NL не работают
ВАЖНО: Phase 0 тестирование ОБЯЗАТЕЛЬНО!

2. 🔒 Безопасность прокси credentials

# В БД прокси пароли хранятся в открытом виде!
# Для production нужно:

1. Шифровать в БД (AES-256)
2. Расшифровывать в памяти worker'а
3. Не логировать credentials

# app/core/security.py
def encrypt_proxy_password(password: str) -> str:
    """Encrypt proxy password before storing in DB"""
    return cipher.encrypt(password.encode()).decode()

def decrypt_proxy_password(encrypted: str) -> str:
    """Decrypt proxy password when loading from DB"""
    return cipher.decrypt(encrypted.encode()).decode()
3. 📊 Стоимость прокси на юзера
Прокси: $11 / 100 прокси / 30 дней

На одного юзера:

100 прокси × $11 / 100 = $11/месяц
