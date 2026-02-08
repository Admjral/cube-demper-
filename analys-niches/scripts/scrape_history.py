"""
Скрейпинг исторических данных продаж по дням
API: /api/v1/niche/product/statisticLineDay

Собирает данные за 2025 год для анализа сезонности
"""

import asyncio
import json
from datetime import datetime
from pathlib import Path

import httpx

BASE_URL = "https://app.algatop.kz"
AUTH_COOKIE = "s%3A35528_06c4dd62-9e8b-4bfb-b5a7-5036bbb75312.aQ3YzCxRlRJ5A%2BQCVZnQldgJFJ1VwKfkMS3EEDAaH2w"

DATA_DIR = Path(__file__).parent.parent / "data"


def get_headers():
    return {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://app.algatop.kz/niche",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
    }


async def get_available_periods(client: httpx.AsyncClient) -> dict:
    """Получение доступных периодов"""
    print("📅 Получение доступных периодов...")

    response = await client.get(
        "/api/v1/niche/period",
        headers=get_headers()
    )

    if response.status_code == 200:
        data = response.json()
        print(f"✅ Периоды получены: {json.dumps(data, indent=2, ensure_ascii=False)[:500]}")
        return data
    else:
        print(f"❌ Ошибка: {response.status_code}")
        return {}


async def get_product_daily_stats(
    client: httpx.AsyncClient,
    product_code: str,
    start_date: str = "20250101",
    end_date: str = "20251231"
) -> list:
    """Получение дневной статистики продаж товара"""

    response = await client.get(
        "/api/v1/niche/product/statisticLineDay",
        params={
            "code": product_code,
            "startDate": start_date,
            "endDate": end_date
        },
        headers=get_headers()
    )

    if response.status_code == 200:
        data = response.json()
        if isinstance(data, dict) and data.get("success"):
            return data.get("data", [])
        return data if isinstance(data, list) else []
    return []


async def scrape_product_history(max_products: int = 100):
    """Скрейпинг истории продаж для топ товаров"""

    print("="*60)
    print("СКРЕЙПИНГ ИСТОРИЧЕСКИХ ДАННЫХ ПРОДАЖ")
    print("="*60)

    # Загружаем список товаров
    products_file = sorted(DATA_DIR.glob("algatop_products_*.json"))[-1]
    print(f"📂 Загружаю товары: {products_file.name}")

    with open(products_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    products = data.get("products", [])
    print(f"✅ Всего товаров: {len(products)}")

    # Берём топ товаров по продажам для анализа
    products_sorted = sorted(products, key=lambda x: x.get("sale_qty", 0), reverse=True)
    top_products = products_sorted[:max_products]

    print(f"📊 Анализируем топ-{len(top_products)} товаров по продажам")

    async with httpx.AsyncClient(
        base_url=BASE_URL,
        timeout=60.0,
        cookies={"auth": AUTH_COOKIE}
    ) as client:

        # Сначала получим доступные периоды
        periods = await get_available_periods(client)

        # Собираем историю для каждого товара
        all_history = []

        for i, product in enumerate(top_products, 1):
            product_code = product.get("product_code")
            product_name = product.get("product_name", "")[:50]
            category = product.get("_category_name", "")

            print(f"\n[{i}/{len(top_products)}] {product_name}...")

            # Получаем данные за 2025 год
            history = await get_product_daily_stats(
                client,
                product_code,
                start_date="20250101",
                end_date="20251231"
            )

            if history:
                all_history.append({
                    "product_code": product_code,
                    "product_name": product.get("product_name"),
                    "category": category,
                    "sale_qty_month": product.get("sale_qty"),
                    "review_qty": product.get("review_qty"),
                    "daily_stats": history
                })
                print(f"  ✅ Получено {len(history)} дней данных")
            else:
                print(f"  ⚠️ Нет данных")

            await asyncio.sleep(0.3)

            # Промежуточное сохранение
            if i % 20 == 0:
                save_history(all_history, f"history_checkpoint_{i}.json")

    # Финальное сохранение
    save_history(all_history, "product_daily_history.json")

    print(f"\n" + "="*60)
    print(f"✅ ЗАВЕРШЕНО: собрана история для {len(all_history)} товаров")
    print("="*60)

    return all_history


def save_history(history: list, filename: str):
    """Сохранение истории в JSON"""
    filepath = DATA_DIR / filename

    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump({
            "scraped_at": datetime.now().isoformat(),
            "total_products": len(history),
            "products": history
        }, f, ensure_ascii=False, indent=2)

    print(f"💾 Сохранено: {filepath}")


async def analyze_sample():
    """Анализ примера данных"""
    print("="*60)
    print("АНАЛИЗ ПРИМЕРА ДНЕВНЫХ ДАННЫХ")
    print("="*60)

    async with httpx.AsyncClient(
        base_url=BASE_URL,
        timeout=60.0,
        cookies={"auth": AUTH_COOKIE}
    ) as client:

        # Получаем периоды
        periods = await get_available_periods(client)

        # Пример товара из запроса пользователя
        sample_code = "124522298"
        print(f"\n📦 Тестируем товар: {sample_code}")

        history = await get_product_daily_stats(
            client,
            sample_code,
            start_date="20250101",
            end_date="20250228"
        )

        if history:
            print(f"\n✅ Получено {len(history)} записей")
            print("\nПример данных (первые 5):")
            for item in history[:5]:
                print(f"  {json.dumps(item, ensure_ascii=False)}")

            # Сохраняем пример
            with open(DATA_DIR / "sample_daily_stats.json", 'w', encoding='utf-8') as f:
                json.dump(history, f, ensure_ascii=False, indent=2)
            print(f"\n💾 Пример сохранён: sample_daily_stats.json")
        else:
            print("❌ Данные не получены")


async def main():
    # Сначала анализируем пример
    await analyze_sample()

    # Затем собираем историю для топ-100 товаров
    # await scrape_product_history(max_products=100)


if __name__ == "__main__":
    asyncio.run(main())
