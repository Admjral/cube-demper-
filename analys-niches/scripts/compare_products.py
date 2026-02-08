"""
Сравнение данных Algatop и Kaspi для калибровки формулы
"""

import asyncio
import json
from pathlib import Path
from datetime import datetime, timedelta

import httpx

BASE_URL_ALGATOP = "https://app.algatop.kz"
BASE_URL_KASPI = "https://kaspi.kz"
CITY_CODE = "710000000"

AUTH_COOKIE = "s%3A35528_06c4dd62-9e8b-4bfb-b5a7-5036bbb75312.aQ3YzCxRlRJ5A%2BQCVZnQldgJFJ1VwKfkMS3EEDAaH2w"

DATA_DIR = Path(__file__).parent.parent / "data"

# Загружаем модель
with open(DATA_DIR / "advanced_model.json", "r", encoding="utf-8") as f:
    MODEL = json.load(f)


def estimate_sales_v1(reviews: int, price: float, category: str) -> int:
    """Текущая формула (v1)"""
    cat_coefs = MODEL.get("category_coefficients", {})
    cat_config = cat_coefs.get(category)

    if cat_config:
        review_weight = cat_config.get("review_weight", 0.1)
        price_weight = cat_config.get("price_weight", 0)
        intercept = cat_config.get("intercept", 0)
        base_coef = cat_config.get("base_coef", 0.39)

        linear_sales = reviews * review_weight + price * price_weight + intercept
        simple_sales = reviews * base_coef

        if linear_sales < simple_sales * 0.5:
            return int(max(0, simple_sales))
        return int(max(0, linear_sales))

    return int(reviews * 0.39)


def estimate_sales_v2(reviews: int, price: float, category: str, merchants: int = 1) -> int:
    """Улучшенная формула (v2) - с учётом количества продавцов"""
    cat_coefs = MODEL.get("category_coefficients", {})
    cat_config = cat_coefs.get(category)

    if cat_config:
        review_weight = cat_config.get("review_weight", 0.1)
        price_weight = cat_config.get("price_weight", 0)
        intercept = cat_config.get("intercept", 0)
        base_coef = cat_config.get("base_coef", 0.39)

        # Линейная модель
        linear_sales = reviews * review_weight + price * price_weight + intercept

        # Простая формула
        simple_sales = reviews * base_coef

        # Комбинируем обе модели с весами
        # Для дорогих товаров с малым числом отзывов линейная модель важнее
        if reviews < 50 and price > 100000:
            # Больший вес на intercept для дорогих товаров с малыми отзывами
            sales = linear_sales * 0.7 + simple_sales * 0.3
        else:
            sales = linear_sales * 0.5 + simple_sales * 0.5

        # Корректировка на количество продавцов (больше продавцов = больше продаж)
        if merchants > 1:
            sales *= (1 + 0.05 * min(merchants - 1, 10))  # +5% за каждого продавца, max +50%

        return int(max(0, sales))

    return int(reviews * 0.39)


async def get_algatop_product(product_code: str) -> dict:
    """Получение данных продукта из Algatop"""
    # Ищем в истории дневных продаж
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)

    async with httpx.AsyncClient(
        base_url=BASE_URL_ALGATOP,
        timeout=60.0,
        cookies={"auth": AUTH_COOKIE}
    ) as client:
        # Получаем статистику по дням
        response = await client.get(
            "/api/v1/niche/product/statisticLineDay",
            params={
                "code": product_code,
                "startDate": start_date.strftime("%Y%m%d"),
                "endDate": end_date.strftime("%Y%m%d")
            },
            headers={
                "Accept": "application/json",
                "Referer": "https://app.algatop.kz/niche"
            }
        )

        if response.status_code == 200:
            data = response.json()
            if data.get("success"):
                stats = data.get("data", [])
                # data - это список дней напрямую
                if isinstance(stats, list):
                    days = stats
                else:
                    days = stats.get("days", []) if isinstance(stats, dict) else []

                total_sales = sum(d.get("sale_qty", 0) for d in days)
                return {
                    "code": product_code,
                    "total_sales_30d": total_sales,
                    "days_count": len(days),
                    "avg_daily_sales": total_sales / len(days) if days else 0,
                    "days": days[:5]  # Первые 5 дней для примера
                }

        return {"error": f"Status {response.status_code}"}


async def get_kaspi_product(product_id: str) -> dict:
    """Получение данных продукта из Kaspi"""
    async with httpx.AsyncClient(
        base_url=BASE_URL_KASPI,
        timeout=60.0,
        follow_redirects=True
    ) as client:
        headers = {
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
            "x-ks-city": CITY_CODE
        }

        # Получаем офферы (продавцы)
        offers_resp = await client.post(
            f"/yml/offer-view/offers/{product_id}",
            json={"cityId": CITY_CODE, "limit": 50, "page": 0, "sort": True},
            headers=headers
        )

        offers = []
        if offers_resp.status_code == 200:
            offers = offers_resp.json().get("offers", [])

        # Получаем отзывы
        reviews_resp = await client.get(
            f"/yml/review-view/api/v1/reviews/product/{product_id}",
            params={"filter": "COMMENT", "sort": "POPULARITY", "limit": 1, "withAgg": "true"},
            headers=headers
        )

        review_count = 0
        avg_rating = 0
        if reviews_resp.status_code == 200:
            reviews_data = reviews_resp.json()
            review_count = reviews_data.get("total", 0)
            avg_rating = reviews_data.get("averageRating", 0)

        prices = [o.get("price", 0) for o in offers if o.get("price")]

        return {
            "product_id": product_id,
            "review_count": review_count,
            "avg_rating": avg_rating,
            "merchant_count": len(offers),
            "min_price": min(prices) if prices else 0,
            "avg_price": sum(prices) / len(prices) if prices else 0
        }


async def analyze_products():
    """Анализ тестовых товаров"""
    print("=" * 70)
    print("СРАВНЕНИЕ ALGATOP И KASPI ДЛЯ КАЛИБРОВКИ ФОРМУЛЫ")
    print("=" * 70)

    # Тестовые товары
    products = [
        {
            "name": "Midea MO11000GB (духовка)",
            "kaspi_id": "121355200",
            "algatop_code": "121355200",
            "category": "Бытовая техника"
        },
        {
            "name": "Hansa ACS 09W55 (кондиционер монт.комплект)",
            "kaspi_id": "137334570",
            "algatop_code": "137334570",
            "category": "Бытовая техника"
        },
        {
            "name": "Кресло-качалка 875947",
            "kaspi_id": "115625025",
            "algatop_code": "115625025",
            "category": "Мебель"
        }
    ]

    results = []

    for p in products:
        print(f"\n{'─' * 70}")
        print(f"📦 {p['name']}")
        print(f"   Kaspi ID: {p['kaspi_id']}, Категория: {p['category']}")

        # Получаем данные
        kaspi_data = await get_kaspi_product(p["kaspi_id"])
        algatop_data = await get_algatop_product(p["algatop_code"])

        print(f"\n   📊 Данные Kaspi:")
        print(f"      Отзывы: {kaspi_data.get('review_count', 'N/A')}")
        print(f"      Рейтинг: {kaspi_data.get('avg_rating', 'N/A')}")
        print(f"      Продавцов: {kaspi_data.get('merchant_count', 'N/A')}")
        print(f"      Цена (avg): {kaspi_data.get('avg_price', 0):,.0f}₸")

        print(f"\n   📈 Данные Algatop (30 дней):")
        if "error" not in algatop_data:
            print(f"      Продажи за 30д: {algatop_data.get('total_sales_30d', 'N/A')}")
            print(f"      Avg в день: {algatop_data.get('avg_daily_sales', 0):.1f}")
        else:
            print(f"      {algatop_data.get('error')}")

        # Расчёт по нашим формулам
        reviews = kaspi_data.get("review_count", 0)
        price = kaspi_data.get("avg_price", 0)
        merchants = kaspi_data.get("merchant_count", 1)

        v1_estimate = estimate_sales_v1(reviews, price, p["category"])
        v2_estimate = estimate_sales_v2(reviews, price, p["category"], merchants)

        algatop_sales = algatop_data.get("total_sales_30d", 0) if "error" not in algatop_data else 0

        print(f"\n   🔮 Расчёт продаж (30 дней):")
        print(f"      Формула V1: {v1_estimate}")
        print(f"      Формула V2: {v2_estimate}")
        print(f"      Algatop:    {algatop_sales}")

        if algatop_sales > 0:
            error_v1 = abs(v1_estimate - algatop_sales) / algatop_sales * 100
            error_v2 = abs(v2_estimate - algatop_sales) / algatop_sales * 100
            print(f"\n   ⚠️  Ошибка V1: {error_v1:.1f}%")
            print(f"   ⚠️  Ошибка V2: {error_v2:.1f}%")

            results.append({
                "name": p["name"],
                "reviews": reviews,
                "price": price,
                "merchants": merchants,
                "algatop": algatop_sales,
                "v1": v1_estimate,
                "v2": v2_estimate,
                "error_v1": error_v1,
                "error_v2": error_v2
            })

    # Итоговая статистика
    if results:
        print(f"\n{'=' * 70}")
        print("ИТОГОВАЯ СТАТИСТИКА")
        print("=" * 70)

        avg_error_v1 = sum(r["error_v1"] for r in results) / len(results)
        avg_error_v2 = sum(r["error_v2"] for r in results) / len(results)

        print(f"\nСредняя ошибка V1: {avg_error_v1:.1f}%")
        print(f"Средняя ошибка V2: {avg_error_v2:.1f}%")

        # Сохраняем результаты
        with open(DATA_DIR / "formula_comparison.json", "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        print(f"\nРезультаты сохранены в: {DATA_DIR / 'formula_comparison.json'}")

    print("\n" + "=" * 70)


if __name__ == "__main__":
    asyncio.run(analyze_products())
