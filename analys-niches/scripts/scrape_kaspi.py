"""
Парсер Kaspi.kz через JSON API

API Endpoints:
- /yml/main-navigation/n/n/desktop-menu - категории
- /yml/product-view/pl/results - список товаров в категории
- /yml/offer-view/offers/{id} - офферы товара (продавцы, цены)
- /yml/review-view/api/v1/reviews/product/{id} - отзывы
"""

import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict

import httpx

BASE_URL = "https://kaspi.kz"
CITY_CODE = "710000000"  # Алматы

DATA_DIR = Path(__file__).parent.parent / "data" / "kaspi"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Загружаем коэффициенты
COEFFICIENTS_FILE = Path(__file__).parent.parent / "data" / "sales_coefficients.json"

# Маппинг категорий Kaspi -> русские названия для коэффициентов
CATEGORY_MAPPING = {
    "Smartphones and gadgets": "Телефоны и гаджеты",
    "Home equipment": "Бытовая техника",
    "TV_Audio": "ТВ, Аудио, Видео",
    "Computers": "Компьютеры",
    "Furniture": "Мебель",
    "Beauty care": "Красота и здоровье",
    "Child goods": "Детские товары",
    "Pharmacy": "Аптека",
    "Construction and repair": "Строительство, ремонт",
    "Sports and outdoors": "Спорт, туризм",
    "Leisure": "Досуг, книги",
    "Car goods": "Автотовары",
    "Jewelry and Bijouterie": "Украшения",
    "Fashion accessories": "Аксессуары",
    "Fashion": "Одежда",
    "Shoes": "Обувь",
    "Home": "Товары для дома и дачи",
    "Gifts and party supplies": "Подарки, товары для праздников",
    "Office and school supplies": "Канцелярские товары",
    "Pet goods": "Товары для животных",
}


class KaspiAPI:
    def __init__(self):
        self.client = httpx.AsyncClient(
            base_url=BASE_URL,
            timeout=60.0,
            follow_redirects=True
        )
        self.categories = []
        self.products = []
        self.coefficients = self._load_coefficients()

    def _load_coefficients(self) -> dict:
        """Загрузка ансамблевой модели для расчёта продаж"""
        # Сначала пробуем ансамблевую модель (v5.0)
        ensemble_file = COEFFICIENTS_FILE.parent / "ensemble_model.json"
        if ensemble_file.exists():
            with open(ensemble_file, 'r', encoding='utf-8') as f:
                return json.load(f)

        # Fallback на log-linear модель (v3.x)
        log_linear_file = COEFFICIENTS_FILE.parent / "log_linear_model.json"
        if log_linear_file.exists():
            with open(log_linear_file, 'r', encoding='utf-8') as f:
                return json.load(f)

        # Fallback на advanced модель
        advanced_file = COEFFICIENTS_FILE.parent / "advanced_model.json"
        if advanced_file.exists():
            with open(advanced_file, 'r', encoding='utf-8') as f:
                return json.load(f)

        # Fallback на простые коэффициенты
        if COEFFICIENTS_FILE.exists():
            with open(COEFFICIENTS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)

        return {"global_coefficient": 0.98, "category_coefficients": {}}

    def _get_headers(self) -> dict:
        return {
            "Accept": "application/json, text/*",
            "Accept-Language": "ru-RU,ru;q=0.9",
            "Content-Type": "application/json; charset=UTF-8",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "x-ks-city": CITY_CODE,
            "x-description-enabled": "true",
            "Referer": "https://kaspi.kz/shop/"
        }

    async def get_categories(self, depth: int = 1) -> list:
        """Получение категорий верхнего уровня"""
        print("Получение категорий Kaspi...")

        response = await self.client.get(
            "/yml/main-navigation/n/n/desktop-menu",
            params={
                "depth": depth,
                "city": CITY_CODE,
                "rootType": "desktop"
            },
            headers=self._get_headers()
        )

        if response.status_code == 200:
            data = response.json()
            # Извлекаем категории из subNodes
            sub_nodes = data.get("subNodes", [])
            self.categories = []

            for node in sub_nodes:
                cat = {
                    "code": node.get("code", ""),
                    "title": node.get("title", "").replace("<br/>", " ").strip(),
                    "link": node.get("link", ""),
                    "popularity": node.get("popularity", 0),
                }
                self.categories.append(cat)

            # Сортируем по популярности
            self.categories.sort(key=lambda x: x["popularity"], reverse=True)

            print(f"Найдено {len(self.categories)} категорий")
            return self.categories
        else:
            print(f"Ошибка: {response.status_code}")
            return []

    async def get_products_in_category(
        self,
        category_code: str,
        page: int = 0,
        size: int = 48
    ) -> List[Dict]:
        """Получение товаров из категории"""

        # API: /yml/product-view/pl/results?q=:category:CategoryCode
        query = f":category:{category_code}"

        response = await self.client.get(
            "/yml/product-view/pl/results",
            params={
                "q": query,
                "page": page,
                "size": size,
                "sort": "relevance"
            },
            headers=self._get_headers()
        )

        if response.status_code == 200:
            data = response.json()
            return data.get("data", [])
        return []

    async def get_all_products_in_category(
        self,
        category_code: str,
        category_name: str,
        max_pages: int = 50,
        size: int = 48
    ) -> List[Dict]:
        """Получение всех товаров из категории (все страницы)"""
        all_products = []
        page = 0

        while page < max_pages:
            products = await self.get_products_in_category(
                category_code=category_code,
                page=page,
                size=size
            )

            if not products:
                break

            # Добавляем данные о категории и расчёт продаж
            for p in products:
                p["_category_code"] = category_code
                p["_category_name"] = category_name

                # Расчёт примерных продаж по формуле Algatop
                review_count = p.get("reviewsQuantity", 0)
                price = p.get("unitPrice", 0)
                rating = p.get("rating", 4.5)

                estimated_sales = self.estimate_sales(
                    review_count=review_count,
                    category_name=category_name,
                    price=price,
                    rating=rating
                )
                p["_estimated_monthly_sales"] = estimated_sales
                p["_estimated_monthly_revenue"] = estimated_sales * price

            all_products.extend(products)
            print(f"    Страница {page}: +{len(products)} товаров (всего: {len(all_products)})")

            if len(products) < size:
                break

            page += 1
            await asyncio.sleep(0.3)

        return all_products

    async def search_products(self, query: str, page: int = 0, size: int = 48) -> List[Dict]:
        """Поиск товаров по запросу"""

        response = await self.client.get(
            "/yml/product-view/pl/results",
            params={
                "q": query,
                "page": page,
                "size": size,
                "sort": "relevance"
            },
            headers=self._get_headers()
        )

        if response.status_code == 200:
            data = response.json()
            return data.get("data", [])
        return []

    async def get_product_offers(self, product_id: str) -> dict:
        """Получение офферов товара (продавцы и цены)"""

        response = await self.client.post(
            f"/yml/offer-view/offers/{product_id}",
            json={
                "cityId": CITY_CODE,
                "limit": 50,
                "page": 0,
                "sort": True
            },
            headers=self._get_headers()
        )

        if response.status_code == 200:
            return response.json()
        return {}

    async def get_product_reviews(
        self,
        product_id: str,
        limit: int = 100
    ) -> dict:
        """Получение отзывов товара"""

        response = await self.client.get(
            f"/yml/review-view/api/v1/reviews/product/{product_id}",
            params={
                "filter": "COMMENT",
                "sort": "POPULARITY",
                "limit": limit,
                "withAgg": "true"
            },
            headers=self._get_headers()
        )

        if response.status_code == 200:
            return response.json()
        return {}

    def estimate_sales(
        self,
        review_count: int,
        category_name: str,
        price: float = 0,
        merchant_count: int = 1,
        rating: float = 4.5
    ) -> int:
        """
        Расчёт примерных продаж по ансамблевой модели

        Формула: sales = w_log × log_linear + w_ratio × ratio_model

        Args:
            review_count: Количество отзывов
            category_name: Название категории на русском (Algatop формат)
            price: Цена товара в тенге
            merchant_count: Количество продавцов
            rating: Рейтинг товара

        Returns:
            Расчётное количество продаж в месяц
        """
        import math

        cat_coefs = self.coefficients.get("category_coefficients", {})
        model_version = self.coefficients.get("version", "1.0")

        # Ищем точное совпадение категории или через маппинг
        algatop_category = CATEGORY_MAPPING.get(category_name, category_name)

        # Ищем конфиг категории
        cat_config = None
        for cat, config in cat_coefs.items():
            if cat == algatop_category:
                cat_config = config
                break
            # Частичное совпадение
            if cat.lower() in algatop_category.lower() or algatop_category.lower() in cat.lower():
                cat_config = config
                break

        if cat_config and isinstance(cat_config, dict):
            # Ансамблевая модель (v5.0)
            if model_version >= "5.0" and "log_weight" in cat_config:
                log_weight = cat_config.get("log_weight", 0.8)
                ratio_weight = cat_config.get("ratio_weight", 0.2)

                # 1. Log-linear предсказание
                review_coef = cat_config.get("review_coef", 0.2)
                price_coef = cat_config.get("price_coef", -0.7)
                intercept = cat_config.get("intercept", 10.0)

                log_reviews = math.log1p(max(review_count, 1))
                log_price = math.log1p(max(price, 1000))

                log_sales = review_coef * log_reviews + price_coef * log_price + intercept
                pred_log = max(0, math.expm1(log_sales))

                # 2. Ratio предсказание
                ratio = cat_config.get("ratio_p50", 0.3)
                price_median = cat_config.get("ratio_price_median", 100000)
                price_factor = cat_config.get("ratio_price_factor", 0.8)

                if price >= price_median:
                    ratio *= price_factor

                pred_ratio = review_count * ratio

                # 3. Ансамбль
                sales = log_weight * pred_log + ratio_weight * pred_ratio

                return int(max(0, sales))

            # Log-linear модель (v3.x)
            elif model_version >= "3.0" and "review_coef" in cat_config:
                review_coef = cat_config.get("review_coef", 0.2)
                price_coef = cat_config.get("price_coef", -0.7)
                intercept = cat_config.get("intercept", 5.0)

                log_reviews = math.log1p(max(review_count, 1))
                log_price = math.log1p(max(price, 1000))

                log_sales = review_coef * log_reviews + price_coef * log_price + intercept
                sales = math.expm1(log_sales)

                return int(max(0, sales))

            else:
                # Старая модель (v2.x) с линейной формулой
                review_weight = cat_config.get("review_weight", 0.1)
                price_weight = cat_config.get("price_weight", 0)
                intercept = cat_config.get("intercept", 0)
                base_coef = cat_config.get("base_coef", 0.39)

                linear_sales = review_count * review_weight + price * price_weight + intercept
                simple_sales = review_count * base_coef

                if linear_sales < simple_sales * 0.5:
                    sales = simple_sales
                else:
                    sales = linear_sales

                return int(max(0, sales))
        else:
            # Fallback на простой коэффициент
            default_coef = self.coefficients.get("default_coefficient", 0.33)
            return int(max(0, review_count * default_coef))

    async def analyze_product(self, product_id: str, product_name: str = "", category: str = "") -> dict:
        """Полный анализ товара"""
        print(f"  📦 Анализ: {product_name[:50]}...")

        # Получаем офферы
        offers_data = await self.get_product_offers(product_id)

        # Получаем отзывы
        reviews_data = await self.get_product_reviews(product_id)

        # Извлекаем данные
        offers = offers_data.get("offers", [])
        review_count = reviews_data.get("total", 0)
        avg_rating = reviews_data.get("averageRating", 0)

        # Цены от продавцов
        prices = [o.get("price", 0) for o in offers if o.get("price")]
        min_price = min(prices) if prices else 0
        max_price = max(prices) if prices else 0
        avg_price = sum(prices) / len(prices) if prices else 0

        # Расчёт продаж по формуле Algatop
        estimated_sales = self.estimate_sales(
            review_count=review_count,
            category_name=category,
            price=avg_price,
            merchant_count=len(offers),
            rating=avg_rating
        )
        estimated_revenue = estimated_sales * avg_price

        return {
            "product_id": product_id,
            "product_name": product_name,
            "category": category,
            "merchant_count": len(offers),
            "min_price": min_price,
            "max_price": max_price,
            "avg_price": round(avg_price, 2),
            "review_count": review_count,
            "avg_rating": avg_rating,
            "estimated_monthly_sales": estimated_sales,
            "estimated_monthly_revenue": round(estimated_revenue, 2)
        }

    async def scrape_all_categories(
        self,
        max_categories: int = None,
        max_pages_per_category: int = 50,
        products_per_page: int = 48
    ):
        """Полный скрейпинг всех категорий"""
        print("\n" + "="*60)
        print("ПОЛНЫЙ СКРЕЙПИНГ KASPI.KZ")
        print("="*60)

        # Получаем категории
        await self.get_categories()

        if not self.categories:
            print("Нет категорий для скрейпинга")
            return

        categories = self.categories[:max_categories] if max_categories else self.categories
        all_products = []

        for i, cat in enumerate(categories, 1):
            cat_code = cat.get("code", "")
            cat_name = cat.get("title", "").replace("<br/>", " ").strip()

            print(f"\n[{i}/{len(categories)}] {cat_name} ({cat_code})")

            products = await self.get_all_products_in_category(
                category_code=cat_code,
                category_name=cat_name,
                max_pages=max_pages_per_category,
                size=products_per_page
            )

            all_products.extend(products)
            print(f"  Собрано: {len(products)} товаров")

            # Промежуточное сохранение
            if i % 5 == 0:
                self._save_checkpoint(all_products, f"kaspi_checkpoint_{i}.json")

            await asyncio.sleep(0.5)

        self.products = all_products

        # Финальное сохранение
        self._save_results()

        print(f"\n" + "="*60)
        print(f"СКРЕЙПИНГ ЗАВЕРШЁН")
        print(f"  Категорий: {len(categories)}")
        print(f"  Товаров: {len(all_products)}")
        print("="*60)

        return all_products

    def _save_checkpoint(self, products: List[Dict], filename: str):
        """Промежуточное сохранение"""
        filepath = DATA_DIR / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump({
                "scraped_at": datetime.now().isoformat(),
                "total": len(products),
                "products": products
            }, f, ensure_ascii=False, indent=2)
        print(f"  Checkpoint: {filepath}")

    def _save_results(self):
        """Сохранение финальных результатов"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # JSON
        json_file = DATA_DIR / f"kaspi_products_{timestamp}.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump({
                "scraped_at": datetime.now().isoformat(),
                "total_categories": len(self.categories),
                "total_products": len(self.products),
                "categories": self.categories,
                "products": self.products
            }, f, ensure_ascii=False, indent=2)
        print(f"JSON: {json_file}")

        # CSV
        import csv
        csv_file = DATA_DIR / f"kaspi_products_{timestamp}.csv"
        if self.products:
            # Выбираем ключевые поля
            fields = [
                "id", "title", "brand", "unitPrice", "rating",
                "reviewsQuantity", "_category_name", "_estimated_monthly_sales",
                "_estimated_monthly_revenue"
            ]
            with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore')
                writer.writeheader()
                writer.writerows(self.products)
            print(f"CSV: {csv_file}")

    async def test_api(self):
        """Тестирование API"""
        print("="*60)
        print("ТЕСТИРОВАНИЕ KASPI API")
        print("="*60)

        # 1. Категории
        print("\n1️⃣ Тест категорий...")
        response = await self.client.get(
            "/yml/main-navigation/n/n/desktop-menu",
            params={"depth": 1, "city": CITY_CODE, "rootType": "desktop"},
            headers=self._get_headers()
        )
        print(f"   Статус: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"   Ответ: {json.dumps(data, ensure_ascii=False)[:500]}...")

            # Сохраняем для анализа
            with open(DATA_DIR / "categories_raw.json", 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

        # 2. Тест товара (iPhone 17 Pro из примера)
        print("\n2️⃣ Тест товара (iPhone 17 Pro)...")
        product_id = "145467625"

        # Офферы
        offers = await self.get_product_offers(product_id)
        print(f"   Офферы: {len(offers.get('offers', []))} продавцов")

        # Отзывы
        reviews = await self.get_product_reviews(product_id)
        print(f"   Отзывы: {reviews.get('total', 0)}")
        print(f"   Рейтинг: {reviews.get('averageRating', 0)}")

        # Сохраняем
        with open(DATA_DIR / "sample_product.json", 'w', encoding='utf-8') as f:
            json.dump({
                "product_id": product_id,
                "offers": offers,
                "reviews": reviews
            }, f, ensure_ascii=False, indent=2)

        # 3. Анализ с расчётом продаж
        print("\n3️⃣ Анализ с расчётом продаж...")
        analysis = await self.analyze_product(
            product_id,
            "Apple iPhone 17 Pro 256GB",
            "Телефоны и гаджеты"
        )
        print(f"   Результат: {json.dumps(analysis, ensure_ascii=False, indent=2)}")

        # Сохраняем
        with open(DATA_DIR / "sample_analysis.json", 'w', encoding='utf-8') as f:
            json.dump(analysis, f, ensure_ascii=False, indent=2)

        print("\n" + "="*60)
        print("✅ ТЕСТИРОВАНИЕ ЗАВЕРШЕНО")
        print(f"   Файлы сохранены в: {DATA_DIR}")
        print("="*60)

    async def close(self):
        await self.client.aclose()


async def test_formula():
    """Тест ансамблевой формулы на реальных данных Algatop"""
    print("="*60)
    print("ТЕСТ АНСАМБЛЕВОЙ МОДЕЛИ (v5.0)")
    print("="*60)

    api = KaspiAPI()

    # Реальные данные из Algatop (10500 товаров)
    test_cases = [
        {
            "name": "Midea MO11000GB (духовка)",
            "reviews": 391,  # реальные данные из Algatop
            "price": 102290,
            "category": "Бытовая техника",
            "algatop_sales": 75
        },
        {
            "name": "iPhone 17 Pro 256GB",
            "reviews": 100,
            "price": 850000,
            "category": "Телефоны и гаджеты",
            "algatop_sales": None
        },
        {
            "name": "Детская коляска Anex",
            "reviews": 198,  # реальные данные из Algatop
            "price": 205900,
            "category": "Детские товары",
            "algatop_sales": 14
        },
        {
            "name": "Шкаф-купе Модерн",
            "reviews": 318,  # реальные данные
            "price": 138993,
            "category": "Мебель",
            "algatop_sales": 81
        },
        {
            "name": "Крем DR.PLINUS Z Cure",
            "reviews": 423,  # реальные данные
            "price": 12499,
            "category": "Красота и здоровье",
            "algatop_sales": 1106
        }
    ]

    print(f"\nВерсия модели: {api.coefficients.get('version', 'N/A')}")
    print(f"Метод: {api.coefficients.get('method', 'N/A')}")

    print("\nКоэффициенты модели:")
    cat_coefs = api.coefficients.get("category_coefficients", {})
    for cat in ["Бытовая техника", "Мебель", "Красота и здоровье"]:
        config = cat_coefs.get(cat, {})
        print(f"  {cat}:")
        print(f"    log_weight: {config.get('log_weight', 'N/A')}")
        print(f"    ratio_p50: {config.get('ratio_p50', 'N/A')}")
        print(f"    median_error: {config.get('median_error', 'N/A')}%")

    print("\nРезультаты расчёта:")
    print("-"*60)

    errors = []
    for case in test_cases:
        estimated = api.estimate_sales(
            review_count=case["reviews"],
            category_name=case["category"],
            price=case["price"]
        )

        print(f"\n{case['name']}:")
        print(f"  Отзывы: {case['reviews']}, Цена: {case['price']:,}₸")
        print(f"  Категория: {case['category']}")
        print(f"  Расчётные продажи: {estimated}")

        if case["algatop_sales"]:
            error = abs(estimated - case["algatop_sales"]) / case["algatop_sales"] * 100
            errors.append(error)
            status = "✅" if error < 50 else "⚠️" if error < 100 else "❌"
            print(f"  Algatop продажи: {case['algatop_sales']}")
            print(f"  {status} Ошибка: {error:.1f}%")

    if errors:
        avg_error = sum(errors) / len(errors)
        print(f"\n{'='*60}")
        print(f"СРЕДНЯЯ ОШИБКА: {avg_error:.1f}%")
        print(f"ТОЧНОСТЬ: {100 - avg_error:.1f}%")
        print(f"{'='*60}")

    await api.close()


async def main():
    import sys

    api = KaspiAPI()

    try:
        if len(sys.argv) > 1 and sys.argv[1] == "formula":
            await api.close()
            await test_formula()
            return
        elif len(sys.argv) > 1 and sys.argv[1] == "test":
            await api.test_api()
        elif len(sys.argv) > 1 and sys.argv[1] == "full":
            # Полный скрейпинг
            await api.scrape_all_categories(
                max_categories=None,  # Все категории
                max_pages_per_category=25,  # ~1200 товаров на категорию
                products_per_page=48
            )
        else:
            # По умолчанию - тест одной категории
            print("Тест скрейпинга категории Smartphones...")
            await api.get_categories()

            products = await api.get_all_products_in_category(
                category_code="Smartphones",
                category_name="Телефоны и гаджеты",
                max_pages=3  # ~150 товаров для теста
            )

            print(f"\nСобрано {len(products)} товаров")

            # Показываем топ-5 по продажам
            top = sorted(products, key=lambda x: x.get("_estimated_monthly_sales", 0), reverse=True)[:5]
            print("\nТоп-5 по расчётным продажам:")
            for p in top:
                print(f"  {p['title'][:50]}: {p['_estimated_monthly_sales']} шт/мес, {p['reviewsQuantity']} отзывов")

    finally:
        await api.close()


if __name__ == "__main__":
    asyncio.run(main())
