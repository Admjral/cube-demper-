"""
Скрейпер данных аналитики ниш через HTTP API

API Endpoints:
- /api/v1/niche/categoryListStatistic - список категорий
- /api/v1/niche/product - товары в категории
- /api/v1/niche/sublingsCategory - подкатегории
"""

import asyncio
import csv
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlencode, quote

import httpx
from dotenv import load_dotenv

# Загрузка credentials
load_dotenv(Path(__file__).parent.parent / ".env")

BASE_URL = "https://app.algatop.kz"

# Auth cookie из браузера
AUTH_COOKIE = "s%3A35528_06c4dd62-9e8b-4bfb-b5a7-5036bbb75312.aQ3YzCxRlRJ5A%2BQCVZnQldgJFJ1VwKfkMS3EEDAaH2w"

# Путь для сохранения данных
DATA_DIR = Path(__file__).parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)


class NicheDataAPI:
    def __init__(self):
        self.client = httpx.AsyncClient(
            base_url=BASE_URL,
            timeout=120.0,
            follow_redirects=True,
            cookies={"auth": AUTH_COOKIE}
        )
        self.categories = []
        self.products = []

    def _get_headers(self) -> dict:
        return {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
            "Referer": "https://app.algatop.kz/niche",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
        }

    async def get_categories(self, start_date: str = None, end_date: str = None) -> list:
        """Получение списка категорий с статистикой"""
        print("📂 Получение категорий...")

        if not start_date:
            end = datetime.now()
            start = end - timedelta(days=30)
            start_date = start.strftime("%Y%m%d")
            end_date = end.strftime("%Y%m%d")

        params = {
            "startDate": start_date,
            "endDate": end_date
        }

        response = await self.client.get(
            "/api/v1/niche/categoryListStatistic",
            params=params,
            headers=self._get_headers()
        )

        if response.status_code == 200:
            data = response.json()
            self.categories = data if isinstance(data, list) else data.get("data", data.get("categories", []))
            print(f"✅ Найдено {len(self.categories)} категорий")
            return self.categories
        else:
            print(f"❌ Ошибка: {response.status_code}")
            return []

    async def get_subcategories(self, category_code: str) -> list:
        """Получение подкатегорий"""
        response = await self.client.get(
            "/api/v1/niche/sublingsCategory",
            params={"categoryCode": category_code},
            headers=self._get_headers()
        )

        if response.status_code == 200:
            data = response.json()
            return data if isinstance(data, list) else data.get("data", [])
        return []

    async def get_products(
        self,
        category_id: str,
        start_date: str = None,
        end_date: str = None,
        page: int = 1,
        sort_type: str = "revenue",
        sort_direction: str = "desc"
    ) -> dict:
        """Получение товаров в категории"""

        if not start_date:
            end = datetime.now()
            start = end - timedelta(days=30)
            start_date = start.strftime("%Y%m%d")
            end_date = end.strftime("%Y%m%d")

        sort_json = json.dumps({
            "type": sort_type,
            "direction": sort_direction,
            "typeName": "По выручке" if sort_type == "revenue" else "По продажам"
        })

        params = {
            "startDate": start_date,
            "endDate": end_date,
            "page": page,
            "filter": "{}",
            "categoryId": category_id,
            "sort": sort_json,
            "categoryType": ""
        }

        response = await self.client.get(
            "/api/v1/niche/product",
            params=params,
            headers=self._get_headers()
        )

        if response.status_code == 200:
            data = response.json()
            # Извлекаем products.lines из структуры ответа
            if isinstance(data, dict) and data.get("success"):
                products_data = data.get("data", {})
                if isinstance(products_data, dict):
                    products = products_data.get("products", {})
                    if isinstance(products, dict):
                        lines = products.get("lines", [])
                        # Добавляем метаданные
                        total_pages = products.get("totalPages", 1)
                        return {"products": lines, "totalPages": total_pages}
            return data
        else:
            print(f"  ⚠️ Ошибка {response.status_code}: {response.text[:200]}")
            return {}

    async def get_all_products_in_category(
        self,
        category_id: str,
        category_name: str,
        max_pages: int = 100,
        start_date: str = None,
        end_date: str = None
    ) -> list:
        """Получение всех товаров из категории (все страницы)"""
        all_products = []
        page = 1

        while page <= max_pages:
            data = await self.get_products(
                category_id=category_id,
                start_date=start_date,
                end_date=end_date,
                page=page
            )

            products = data.get("products", [])
            if not products:
                break

            # Добавляем инфо о категории
            for p in products:
                p["_category_id"] = category_id
                p["_category_name"] = category_name

            all_products.extend(products)
            print(f"    Страница {page}: +{len(products)} товаров (всего: {len(all_products)})")

            # Если получили меньше 20 товаров - это последняя страница
            if len(products) < 20:
                break

            page += 1
            await asyncio.sleep(0.3)  # Пауза между запросами

        return all_products

    async def scrape_all(
        self,
        max_categories: int = None,
        max_pages_per_category: int = 100,
        include_subcategories: bool = True
    ):
        """Полный скрейпинг всех категорий"""
        print("\n" + "="*60)
        print("ПОЛНЫЙ СКРЕЙПИНГ ALGATOP")
        print("="*60)

        # Даты: последний месяц
        end = datetime.now()
        start = end - timedelta(days=30)
        start_date = start.strftime("%Y%m%d")
        end_date = end.strftime("%Y%m%d")
        print(f"📅 Период: {start_date} - {end_date}")

        # Получаем категории
        categories = await self.get_categories(start_date, end_date)
        if not categories:
            print("❌ Не удалось получить категории")
            return

        if max_categories:
            categories = categories[:max_categories]

        all_products = []
        all_categories_data = []

        for i, cat in enumerate(categories, 1):
            cat_id = cat.get("category_id", cat.get("id", cat.get("code")))
            cat_name = cat.get("category_name", cat.get("name", f"Category_{cat_id}"))
            has_subcategories = cat.get("is_has_subcategory", 0) == 1

            print(f"\n[{i}/{len(categories)}] 📁 {cat_name} (ID: {cat_id})")

            # Сохраняем данные категории
            all_categories_data.append(cat)

            # Получаем товары напрямую из категории (без подкатегорий)
            products = await self.get_all_products_in_category(
                category_id=cat_id,
                category_name=cat_name,
                max_pages=max_pages_per_category,
                start_date=start_date,
                end_date=end_date
            )
            all_products.extend(products)
            print(f"  ✅ Собрано {len(products)} товаров")

            # Промежуточное сохранение каждые 5 категорий
            if i % 5 == 0:
                print(f"\n💾 Промежуточное сохранение ({len(all_products)} товаров)...")
                self.products = all_products
                await self.save_to_json(f"products_checkpoint_{i}.json")

            await asyncio.sleep(0.5)

        self.products = all_products
        self.categories = all_categories_data

        print(f"\n" + "="*60)
        print(f"✅ СКРЕЙПИНГ ЗАВЕРШЁН")
        print(f"   Категорий: {len(all_categories_data)}")
        print(f"   Товаров: {len(all_products)}")
        print("="*60)

        return all_products

    async def save_to_csv(self, filename: str = None):
        """Сохранение в CSV"""
        if not filename:
            filename = f"algatop_products_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

        filepath = DATA_DIR / filename

        if not self.products:
            print("⚠️ Нет товаров для сохранения")
            return

        # Собираем все ключи
        all_keys = set()
        for p in self.products:
            all_keys.update(p.keys())

        headers = sorted(list(all_keys))

        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            for product in self.products:
                writer.writerow(product)

        print(f"\n💾 CSV сохранён: {filepath}")
        print(f"   Товаров: {len(self.products)}")

    async def save_to_json(self, filename: str = None):
        """Сохранение в JSON"""
        if not filename:
            filename = f"algatop_products_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

        filepath = DATA_DIR / filename

        data = {
            "scraped_at": datetime.now().isoformat(),
            "total_categories": len(self.categories),
            "total_products": len(self.products),
            "categories": self.categories,
            "products": self.products
        }

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        print(f"💾 JSON сохранён: {filepath}")

    async def close(self):
        await self.client.aclose()


async def main():
    print("=" * 60)
    print("ALGATOP API SCRAPER")
    print("=" * 60)

    api = NicheDataAPI()

    try:
        # Полный скрейпинг - 500 товаров на категорию (25 страниц по 20)
        await api.scrape_all(
            max_categories=None,  # Все категории
            max_pages_per_category=25,  # 25 страниц × 20 = 500 товаров
            include_subcategories=False
        )

        # Сохраняем результаты
        await api.save_to_json()
        await api.save_to_csv()

    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

        # Сохраняем что успели собрать
        if api.products:
            print("\n💾 Сохраняю собранные данные...")
            await api.save_to_json("products_partial.json")
            await api.save_to_csv("products_partial.csv")

    finally:
        await api.close()


if __name__ == "__main__":
    asyncio.run(main())
