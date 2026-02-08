"""
Загрузка данных аналитики ниш в базу данных Cube Demper

Использует реальные данные продаж с маркетплейса
"""

import asyncio
import json
import os
from pathlib import Path
from datetime import datetime
from typing import List, Dict
import asyncpg

# Путь к данным
DATA_DIR = Path(__file__).parent.parent / "data"

# Database URL
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/cube_demper"
)


async def get_connection() -> asyncpg.Connection:
    """Получить соединение с базой данных"""
    return await asyncpg.connect(DATABASE_URL)


async def load_categories(conn: asyncpg.Connection, products: List[Dict]) -> Dict[str, str]:
    """
    Создать категории из данных товаров
    """
    # Собираем уникальные категории
    categories = {}
    for p in products:
        cat_name = p.get("_category_name", "")
        if cat_name and cat_name not in categories:
            categories[cat_name] = {
                "name": cat_name,
                "products": 0,
                "revenue": 0
            }
        if cat_name:
            categories[cat_name]["products"] += 1
            categories[cat_name]["revenue"] += p.get("sale_amount", 0) or 0

    category_map = {}

    for cat_name, data in categories.items():
        # Генерируем kaspi_category_id из названия
        kaspi_id = cat_name.lower().replace(" ", "_").replace(",", "")

        result = await conn.fetchrow(
            """
            INSERT INTO niche_categories (
                kaspi_category_id, name, coefficient, status,
                total_products, total_revenue
            ) VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (kaspi_category_id) DO UPDATE SET
                name = EXCLUDED.name,
                total_products = EXCLUDED.total_products,
                total_revenue = EXCLUDED.total_revenue,
                updated_at = NOW()
            RETURNING id
            """,
            kaspi_id,
            cat_name,
            15.0,
            "open",
            data["products"],
            int(data["revenue"])
        )

        if result:
            category_map[cat_name] = str(result["id"])

    return category_map


async def load_products(
    conn: asyncpg.Connection,
    products: List[Dict],
    category_map: Dict[str, str]
) -> int:
    """
    Загрузить товары в базу данных
    """
    loaded = 0

    for p in products:
        # ID товара
        kaspi_product_id = str(p.get("product_code", ""))
        if not kaspi_product_id:
            kaspi_product_id = str(hash(p.get("product_name", "") + str(p.get("sale_price", 0))))

        # Категория
        cat_name = p.get("_category_name", "")
        category_id = category_map.get(cat_name)
        if not category_id:
            continue

        # Данные товара
        name = p.get("product_name", "")[:500] if p.get("product_name") else "Unknown"
        brand = p.get("brand_name", "") or ""
        price = int(p.get("sale_price", 0) or 0)
        reviews_count = int(p.get("review_qty", 0) or 0)
        rating = float(p.get("product_rate", 0) or 0)
        sellers_count = int(p.get("merchant_count", 1) or 1)

        # Реальные данные продаж
        estimated_sales = int(p.get("sale_qty", 0) or 0)
        estimated_revenue = int(p.get("sale_amount", 0) or 0)

        # URL картинки
        images_raw = p.get("preview_image_list")
        image_url = None
        try:
            # Может быть JSON-строкой
            if isinstance(images_raw, str):
                images = json.loads(images_raw)
            else:
                images = images_raw

            if images and isinstance(images, list) and len(images) > 0:
                first_img = images[0]
                if isinstance(first_img, dict):
                    image_url = first_img.get("large") or first_img.get("medium") or first_img.get("small")
                elif isinstance(first_img, str):
                    image_url = first_img
        except (json.JSONDecodeError, TypeError):
            pass

        # Kaspi URL
        product_url = p.get("product_url", "")
        kaspi_url = f"https://kaspi.kz{product_url}" if product_url else None

        try:
            await conn.execute(
                """
                INSERT INTO niche_products (
                    category_id, kaspi_product_id, name, brand,
                    price, reviews_count, rating, sellers_count,
                    estimated_sales, estimated_revenue, image_url, kaspi_url
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                ON CONFLICT (kaspi_product_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    brand = EXCLUDED.brand,
                    price = EXCLUDED.price,
                    reviews_count = EXCLUDED.reviews_count,
                    rating = EXCLUDED.rating,
                    sellers_count = EXCLUDED.sellers_count,
                    estimated_sales = EXCLUDED.estimated_sales,
                    estimated_revenue = EXCLUDED.estimated_revenue,
                    image_url = COALESCE(EXCLUDED.image_url, niche_products.image_url),
                    kaspi_url = COALESCE(EXCLUDED.kaspi_url, niche_products.kaspi_url),
                    updated_at = NOW()
                """,
                category_id,
                kaspi_product_id,
                name if name else "Unknown",
                brand[:255] if brand else None,
                price,
                reviews_count,
                rating,
                sellers_count,
                estimated_sales,
                estimated_revenue,
                image_url,
                kaspi_url
            )
            loaded += 1

            if loaded % 500 == 0:
                print(f"    Загружено {loaded} товаров...")

        except Exception as e:
            print(f"  Error: {e}")

    return loaded


async def load_niche_data():
    """Загрузить данные аналитики ниш в базу"""
    print("=" * 60)
    print("ЗАГРУЗКА ДАННЫХ АНАЛИТИКИ НИШ В БАЗУ ДАННЫХ")
    print("=" * 60)

    # Ищем последний файл с данными
    json_files = sorted(DATA_DIR.glob("*_products_*.json"))

    if not json_files:
        print("Нет файлов с данными аналитики")
        return

    latest = json_files[-1]
    print(f"\nФайл: {latest.name}")

    with open(latest, 'r', encoding='utf-8') as f:
        data = json.load(f)

    products = data.get("products", [])
    print(f"Товаров в файле: {len(products)}")

    conn = await get_connection()

    try:
        # Загружаем категории
        print("\n1. Загрузка категорий...")
        category_map = await load_categories(conn, products)
        print(f"   Создано категорий: {len(category_map)}")

        # Загружаем товары
        print("\n2. Загрузка товаров...")
        loaded = await load_products(conn, products, category_map)
        print(f"   Загружено товаров: {loaded}")

        # Финальная статистика
        stats = await conn.fetchrow(
            """
            SELECT
                (SELECT COUNT(*) FROM niche_categories) as categories,
                (SELECT COUNT(*) FROM niche_products) as products,
                (SELECT SUM(estimated_revenue) FROM niche_products) as revenue
            """
        )
        print(f"\n{'=' * 60}")
        print("ИТОГО В БАЗЕ:")
        print(f"  Категорий: {stats['categories']}")
        print(f"  Товаров: {stats['products']}")
        print(f"  Выручка: {stats['revenue']:,} ₸")
        print("=" * 60)

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(load_niche_data())
