"""
Загрузка данных Kaspi в базу данных Cube Demper

Этот скрипт:
1. Читает собранные данные из JSON файлов
2. Загружает их в таблицы niche_categories и niche_products
"""

import asyncio
import json
import os
from pathlib import Path
from datetime import datetime
from typing import List, Dict
import asyncpg

# Путь к данным
DATA_DIR = Path(__file__).parent.parent / "data" / "kaspi"

# Database URL (можно переопределить через переменную окружения)
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/cube_demper"
)


async def get_connection() -> asyncpg.Connection:
    """Получить соединение с базой данных"""
    return await asyncpg.connect(DATABASE_URL)


async def load_categories(conn: asyncpg.Connection, categories: List[Dict]) -> Dict[str, str]:
    """
    Загрузить категории в базу данных
    Возвращает маппинг kaspi_category_id -> uuid
    """
    category_map = {}

    for cat in categories:
        kaspi_id = cat.get("code", "")
        name = cat.get("title", "").replace("<br/>", " ").strip()

        if not name:
            continue

        # Upsert категории
        result = await conn.fetchrow(
            """
            INSERT INTO niche_categories (
                kaspi_category_id, name, coefficient, status
            ) VALUES ($1, $2, $3, $4)
            ON CONFLICT (kaspi_category_id) DO UPDATE SET
                name = EXCLUDED.name,
                updated_at = NOW()
            RETURNING id
            """,
            kaspi_id,
            name,
            15.0,  # default coefficient
            "open"
        )

        if result:
            category_map[kaspi_id] = str(result["id"])
            category_map[name] = str(result["id"])

    return category_map


async def load_products(
    conn: asyncpg.Connection,
    products: List[Dict],
    category_map: Dict[str, str]
) -> int:
    """
    Загрузить товары в базу данных
    Возвращает количество загруженных товаров
    """
    loaded = 0

    for p in products:
        kaspi_product_id = str(p.get("id", ""))
        if not kaspi_product_id:
            continue

        # Ищем категорию
        cat_code = p.get("_category_code", "")
        cat_name = p.get("_category_name", "")
        category_id = category_map.get(cat_code) or category_map.get(cat_name)

        if not category_id:
            # Создаём категорию если не нашли
            if cat_name:
                result = await conn.fetchrow(
                    """
                    INSERT INTO niche_categories (kaspi_category_id, name, coefficient, status)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (kaspi_category_id) DO UPDATE SET updated_at = NOW()
                    RETURNING id
                    """,
                    cat_code or "unknown",
                    cat_name,
                    15.0,
                    "open"
                )
                if result:
                    category_id = str(result["id"])
                    category_map[cat_code] = category_id
                    category_map[cat_name] = category_id

        if not category_id:
            continue

        # Данные товара
        name = p.get("title", "") or p.get("name", "")
        brand = p.get("brand", "")
        price = int(p.get("unitPrice", 0) or p.get("price", 0))
        reviews_count = int(p.get("reviewsQuantity", 0) or p.get("reviews_count", 0))
        rating = float(p.get("rating", 0) or 0)
        estimated_sales = int(p.get("_estimated_monthly_sales", 0))
        estimated_revenue = int(p.get("_estimated_monthly_revenue", 0))

        # Image URL
        images = p.get("images", [])
        image_url = images[0] if images else None
        if not image_url:
            primary_image = p.get("primaryImage", {})
            image_url = primary_image.get("large") or primary_image.get("medium") or primary_image.get("small")

        # Kaspi URL
        kaspi_url = f"https://kaspi.kz/shop/p/-{kaspi_product_id}/"

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
                    estimated_sales = EXCLUDED.estimated_sales,
                    estimated_revenue = EXCLUDED.estimated_revenue,
                    image_url = COALESCE(EXCLUDED.image_url, niche_products.image_url),
                    updated_at = NOW()
                """,
                category_id,
                kaspi_product_id,
                name[:500] if name else "Unknown",
                brand[:255] if brand else None,
                price,
                reviews_count,
                rating,
                1,  # sellers_count (пока 1, потом можно обогатить)
                estimated_sales,
                estimated_revenue,
                image_url,
                kaspi_url
            )
            loaded += 1
        except Exception as e:
            print(f"  Error loading product {kaspi_product_id}: {e}")

    return loaded


async def update_category_stats(conn: asyncpg.Connection):
    """Обновить агрегированную статистику по категориям"""
    await conn.execute(
        """
        UPDATE niche_categories nc SET
            total_products = stats.cnt,
            total_revenue = stats.revenue,
            avg_price = stats.avg_price,
            updated_at = NOW()
        FROM (
            SELECT
                category_id,
                COUNT(*) as cnt,
                COALESCE(SUM(estimated_revenue), 0) as revenue,
                COALESCE(AVG(price), 0) as avg_price
            FROM niche_products
            GROUP BY category_id
        ) stats
        WHERE nc.id = stats.category_id
        """
    )


async def load_from_json(filepath: Path):
    """Загрузить данные из JSON файла"""
    print(f"\nЗагрузка данных из: {filepath.name}")

    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)

    categories = data.get("categories", [])
    products = data.get("products", [])

    print(f"  Категорий: {len(categories)}")
    print(f"  Товаров: {len(products)}")

    conn = await get_connection()

    try:
        # Загружаем категории
        print("  Загрузка категорий...")
        category_map = await load_categories(conn, categories)
        print(f"    Загружено: {len(category_map) // 2} категорий")

        # Загружаем товары
        print("  Загрузка товаров...")
        loaded = await load_products(conn, products, category_map)
        print(f"    Загружено: {loaded} товаров")

        # Обновляем статистику категорий
        print("  Обновление статистики категорий...")
        await update_category_stats(conn)

        print("  ✅ Готово!")

    finally:
        await conn.close()


async def load_all_data():
    """Загрузить все данные из папки kaspi"""
    print("=" * 60)
    print("ЗАГРУЗКА ДАННЫХ KASPI В БАЗУ ДАННЫХ")
    print("=" * 60)

    # Ищем JSON файлы с данными
    json_files = sorted(DATA_DIR.glob("kaspi_products_*.json"))

    if not json_files:
        print(f"Нет файлов данных в {DATA_DIR}")
        print("Сначала запустите scrape_kaspi.py для сбора данных")
        return

    print(f"Найдено {len(json_files)} файлов данных")

    # Загружаем последний (самый свежий) файл
    latest = json_files[-1]
    await load_from_json(latest)

    # Финальная статистика
    conn = await get_connection()
    try:
        stats = await conn.fetchrow(
            """
            SELECT
                (SELECT COUNT(*) FROM niche_categories) as categories,
                (SELECT COUNT(*) FROM niche_products) as products,
                (SELECT SUM(estimated_revenue) FROM niche_products) as total_revenue
            """
        )
        print(f"\n{'=' * 60}")
        print("ИТОГОВАЯ СТАТИСТИКА В БАЗЕ:")
        print(f"  Категорий: {stats['categories']}")
        print(f"  Товаров: {stats['products']}")
        print(f"  Общая выручка: {stats['total_revenue']:,} ₸")
        print("=" * 60)
    finally:
        await conn.close()


async def main():
    import sys

    if len(sys.argv) > 1:
        # Загрузить конкретный файл
        filepath = Path(sys.argv[1])
        if filepath.exists():
            await load_from_json(filepath)
        else:
            print(f"Файл не найден: {filepath}")
    else:
        # Загрузить все данные
        await load_all_data()


if __name__ == "__main__":
    asyncio.run(main())
