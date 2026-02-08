"""Niche analysis API endpoints - поиск прибыльных ниш на Kaspi.kz"""

from fastapi import APIRouter, Depends, HTTPException, Query, status
from typing import Annotated, Optional, List
import asyncpg
from datetime import datetime
import uuid

from ..core.database import get_db_pool
from ..dependencies import get_current_user
from ..models.niche import NicheCategory, NicheProduct, NicheProductHistory

router = APIRouter()


# ================== КАТЕГОРИИ ==================

@router.get("/categories")
async def get_categories(
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
    current_user: Annotated[dict, Depends(get_current_user)],
    parent_id: Optional[str] = None,
    sort_by: str = Query("total_revenue", enum=["total_revenue", "total_products", "total_sellers", "name"]),
    order: str = Query("desc", enum=["asc", "desc"])
):
    """
    Получить список категорий с метриками

    Уровень 1 аналитики - "вертолётный взгляд" на ниши
    """
    async with pool.acquire() as conn:
        order_dir = "DESC" if order == "desc" else "ASC"
        parent_filter = "parent_id = $1" if parent_id else "parent_id IS NULL"

        query = f"""
            SELECT
                id, name, parent_id, kaspi_category_id,
                coefficient, total_products, total_sellers,
                avg_price, total_revenue, status,
                created_at, updated_at
            FROM niche_categories
            WHERE {parent_filter}
            ORDER BY {sort_by} {order_dir}
        """

        if parent_id:
            rows = await conn.fetch(query, uuid.UUID(parent_id))
        else:
            rows = await conn.fetch(query.replace("$1", "NULL").replace("parent_id = NULL", "parent_id IS NULL"))

        categories = [NicheCategory.from_row(dict(row)).to_dict() for row in rows]

        return {
            "categories": categories,
            "total": len(categories)
        }


@router.get("/categories/{category_id}")
async def get_category_details(
    category_id: str,
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
    current_user: Annotated[dict, Depends(get_current_user)]
):
    """
    Детальная информация о категории

    Уровень 2 аналитики - тренды, топ брендов, сезонность
    """
    async with pool.acquire() as conn:
        # Основная информация о категории
        category = await conn.fetchrow(
            """
            SELECT * FROM niche_categories WHERE id = $1
            """,
            uuid.UUID(category_id)
        )

        if not category:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Category not found"
            )

        # Топ брендов в категории
        top_brands = await conn.fetch(
            """
            SELECT
                brand,
                COUNT(*) as products_count,
                SUM(estimated_revenue) as total_revenue,
                SUM(estimated_sales) as total_sales
            FROM niche_products
            WHERE category_id = $1 AND brand IS NOT NULL
            GROUP BY brand
            ORDER BY total_revenue DESC
            LIMIT 10
            """,
            uuid.UUID(category_id)
        )

        # Сезонность - продажи по месяцам
        seasonality = await conn.fetch(
            """
            SELECT
                year, month,
                SUM(estimated_sales) as total_sales,
                SUM(estimated_revenue) as total_revenue
            FROM niche_product_history nph
            JOIN niche_products np ON np.id = nph.product_id
            WHERE np.category_id = $1
            GROUP BY year, month
            ORDER BY year, month
            """,
            uuid.UUID(category_id)
        )

        return {
            "category": NicheCategory.from_row(dict(category)).to_dict(),
            "top_brands": [
                {
                    "brand": row["brand"],
                    "products_count": row["products_count"],
                    "total_revenue": row["total_revenue"],
                    "total_sales": row["total_sales"]
                }
                for row in top_brands
            ],
            "seasonality": [
                {
                    "year": row["year"],
                    "month": row["month"],
                    "total_sales": row["total_sales"],
                    "total_revenue": row["total_revenue"]
                }
                for row in seasonality
            ]
        }


# ================== ТОВАРЫ ==================

@router.get("/products")
async def get_products(
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
    current_user: Annotated[dict, Depends(get_current_user)],
    category_id: Optional[str] = None,
    min_revenue: Optional[int] = None,
    max_revenue: Optional[int] = None,
    min_sales: Optional[int] = None,
    max_sellers: Optional[int] = None,
    min_rating: Optional[float] = None,
    brand: Optional[str] = None,
    search: Optional[str] = None,
    sort_by: str = Query("estimated_revenue", enum=[
        "estimated_revenue", "estimated_sales", "reviews_count",
        "rating", "sellers_count", "price", "name"
    ]),
    order: str = Query("desc", enum=["asc", "desc"]),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0)
):
    """
    Поиск товаров с фильтрами

    Уровень 3 аналитики - таблица товаров с фильтрами
    Основной инструмент для поиска "золотых" товаров
    """
    async with pool.acquire() as conn:
        # Построение WHERE условий
        conditions = []
        params = []
        param_idx = 1

        if category_id:
            conditions.append(f"category_id = ${param_idx}")
            params.append(uuid.UUID(category_id))
            param_idx += 1

        if min_revenue:
            conditions.append(f"estimated_revenue >= ${param_idx}")
            params.append(min_revenue)
            param_idx += 1

        if max_revenue:
            conditions.append(f"estimated_revenue <= ${param_idx}")
            params.append(max_revenue)
            param_idx += 1

        if min_sales:
            conditions.append(f"estimated_sales >= ${param_idx}")
            params.append(min_sales)
            param_idx += 1

        if max_sellers:
            conditions.append(f"sellers_count <= ${param_idx}")
            params.append(max_sellers)
            param_idx += 1

        if min_rating:
            conditions.append(f"rating >= ${param_idx}")
            params.append(min_rating)
            param_idx += 1

        if brand:
            conditions.append(f"brand ILIKE ${param_idx}")
            params.append(f"%{brand}%")
            param_idx += 1

        if search:
            conditions.append(f"name ILIKE ${param_idx}")
            params.append(f"%{search}%")
            param_idx += 1

        where_clause = " AND ".join(conditions) if conditions else "1=1"
        order_dir = "DESC" if order == "desc" else "ASC"

        # Основной запрос
        query = f"""
            SELECT
                np.*,
                nc.name as category_name
            FROM niche_products np
            LEFT JOIN niche_categories nc ON nc.id = np.category_id
            WHERE {where_clause}
            ORDER BY {sort_by} {order_dir}
            LIMIT ${param_idx} OFFSET ${param_idx + 1}
        """
        params.extend([limit, offset])

        rows = await conn.fetch(query, *params)

        # Подсчёт общего количества
        count_query = f"""
            SELECT COUNT(*) FROM niche_products WHERE {where_clause}
        """
        total = await conn.fetchval(count_query, *params[:-2]) if params[:-2] else await conn.fetchval(count_query)

        products = []
        for row in rows:
            product = NicheProduct.from_row(dict(row)).to_dict()
            product["category_name"] = row.get("category_name")
            products.append(product)

        return {
            "products": products,
            "total": total,
            "limit": limit,
            "offset": offset
        }


@router.get("/products/{product_id}")
async def get_product_details(
    product_id: str,
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
    current_user: Annotated[dict, Depends(get_current_user)]
):
    """
    Детальная информация о товаре с историей
    """
    async with pool.acquire() as conn:
        # Основная информация
        product = await conn.fetchrow(
            """
            SELECT
                np.*,
                nc.name as category_name,
                nc.coefficient as category_coefficient
            FROM niche_products np
            LEFT JOIN niche_categories nc ON nc.id = np.category_id
            WHERE np.id = $1
            """,
            uuid.UUID(product_id)
        )

        if not product:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Product not found"
            )

        # История по месяцам
        history = await conn.fetch(
            """
            SELECT * FROM niche_product_history
            WHERE product_id = $1
            ORDER BY year, month
            """,
            uuid.UUID(product_id)
        )

        product_data = NicheProduct.from_row(dict(product)).to_dict()
        product_data["category_name"] = product.get("category_name")
        product_data["category_coefficient"] = product.get("category_coefficient")

        return {
            "product": product_data,
            "history": [
                NicheProductHistory.from_row(dict(row)).to_dict()
                for row in history
            ]
        }


# ================== КАЛЬКУЛЯТОР UNIT-ЭКОНОМИКИ ==================

@router.post("/calculate-unit-economics")
async def calculate_unit_economics(
    current_user: Annotated[dict, Depends(get_current_user)],
    purchase_price: int,  # Закупочная цена
    selling_price: int,  # Цена продажи
    packaging_cost: int = 0,  # Упаковка
    delivery_cost: int = 0,  # Доставка до склада
    kaspi_commission_percent: float = 12.0,  # Комиссия Kaspi (%)
    kaspi_delivery_cost: int = 0,  # Доставка Kaspi (если есть)
):
    """
    Калькулятор Unit-экономики

    Рассчитывает чистую прибыль и ROI с учётом всех расходов и комиссий Kaspi
    """
    # Расчёт комиссии Kaspi
    kaspi_commission = int(selling_price * kaspi_commission_percent / 100)

    # Все расходы
    total_costs = purchase_price + packaging_cost + delivery_cost + kaspi_commission + kaspi_delivery_cost

    # Чистая прибыль
    net_profit = selling_price - total_costs

    # ROI (Return on Investment)
    investment = purchase_price + packaging_cost + delivery_cost
    roi = (net_profit / investment * 100) if investment > 0 else 0

    # Маржинальность
    margin = (net_profit / selling_price * 100) if selling_price > 0 else 0

    return {
        "selling_price": selling_price,
        "costs": {
            "purchase_price": purchase_price,
            "packaging": packaging_cost,
            "delivery": delivery_cost,
            "kaspi_commission": kaspi_commission,
            "kaspi_commission_percent": kaspi_commission_percent,
            "kaspi_delivery": kaspi_delivery_cost,
            "total": total_costs
        },
        "net_profit": net_profit,
        "roi_percent": round(roi, 2),
        "margin_percent": round(margin, 2),
        "profitable": net_profit > 0
    }


# ================== СТАТИСТИКА ==================

@router.get("/stats")
async def get_niche_stats(
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
    current_user: Annotated[dict, Depends(get_current_user)]
):
    """
    Общая статистика по нишам
    """
    async with pool.acquire() as conn:
        stats = await conn.fetchrow(
            """
            SELECT
                (SELECT COUNT(*) FROM niche_categories) as total_categories,
                (SELECT COUNT(*) FROM niche_products) as total_products,
                (SELECT SUM(estimated_revenue) FROM niche_products) as total_revenue,
                (SELECT AVG(coefficient) FROM niche_categories) as avg_coefficient
            """
        )

        # Топ категории по выручке
        top_categories = await conn.fetch(
            """
            SELECT name, total_revenue, total_products, total_sellers
            FROM niche_categories
            ORDER BY total_revenue DESC
            LIMIT 5
            """
        )

        return {
            "total_categories": stats["total_categories"] or 0,
            "total_products": stats["total_products"] or 0,
            "total_revenue": stats["total_revenue"] or 0,
            "avg_coefficient": round(stats["avg_coefficient"] or 15.0, 2),
            "top_categories": [
                {
                    "name": row["name"],
                    "total_revenue": row["total_revenue"],
                    "total_products": row["total_products"],
                    "total_sellers": row["total_sellers"]
                }
                for row in top_categories
            ]
        }
