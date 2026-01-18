"""Kaspi router - handles Kaspi store management, authentication, and product sync"""

from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks
from typing import Annotated, List
import asyncpg
import uuid
import logging
import json
from datetime import datetime, timedelta

from ..schemas.kaspi import (
    KaspiStoreResponse,
    KaspiAuthRequest,
    KaspiAuthSMSRequest,
    StoreSyncRequest,
    ProductUpdateRequest,
    BulkPriceUpdateRequest,
    StoreCreateRequest,
    DempingSettings,
    StoreStats,
    SalesAnalytics,
    TopProduct,
)
from ..schemas.products import ProductResponse, ProductListResponse, ProductFilters, ProductAnalytics
from ..core.database import get_db_pool
from ..dependencies import get_current_user
from ..services.kaspi_auth_service import (
    authenticate_kaspi,
    verify_sms_code,
    get_active_session,
    KaspiSMSRequiredError,
    KaspiAuthError,
)
from ..services.api_parser import (
    get_products,
    sync_product,
    batch_sync_products,
)
from ..core.security import encrypt_session

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/stores", response_model=List[KaspiStoreResponse])
async def list_stores(
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """List all Kaspi stores for current user"""
    async with pool.acquire() as conn:
        stores = await conn.fetch(
            """
            SELECT id, user_id, merchant_id, name, api_key, products_count,
                   last_sync, is_active, created_at, updated_at
            FROM kaspi_stores
            WHERE user_id = $1
            ORDER BY created_at DESC
            """,
            current_user['id']
        )

        return [
            KaspiStoreResponse(
                id=str(store['id']),
                user_id=str(store['user_id']),
                merchant_id=store['merchant_id'],
                name=store['name'],
                products_count=store['products_count'],
                last_sync=store['last_sync'],
                is_active=store['is_active'],
                created_at=store['created_at'],
                updated_at=store['updated_at']
            )
            for store in stores
        ]


@router.post("/auth", status_code=status.HTTP_200_OK)
async def authenticate_store(
    auth_data: KaspiAuthRequest,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
    background_tasks: BackgroundTasks
):
    """
    Authenticate with Kaspi and create/update store.

    Returns:
        - Success: { "status": "success", "store_id": "...", "merchant_id": "..." }
        - SMS Required: { "status": "sms_required", "merchant_id": "..." }
    """
    try:
        # Attempt authentication
        session_data = await authenticate_kaspi(
            email=auth_data.email,
            password=auth_data.password,
            merchant_id=auth_data.merchant_id
        )

        # Encrypt session data
        encrypted_guid = encrypt_session(session_data)
        merchant_id = session_data.get('merchant_uid')
        shop_name = session_data.get('shop_name', f"Store {merchant_id}")

        # Store in database (wrap encrypted string in JSON object)
        async with pool.acquire() as conn:
            store = await conn.fetchrow(
                """
                INSERT INTO kaspi_stores (user_id, merchant_id, name, guid, is_active)
                VALUES ($1, $2, $3, $4, true)
                ON CONFLICT (merchant_id)
                DO UPDATE SET guid = $4, is_active = true, updated_at = NOW()
                RETURNING id, merchant_id
                """,
                current_user['id'],
                merchant_id,
                shop_name,
                json.dumps({'encrypted': encrypted_guid})
            )

        # Auto-sync products after successful authentication
        background_tasks.add_task(
            _sync_store_products_task,
            store_id=str(store['id']),
            merchant_id=merchant_id
        )
        logger.info(f"Started automatic product sync for store {store['id']}")

        return {
            "status": "success",
            "store_id": str(store['id']),
            "merchant_id": merchant_id
        }

    except KaspiSMSRequiredError as e:
        # SMS verification required
        return {
            "status": "sms_required",
            "merchant_id": e.partial_session.get('merchant_uid'),
            "message": "SMS verification code required"
        }

    except KaspiAuthError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(e)
        )


@router.post("/auth/verify-sms", status_code=status.HTTP_200_OK)
async def verify_sms(
    sms_data: KaspiAuthSMSRequest,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
    background_tasks: BackgroundTasks
):
    """Complete Kaspi authentication with SMS code"""
    try:
        # Get partial session from database
        async with pool.acquire() as conn:
            store = await conn.fetchrow(
                """
                SELECT guid FROM kaspi_stores
                WHERE merchant_id = $1 AND user_id = $2
                """,
                sms_data.merchant_id,
                current_user['id']
            )

            if not store or not store['guid']:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="No pending authentication found for this merchant"
                )

            # Verify SMS code
            from ..core.security import decrypt_session
            # Extract encrypted string from JSON object
            guid_data = store['guid']
            if isinstance(guid_data, dict):
                encrypted_guid = guid_data.get('encrypted')
            else:
                # Fallback for old format (plain string)
                encrypted_guid = guid_data

            partial_session = decrypt_session(encrypted_guid)

            complete_session = await verify_sms_code(
                merchant_id=sms_data.merchant_id,
                sms_code=sms_data.sms_code,
                partial_session=partial_session
            )

            # Update store with complete session
            encrypted_guid = encrypt_session(complete_session)
            store_id = await conn.fetchval(
                """
                UPDATE kaspi_stores
                SET guid = $1, is_active = true, updated_at = NOW()
                WHERE merchant_id = $2 AND user_id = $3
                RETURNING id
                """,
                encrypted_guid,
                sms_data.merchant_id,
                current_user['id']
            )

        # Auto-sync products after successful SMS verification
        background_tasks.add_task(
            _sync_store_products_task,
            store_id=str(store_id),
            merchant_id=sms_data.merchant_id
        )
        logger.info(f"Started automatic product sync for store {store_id} after SMS verification")

        return {"status": "success", "merchant_id": sms_data.merchant_id}

    except KaspiAuthError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(e)
        )


async def _sync_store_products_task(store_id: str, merchant_id: str):
    """Background task to sync store products"""
    try:
        pool = await get_db_pool()

        async with pool.acquire() as conn:
            # Get session
            store = await conn.fetchrow(
                "SELECT guid FROM kaspi_stores WHERE id = $1",
                uuid.UUID(store_id)
            )

            from ..core.security import decrypt_session
            # Extract encrypted string from JSON object
            guid_data = store['guid']
            if isinstance(guid_data, dict):
                encrypted_guid = guid_data.get('encrypted')
            else:
                # Fallback for old format (plain string)
                encrypted_guid = guid_data

            session = decrypt_session(encrypted_guid)

            # Fetch products from Kaspi API
            products = await get_products(merchant_id, session)

            # Upsert products to database
            for product_data in products:
                await conn.execute(
                    """
                    INSERT INTO products (
                        store_id, kaspi_product_id, kaspi_sku, external_kaspi_id,
                        name, price, availabilities
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (store_id, kaspi_product_id)
                    DO UPDATE SET
                        name = $5,
                        price = $6,
                        availabilities = $7,
                        updated_at = NOW()
                    """,
                    uuid.UUID(store_id),
                    product_data['kaspi_product_id'],
                    product_data.get('kaspi_sku'),
                    product_data.get('external_kaspi_id'),
                    product_data['name'],
                    product_data['price'],
                    product_data.get('availabilities')
                )

            # Update store products count and last sync
            await conn.execute(
                """
                UPDATE kaspi_stores
                SET products_count = $1, last_sync = NOW()
                WHERE id = $2
                """,
                len(products),
                uuid.UUID(store_id)
            )

            logger.info(f"Synced {len(products)} products for store {store_id}")

    except Exception as e:
        logger.error(f"Error syncing store {store_id}: {e}")


@router.get("/products", response_model=ProductListResponse)
async def list_products(
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
    filters: ProductFilters = Depends()
):
    """List products with filtering and pagination"""
    async with pool.acquire() as conn:
        # Build query conditions
        conditions = ["k.user_id = $1"]
        params = [current_user['id']]
        param_count = 1

        if filters.store_id:
            param_count += 1
            conditions.append(f"p.store_id = ${param_count}")
            params.append(uuid.UUID(filters.store_id))

        if filters.bot_active is not None:
            param_count += 1
            conditions.append(f"p.bot_active = ${param_count}")
            params.append(filters.bot_active)

        if filters.search:
            param_count += 1
            conditions.append(f"p.name ILIKE ${param_count}")
            params.append(f"%{filters.search}%")

        where_clause = " AND ".join(conditions)

        # Get total count
        total = await conn.fetchval(
            f"""
            SELECT COUNT(*)
            FROM products p
            JOIN kaspi_stores k ON k.id = p.store_id
            WHERE {where_clause}
            """,
            *params
        )

        # Get products
        offset = (filters.page - 1) * filters.page_size
        products = await conn.fetch(
            f"""
            SELECT p.id, p.store_id, p.kaspi_product_id, p.kaspi_sku, p.external_kaspi_id,
                   p.name, p.price, p.min_profit, p.bot_active, p.last_check_time,
                   p.availabilities, p.created_at, p.updated_at
            FROM products p
            JOIN kaspi_stores k ON k.id = p.store_id
            WHERE {where_clause}
            ORDER BY p.created_at DESC
            LIMIT ${param_count + 1} OFFSET ${param_count + 2}
            """,
            *params, filters.page_size, offset
        )

        product_responses = [
            ProductResponse(
                id=str(p['id']),
                store_id=str(p['store_id']),
                kaspi_product_id=p['kaspi_product_id'],
                kaspi_sku=p['kaspi_sku'],
                external_kaspi_id=p['external_kaspi_id'],
                name=p['name'],
                price=p['price'],
                min_profit=p['min_profit'],
                bot_active=p['bot_active'],
                last_check_time=p['last_check_time'],
                availabilities=p['availabilities'],
                created_at=p['created_at'],
                updated_at=p['updated_at']
            )
            for p in products
        ]

        return ProductListResponse(
            products=product_responses,
            total=total,
            page=filters.page,
            page_size=filters.page_size,
            has_more=(offset + filters.page_size) < total
        )


@router.patch("/products/{product_id}", response_model=ProductResponse)
async def update_product(
    product_id: str,
    update_data: ProductUpdateRequest,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Update product settings"""
    async with pool.acquire() as conn:
        # Verify ownership
        product = await conn.fetchrow(
            """
            SELECT p.* FROM products p
            JOIN kaspi_stores k ON k.id = p.store_id
            WHERE p.id = $1 AND k.user_id = $2
            """,
            uuid.UUID(product_id),
            current_user['id']
        )

        if not product:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Product not found"
            )

        # Build update query
        updates = []
        params = []
        param_count = 0

        if update_data.price is not None:
            param_count += 1
            updates.append(f"price = ${param_count}")
            params.append(update_data.price)

        if update_data.min_profit is not None:
            param_count += 1
            updates.append(f"min_profit = ${param_count}")
            params.append(update_data.min_profit)

        if update_data.bot_active is not None:
            param_count += 1
            updates.append(f"bot_active = ${param_count}")
            params.append(update_data.bot_active)

        if not updates:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No fields to update"
            )

        param_count += 1
        params.append(uuid.UUID(product_id))

        updated = await conn.fetchrow(
            f"""
            UPDATE products
            SET {', '.join(updates)}, updated_at = NOW()
            WHERE id = ${param_count}
            RETURNING *
            """,
            *params
        )

        return ProductResponse(
            id=str(updated['id']),
            store_id=str(updated['store_id']),
            kaspi_product_id=updated['kaspi_product_id'],
            kaspi_sku=updated['kaspi_sku'],
            external_kaspi_id=updated['external_kaspi_id'],
            name=updated['name'],
            price=updated['price'],
            min_profit=updated['min_profit'],
            bot_active=updated['bot_active'],
            last_check_time=updated['last_check_time'],
            availabilities=updated['availabilities'],
            created_at=updated['created_at'],
            updated_at=updated['updated_at']
        )


@router.post("/products/bulk-update", status_code=status.HTTP_200_OK)
async def bulk_update_products(
    bulk_data: BulkPriceUpdateRequest,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Bulk update product prices or settings"""
    async with pool.acquire() as conn:
        # Verify all products belong to user
        product_uuids = [uuid.UUID(pid) for pid in bulk_data.product_ids]

        count = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM products p
            JOIN kaspi_stores k ON k.id = p.store_id
            WHERE p.id = ANY($1) AND k.user_id = $2
            """,
            product_uuids,
            current_user['id']
        )

        if count != len(bulk_data.product_ids):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Some products do not belong to you"
            )

        # Perform bulk update
        updates = []
        if bulk_data.bot_active is not None:
            updates.append(f"bot_active = {bulk_data.bot_active}")

        if bulk_data.price_change_percent is not None:
            updates.append(
                f"price = ROUND(price * (1 + {bulk_data.price_change_percent / 100.0}))::INTEGER"
            )
        elif bulk_data.price_change_tiyns is not None:
            updates.append(f"price = price + {bulk_data.price_change_tiyns}")

        if not updates:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No updates specified"
            )

        result = await conn.execute(
            f"""
            UPDATE products
            SET {', '.join(updates)}, updated_at = NOW()
            WHERE id = ANY($1)
            """,
            product_uuids
        )

        updated_count = int(result.split()[-1])

        return {
            "status": "success",
            "updated_count": updated_count
        }


@router.get("/analytics", response_model=ProductAnalytics)
async def get_analytics(
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Get product analytics for user"""
    async with pool.acquire() as conn:
        stats = await conn.fetchrow(
            """
            SELECT
                COUNT(*) as total_products,
                COUNT(*) FILTER (WHERE p.bot_active = true) as active_demping,
                COALESCE(AVG(p.min_profit), 0)::INTEGER as avg_profit,
                (
                    SELECT COUNT(*)
                    FROM price_history ph
                    JOIN products p2 ON p2.id = ph.product_id
                    JOIN kaspi_stores k2 ON k2.id = p2.store_id
                    WHERE k2.user_id = $1
                      AND ph.created_at >= CURRENT_DATE
                ) as changes_today
            FROM products p
            JOIN kaspi_stores k ON k.id = p.store_id
            WHERE k.user_id = $1
            """,
            current_user['id']
        )

        return ProductAnalytics(
            total_products=stats['total_products'],
            active_demping=stats['active_demping'],
            total_price_changes_today=stats['changes_today'],
            average_profit_margin_tiyns=stats['avg_profit']
        )


# ============================================================================
# Store-Specific Endpoints (REST-style)
# ============================================================================

@router.get("/stores/{store_id}/products", response_model=ProductListResponse)
async def list_store_products(
    store_id: str,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
    filters: ProductFilters = Depends()
):
    """List products for specific store (REST-style endpoint)"""
    # Verify store ownership
    async with pool.acquire() as conn:
        store = await conn.fetchrow(
            "SELECT id FROM kaspi_stores WHERE id = $1 AND user_id = $2",
            uuid.UUID(store_id),
            current_user['id']
        )
        if not store:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Store not found"
            )

    # Set store_id filter and reuse existing list_products logic
    filters.store_id = store_id
    return await list_products(current_user, pool, filters)


@router.get("/stores/{store_id}/demping", response_model=DempingSettings)
async def get_store_demping_settings(
    store_id: str,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Get demping settings for store"""
    async with pool.acquire() as conn:
        # Verify ownership
        store = await conn.fetchrow(
            "SELECT id FROM kaspi_stores WHERE id = $1 AND user_id = $2",
            uuid.UUID(store_id),
            current_user['id']
        )
        if not store:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Store not found"
            )

        # Get settings from demping_settings table (if exists)
        # For now, return default settings since table might not exist yet
        # TODO: Query actual demping_settings table after migration
        return DempingSettings(min_profit=0, bot_active=True)


@router.patch("/stores/{store_id}/demping", response_model=DempingSettings)
async def update_store_demping_settings(
    store_id: str,
    settings_update: DempingSettings,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Update demping settings for store"""
    async with pool.acquire() as conn:
        # Verify ownership
        store = await conn.fetchrow(
            "SELECT id FROM kaspi_stores WHERE id = $1 AND user_id = $2",
            uuid.UUID(store_id),
            current_user['id']
        )
        if not store:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Store not found"
            )

        # TODO: Upsert to demping_settings table after migration
        # For now, just return the settings
        logger.info(f"Demping settings update requested for store {store_id}: {settings_update}")
        return settings_update


@router.get("/stores/{store_id}/stats", response_model=StoreStats)
async def get_store_stats(
    store_id: str,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Get store statistics"""
    async with pool.acquire() as conn:
        # Verify ownership and get store info
        store = await conn.fetchrow(
            "SELECT * FROM kaspi_stores WHERE id = $1 AND user_id = $2",
            uuid.UUID(store_id),
            current_user['id']
        )
        if not store:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Store not found"
            )

        # Get products stats
        stats = await conn.fetchrow(
            """
            SELECT
                COUNT(*) as total_products,
                COUNT(*) FILTER (WHERE price > 0) as active_products,
                COUNT(*) FILTER (WHERE bot_active = true) as demping_enabled
            FROM products
            WHERE store_id = $1
            """,
            uuid.UUID(store_id)
        )

        return StoreStats(
            store_id=store_id,
            store_name=store['name'],
            products_count=stats['total_products'] or 0,
            active_products_count=stats['active_products'] or 0,
            demping_enabled_count=stats['demping_enabled'] or 0,
            last_sync=store['last_sync']
        )


@router.get("/stores/{store_id}/analytics", response_model=SalesAnalytics)
async def get_store_analytics(
    store_id: str,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
    period: str = '7d'
):
    """Get sales analytics (placeholder - orders not implemented yet)"""
    # Validate period
    if period not in ['7d', '30d', '90d']:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Period must be one of: 7d, 30d, 90d"
        )

    async with pool.acquire() as conn:
        # Verify ownership
        store = await conn.fetchrow(
            "SELECT * FROM kaspi_stores WHERE id = $1 AND user_id = $2",
            uuid.UUID(store_id),
            current_user['id']
        )
        if not store:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Store not found"
            )

        # Generate empty daily stats for the period
        days = {'7d': 7, '30d': 30, '90d': 90}[period]
        daily_stats = []
        for i in range(days):
            date = (datetime.utcnow() - timedelta(days=days-i-1)).strftime('%Y-%m-%d')
            daily_stats.append({
                'date': date,
                'orders': 0,
                'revenue': 0,
                'items': 0
            })

        return SalesAnalytics(
            store_id=store_id,
            period=period,
            daily_stats=daily_stats
        )


@router.get("/stores/{store_id}/top-products", response_model=List[TopProduct])
async def get_top_products(
    store_id: str,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
    limit: int = 10
):
    """Get top products by price changes (placeholder for sales)"""
    async with pool.acquire() as conn:
        # Verify ownership
        store = await conn.fetchrow(
            "SELECT * FROM kaspi_stores WHERE id = $1 AND user_id = $2",
            uuid.UUID(store_id),
            current_user['id']
        )
        if not store:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Store not found"
            )

        # Get products with most price updates (proxy for popularity)
        products = await conn.fetch(
            """
            SELECT
                p.id, p.kaspi_sku, p.name, p.price as current_price,
                COUNT(ph.id) as price_changes
            FROM products p
            LEFT JOIN price_history ph ON ph.product_id = p.id
            WHERE p.store_id = $1
            GROUP BY p.id, p.kaspi_sku, p.name, p.price
            ORDER BY price_changes DESC
            LIMIT $2
            """,
            uuid.UUID(store_id),
            limit
        )

        return [
            TopProduct(
                id=str(p['id']),
                kaspi_sku=p['kaspi_sku'] or '',
                name=p['name'],
                current_price=p['current_price'],
                sales_count=0,  # Placeholder
                revenue=0       # Placeholder
            )
            for p in products
        ]


# ============================================================================
# Additional Frontend Compatibility Endpoints
# ============================================================================

@router.delete("/stores/{store_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_store(
    store_id: str,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Delete a Kaspi store"""
    async with pool.acquire() as conn:
        result = await conn.execute(
            """
            DELETE FROM kaspi_stores
            WHERE id = $1 AND user_id = $2
            """,
            uuid.UUID(store_id),
            current_user['id']
        )

        if result == "DELETE 0":
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Store not found"
            )

    return None


@router.post("/stores/{store_id}/sync", status_code=status.HTTP_202_ACCEPTED)
async def sync_store_products_by_id(
    store_id: str,
    background_tasks: BackgroundTasks,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Sync products for a specific store (REST-style endpoint)"""
    async with pool.acquire() as conn:
        # Verify store ownership and get merchant_id
        store = await conn.fetchrow(
            "SELECT id, merchant_id FROM kaspi_stores WHERE id = $1 AND user_id = $2",
            uuid.UUID(store_id),
            current_user['id']
        )

        if not store:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Store not found"
            )

    # Add background task to sync products using existing function
    background_tasks.add_task(
        _sync_store_products_task,
        store_id=store_id,
        merchant_id=store['merchant_id']
    )

    return {
        "status": "accepted",
        "message": "Product sync started in background",
        "store_id": store_id
    }


@router.get("/products/{product_id}/price-history")
async def get_product_price_history(
    product_id: str,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
    limit: int = 50
):
    """Get price history for a product"""
    async with pool.acquire() as conn:
        # Verify ownership
        product = await conn.fetchrow(
            """
            SELECT p.id FROM products p
            JOIN kaspi_stores k ON k.id = p.store_id
            WHERE p.id = $1 AND k.user_id = $2
            """,
            uuid.UUID(product_id),
            current_user['id']
        )

        if not product:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Product not found"
            )

        # Get price history
        history = await conn.fetch(
            """
            SELECT id, product_id, old_price, new_price, competitor_price,
                   change_reason, created_at
            FROM price_history
            WHERE product_id = $1
            ORDER BY created_at DESC
            LIMIT $2
            """,
            uuid.UUID(product_id),
            limit
        )

        return {
            "history": [
                {
                    "id": str(h['id']),
                    "product_id": str(h['product_id']),
                    "old_price": h['old_price'],
                    "new_price": h['new_price'],
                    "competitor_price": h['competitor_price'],
                    "change_reason": h['change_reason'],
                    "created_at": h['created_at']
                }
                for h in history
            ]
        }


@router.patch("/products/{product_id}/price")
async def update_product_price(
    product_id: str,
    price_update: dict,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Update product price (frontend compatibility endpoint)"""
    new_price = price_update.get('price')

    if new_price is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Price is required"
        )

    async with pool.acquire() as conn:
        # Verify ownership and get current price
        product = await conn.fetchrow(
            """
            SELECT p.*, k.user_id FROM products p
            JOIN kaspi_stores k ON k.id = p.store_id
            WHERE p.id = $1 AND k.user_id = $2
            """,
            uuid.UUID(product_id),
            current_user['id']
        )

        if not product:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Product not found"
            )

        old_price = product['price']

        # Update price
        updated = await conn.fetchrow(
            """
            UPDATE products
            SET price = $1, updated_at = NOW()
            WHERE id = $2
            RETURNING *
            """,
            new_price,
            uuid.UUID(product_id)
        )

        # Record price change
        await conn.execute(
            """
            INSERT INTO price_history (
                product_id, old_price, new_price, change_reason
            )
            VALUES ($1, $2, $3, 'manual')
            """,
            uuid.UUID(product_id),
            old_price,
            new_price
        )

        return ProductResponse(
            id=str(updated['id']),
            store_id=str(updated['store_id']),
            kaspi_product_id=updated['kaspi_product_id'],
            kaspi_sku=updated['kaspi_sku'],
            external_kaspi_id=updated['external_kaspi_id'],
            name=updated['name'],
            price=updated['price'],
            min_profit=updated['min_profit'],
            bot_active=updated['bot_active'],
            last_check_time=updated['last_check_time'],
            availabilities=updated['availabilities'],
            created_at=updated['created_at'],
            updated_at=updated['updated_at']
        )
