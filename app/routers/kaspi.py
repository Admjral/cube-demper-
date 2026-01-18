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
    DempingSettingsUpdate,
    ProductDempingDetails,
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
    get_active_session_with_refresh,
    KaspiSMSRequiredError,
    KaspiAuthError,
)
from ..services.api_parser import (
    get_products,
    sync_product,
    batch_sync_products,
    parse_product_by_sku,
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

        # Extract already-encrypted GUID from session_data
        encrypted_guid = session_data.get('guid')  # Already encrypted by authenticate_kaspi
        merchant_id = session_data.get('merchant_uid')
        shop_name = session_data.get('shop_name', f"Store {merchant_id}")

        # Store in database (wrap encrypted string in JSON object)
        # Also store email/password separately for auto-reauthentication
        async with pool.acquire() as conn:
            # Try with new columns first, fallback to old schema
            try:
                store = await conn.fetchrow(
                    """
                    INSERT INTO kaspi_stores (user_id, merchant_id, name, guid, kaspi_email, kaspi_password, is_active)
                    VALUES ($1, $2, $3, $4, $5, $6, true)
                    ON CONFLICT (merchant_id)
                    DO UPDATE SET guid = $4, kaspi_email = $5, kaspi_password = $6, is_active = true, needs_reauth = false, reauth_reason = NULL, updated_at = NOW()
                    RETURNING id, merchant_id
                    """,
                    current_user['id'],
                    merchant_id,
                    shop_name,
                    json.dumps({'encrypted': encrypted_guid}),
                    auth_data.email,
                    auth_data.password
                )
            except Exception as e:
                # Fallback for old schema without email/password columns
                logger.warning(f"Could not save credentials (migration may be pending): {e}")
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

            # Extract already-encrypted GUID from complete_session
            encrypted_guid = complete_session.get('guid')  # Already encrypted by verify_sms_code
            store_id = await conn.fetchval(
                """
                UPDATE kaspi_stores
                SET guid = $1, is_active = true, updated_at = NOW()
                WHERE merchant_id = $2 AND user_id = $3
                RETURNING id
                """,
                json.dumps({'encrypted': encrypted_guid}),
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
            logger.info(f"guid_data type: {type(guid_data)}, value: {guid_data}")

            # PostgreSQL may return JSON as string, parse it first
            if isinstance(guid_data, str):
                try:
                    guid_data = json.loads(guid_data)
                    logger.info(f"Parsed JSON string to dict: {type(guid_data)}")
                except json.JSONDecodeError:
                    logger.warning(f"guid_data is not JSON, treating as plain encrypted string")

            if isinstance(guid_data, dict):
                encrypted_guid = guid_data.get('encrypted')
                logger.info(f"Extracted encrypted_guid from dict: {encrypted_guid[:50]}..." if encrypted_guid else "None")
            else:
                # Fallback for old format (plain string)
                encrypted_guid = guid_data
                logger.info(f"Using plain encrypted_guid: {encrypted_guid[:50]}..." if encrypted_guid else "None")

            session = decrypt_session(encrypted_guid)
            logger.info(f"Decrypted session type: {type(session)}, keys: {session.keys() if isinstance(session, dict) else 'NOT A DICT'}")

            # Fetch products from Kaspi API
            products = await get_products(merchant_id, session)

            # Collect all kaspi_product_ids from API response
            new_product_ids = [p['kaspi_product_id'] for p in products]

            # Delete products that are no longer in Kaspi API
            # This ensures we remove products that were deleted or became unavailable
            if new_product_ids:
                deleted_count = await conn.execute(
                    """
                    DELETE FROM products
                    WHERE store_id = $1 AND kaspi_product_id != ALL($2)
                    """,
                    uuid.UUID(store_id),
                    new_product_ids
                )
                logger.info(f"Deleted old products not in current sync: {deleted_count}")
            else:
                # If no products from API, delete all products for this store
                await conn.execute(
                    "DELETE FROM products WHERE store_id = $1",
                    uuid.UUID(store_id)
                )
                logger.info(f"Deleted all products for store {store_id} (no products from API)")

            # Upsert products to database
            for product_data in products:
                # Convert availabilities dict to JSON string for PostgreSQL
                availabilities = product_data.get('availabilities')
                if isinstance(availabilities, dict):
                    availabilities = json.dumps(availabilities)

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
                        kaspi_sku = COALESCE($3, products.kaspi_sku),
                        external_kaspi_id = COALESCE($4, products.external_kaspi_id),
                        updated_at = NOW()
                    """,
                    uuid.UUID(store_id),
                    product_data['kaspi_product_id'],
                    product_data.get('kaspi_sku'),
                    product_data.get('external_kaspi_id'),
                    product_data['name'],
                    product_data['price'],
                    availabilities
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
                availabilities=json.loads(p['availabilities']) if isinstance(p['availabilities'], str) else p['availabilities'],
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

        # New product-level demping fields
        if update_data.max_price is not None:
            param_count += 1
            updates.append(f"max_price = ${param_count}")
            params.append(update_data.max_price)

        if update_data.min_price is not None:
            param_count += 1
            updates.append(f"min_price = ${param_count}")
            params.append(update_data.min_price)

        if update_data.price_step_override is not None:
            param_count += 1
            updates.append(f"price_step_override = ${param_count}")
            params.append(update_data.price_step_override)

        if update_data.demping_strategy is not None:
            param_count += 1
            updates.append(f"demping_strategy = ${param_count}")
            params.append(update_data.demping_strategy)

        if update_data.strategy_params is not None:
            param_count += 1
            updates.append(f"strategy_params = ${param_count}")
            params.append(json.dumps(update_data.strategy_params))

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

        # Parse availabilities if it's a JSON string
        availabilities = updated['availabilities']
        if isinstance(availabilities, str):
            availabilities = json.loads(availabilities)

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
            availabilities=availabilities,
            created_at=updated['created_at'],
            updated_at=updated['updated_at']
        )


@router.get("/products/{product_id}/demping-details", response_model=ProductDempingDetails)
async def get_product_demping_details(
    product_id: str,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Get detailed demping information for a product"""
    async with pool.acquire() as conn:
        # JOIN products + demping_settings + price_history count
        details = await conn.fetchrow(
            """
            SELECT
                p.*,
                COALESCE(ds.price_step, 100) as store_price_step,
                COALESCE(ds.min_margin_percent, 5) as store_min_margin_percent,
                COALESCE(ds.work_hours_start, '09:00') as store_work_hours_start,
                COALESCE(ds.work_hours_end, '21:00') as store_work_hours_end,
                (
                    SELECT COUNT(*)
                    FROM price_history ph
                    WHERE ph.product_id = p.id
                    AND ph.created_at > NOW() - INTERVAL '7 days'
                ) as price_changes_count
            FROM products p
            JOIN kaspi_stores ks ON ks.id = p.store_id
            LEFT JOIN demping_settings ds ON ds.store_id = p.store_id
            WHERE p.id = $1 AND ks.user_id = $2
            """,
            uuid.UUID(product_id),
            current_user['id']
        )

        if not details:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Product not found"
            )

        # Parse strategy_params if it's a string
        strategy_params = details['strategy_params']
        if isinstance(strategy_params, str):
            strategy_params = json.loads(strategy_params)

        return ProductDempingDetails(
            product_id=str(details['id']),
            product_name=details['name'],
            kaspi_sku=details['kaspi_sku'],
            current_price=details['price'],
            min_profit=details['min_profit'],
            bot_active=details['bot_active'],
            max_price=details['max_price'],
            min_price=details['min_price'],
            price_step_override=details['price_step_override'],
            demping_strategy=details['demping_strategy'] or 'standard',
            strategy_params=strategy_params,
            store_price_step=details['store_price_step'],
            store_min_margin_percent=details['store_min_margin_percent'],
            store_work_hours_start=details['store_work_hours_start'],
            store_work_hours_end=details['store_work_hours_end'],
            last_check_time=details['last_check_time'],
            price_changes_count=details['price_changes_count']
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


@router.post("/products/{product_id}/check-demping")
async def check_product_demping(
    product_id: str,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """
    Manually trigger demping check for a specific product.
    Returns current settings without making changes.
    """
    async with pool.acquire() as conn:
        product = await conn.fetchrow(
            """
            SELECT
                p.*,
                ks.merchant_id,
                COALESCE(ds.price_step, 1) as store_price_step,
                COALESCE(ds.min_margin_percent, 5) as store_min_margin_percent
            FROM products p
            JOIN kaspi_stores ks ON ks.id = p.store_id
            LEFT JOIN demping_settings ds ON ds.store_id = p.store_id
            WHERE p.id = $1 AND ks.user_id = $2
            """,
            uuid.UUID(product_id),
            current_user['id']
        )

        if not product:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Product not found"
            )

        current_price = product['price']
        min_price = product['min_price'] or product['min_profit']
        max_price = product['max_price']
        price_step = product['price_step_override'] or product['store_price_step']
        strategy = product['demping_strategy'] or 'standard'

        return {
            "status": "success",
            "product_id": product_id,
            "product_name": product['name'],
            "current_price": current_price,
            "min_price": min_price,
            "max_price": max_price,
            "price_step": price_step,
            "strategy": strategy,
            "bot_active": product['bot_active'],
            "message": "Настройки демпинга получены",
            "last_check_time": product['last_check_time']
        }


@router.post("/products/{product_id}/run-demping")
async def run_product_demping(
    product_id: str,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """
    Manually run demping for a specific product.
    Fetches competitor prices from Kaspi and updates price if needed.
    """
    async with pool.acquire() as conn:
        # Get product with store info
        product = await conn.fetchrow(
            """
            SELECT
                p.*,
                ks.merchant_id,
                ks.guid,
                COALESCE(ds.price_step, 1) as store_price_step
            FROM products p
            JOIN kaspi_stores ks ON ks.id = p.store_id
            LEFT JOIN demping_settings ds ON ds.store_id = p.store_id
            WHERE p.id = $1 AND ks.user_id = $2
            """,
            uuid.UUID(product_id),
            current_user['id']
        )

        if not product:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Product not found"
            )

        external_id = product['external_kaspi_id']
        merchant_id = product['merchant_id']
        current_price = product['price']
        min_price = product['min_price'] or product['min_profit'] or 0
        max_price = product['max_price']
        price_step = product['price_step_override'] or product['store_price_step']
        strategy = product['demping_strategy'] or 'standard'

        # Get session for this store (with auto-refresh if expired)
        session = await get_active_session_with_refresh(merchant_id)
        if not session:
            # Try to check if it needs re-auth (column may not exist if migration pending)
            reason = 'unknown'
            try:
                reauth_info = await conn.fetchrow(
                    "SELECT needs_reauth, reauth_reason FROM kaspi_stores WHERE merchant_id = $1",
                    merchant_id
                )
                if reauth_info:
                    reason = reauth_info.get('reauth_reason') or 'unknown'
            except Exception as e:
                logger.warning(f"Could not check reauth status (migration may be pending): {e}")

            if reason == 'sms_required':
                message = f"Требуется SMS верификация. Перейдите в раздел Магазины для повторной авторизации."
            elif reason == 'invalid_credentials':
                message = f"Неверные учётные данные. Обновите логин/пароль в разделе Магазины."
            elif reason == 'credentials_missing':
                message = f"Учётные данные не сохранены. Переавторизуйтесь в разделе Магазины."
            else:
                message = f"Нет активной сессии для магазина {merchant_id}. Требуется повторная авторизация."

            return {
                "status": "error",
                "message": message,
                "product_id": product_id,
                "current_price": current_price,
                "needs_reauth": True,
                "reauth_reason": reason
            }

        # Fetch competitor prices from Kaspi
        try:
            product_data = await parse_product_by_sku(str(external_id), session)
        except Exception as e:
            logger.error(f"Error fetching competitor prices: {e}")
            return {
                "status": "error",
                "message": f"Ошибка получения цен конкурентов: {str(e)}",
                "product_id": product_id,
                "current_price": current_price
            }

        if not product_data:
            return {
                "status": "no_data",
                "message": "Не удалось получить данные о товаре от Kaspi",
                "product_id": product_id,
                "current_price": current_price
            }

        # Extract offers
        offers = product_data.get("offers", []) if isinstance(product_data, dict) else product_data

        if not offers:
            return {
                "status": "no_offers",
                "message": "Нет предложений конкурентов",
                "product_id": product_id,
                "current_price": current_price,
                "offers_count": 0
            }

        # Find minimum competitor price (excluding our offer)
        min_competitor_price = None
        our_position = None
        sorted_offers = []

        for i, offer in enumerate(offers):
            offer_merchant_id = offer.get("merchantId")
            offer_price = offer.get("price")

            if offer_price is not None:
                sorted_offers.append({
                    "merchant_id": offer_merchant_id,
                    "price": offer_price,
                    "is_ours": offer_merchant_id == merchant_id
                })

                if offer_merchant_id == merchant_id:
                    our_position = i + 1
                elif min_competitor_price is None or offer_price < min_competitor_price:
                    min_competitor_price = offer_price

        # Sort by price
        sorted_offers.sort(key=lambda x: x['price'])

        if min_competitor_price is None:
            return {
                "status": "no_competitors",
                "message": "Вы единственный продавец",
                "product_id": product_id,
                "current_price": current_price,
                "offers": sorted_offers[:5]
            }

        # Calculate target price based on strategy
        if strategy == 'always_first':
            target_price = min_competitor_price - price_step  # На price_step дешевле
        elif strategy == 'stay_top_n':
            top_position = (product['strategy_params'] or {}).get('top_position', 3)
            prices = [o['price'] for o in sorted_offers if not o['is_ours']]
            if len(prices) >= top_position:
                target_price = prices[top_position - 1] - price_step
            else:
                target_price = prices[-1] - price_step if prices else current_price
        else:  # standard
            target_price = min_competitor_price - price_step

        # Apply min/max constraints
        # Если min_price не задана (0), используем минимум 1 тенге
        effective_min_price = min_price if min_price > 0 else 1

        if target_price < effective_min_price:
            # Конкурент ниже нашего минимума
            if current_price > effective_min_price:
                # Мы выше минимума - опускаемся до минимума
                target_price = effective_min_price
            else:
                # Мы уже на минимуме или ниже - ждём повышения конкурента
                return {
                    "status": "waiting",
                    "message": f"Цена конкурента ({min_competitor_price}) ниже нашего минимума ({effective_min_price}). Ждём повышения.",
                    "product_id": product_id,
                    "current_price": current_price,
                    "min_price": effective_min_price,
                    "min_competitor_price": min_competitor_price,
                    "strategy": strategy,
                    "offers": sorted_offers[:5]
                }

        if max_price and target_price > max_price:
            target_price = max_price

        # Check if update is needed
        if target_price == current_price:
            return {
                "status": "no_change",
                "message": f"Изменение не требуется. Целевая цена ({target_price}) = текущей ({current_price})",
                "product_id": product_id,
                "current_price": current_price,
                "target_price": target_price,
                "min_competitor_price": min_competitor_price,
                "strategy": strategy,
                "offers": sorted_offers[:5]
            }

        # Update price via Kaspi API
        try:
            sync_result = await sync_product(
                product_id=str(product['id']),
                new_price=int(target_price),
                session=session
            )

            if not sync_result or not sync_result.get("success"):
                return {
                    "status": "sync_failed",
                    "message": "Не удалось обновить цену в Kaspi",
                    "product_id": product_id,
                    "current_price": current_price,
                    "target_price": target_price
                }

            # Record price change
            await conn.execute(
                """
                INSERT INTO price_history (
                    id, product_id, old_price, new_price,
                    competitor_price, change_reason, created_at
                )
                VALUES (gen_random_uuid(), $1, $2, $3, $4, 'demper', NOW())
                """,
                uuid.UUID(product_id),
                current_price,
                int(target_price),
                int(min_competitor_price)
            )

            # Update product price in DB
            await conn.execute(
                """
                UPDATE products
                SET price = $1, last_check_time = NOW(), updated_at = NOW()
                WHERE id = $2
                """,
                int(target_price),
                uuid.UUID(product_id)
            )

            return {
                "status": "success",
                "message": f"Цена успешно обновлена: {current_price} → {target_price} ₸",
                "product_id": product_id,
                "old_price": current_price,
                "new_price": int(target_price),
                "min_competitor_price": min_competitor_price,
                "strategy": strategy,
                "offers": sorted_offers[:5]
            }

        except Exception as e:
            logger.error(f"Error updating price: {e}")
            return {
                "status": "error",
                "message": f"Ошибка обновления цены: {str(e)}",
                "product_id": product_id,
                "current_price": current_price
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

        # Get or create settings
        settings = await conn.fetchrow(
            "SELECT * FROM demping_settings WHERE store_id = $1",
            uuid.UUID(store_id)
        )

        if not settings:
            # Create default settings
            settings = await conn.fetchrow(
                """
                INSERT INTO demping_settings (store_id)
                VALUES ($1)
                RETURNING *
                """,
                uuid.UUID(store_id)
            )

        return DempingSettings(
            id=str(settings['id']),
            store_id=str(settings['store_id']),
            min_profit=settings['min_profit'],
            bot_active=settings['bot_active'],
            price_step=settings['price_step'],
            min_margin_percent=settings['min_margin_percent'],
            check_interval_minutes=settings['check_interval_minutes'],
            work_hours_start=settings['work_hours_start'],
            work_hours_end=settings['work_hours_end'],
            is_enabled=settings['is_enabled'],
            last_check=settings['last_check'],
            created_at=settings['created_at'],
            updated_at=settings['updated_at']
        )


@router.patch("/stores/{store_id}/demping", response_model=DempingSettings)
async def update_store_demping_settings(
    store_id: str,
    settings_update: DempingSettingsUpdate,
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

        # Build UPDATE query dynamically for only the fields provided
        update_dict = settings_update.dict(exclude_unset=True)

        if not update_dict:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No fields to update"
            )

        # Build SET clause
        set_clauses = []
        values = [uuid.UUID(store_id)]
        param_num = 2

        for field, value in update_dict.items():
            set_clauses.append(f"{field} = ${param_num}")
            values.append(value)
            param_num += 1

        # Try to update existing settings
        query = f"""
            UPDATE demping_settings
            SET {', '.join(set_clauses)}
            WHERE store_id = $1
            RETURNING *
        """

        settings = await conn.fetchrow(query, *values)

        # If no settings exist, create with provided values
        if not settings:
            columns = ['store_id'] + list(update_dict.keys())
            placeholders = ['$1'] + [f'${i+2}' for i in range(len(update_dict))]

            insert_query = f"""
                INSERT INTO demping_settings ({', '.join(columns)})
                VALUES ({', '.join(placeholders)})
                RETURNING *
            """

            settings = await conn.fetchrow(insert_query, uuid.UUID(store_id), *update_dict.values())

        return DempingSettings(
            id=str(settings['id']),
            store_id=str(settings['store_id']),
            min_profit=settings['min_profit'],
            bot_active=settings['bot_active'],
            price_step=settings['price_step'],
            min_margin_percent=settings['min_margin_percent'],
            check_interval_minutes=settings['check_interval_minutes'],
            work_hours_start=settings['work_hours_start'],
            work_hours_end=settings['work_hours_end'],
            is_enabled=settings['is_enabled'],
            last_check=settings['last_check'],
            created_at=settings['created_at'],
            updated_at=settings['updated_at']
        )


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
    """Get sales analytics from orders data"""
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

        days = {'7d': 7, '30d': 30, '90d': 90}[period]
        start_date = datetime.utcnow() - timedelta(days=days)

        # Get daily stats from orders table
        daily_data = await conn.fetch(
            """
            SELECT
                DATE(order_date) as date,
                COUNT(*) as orders,
                COALESCE(SUM(total_price), 0) as revenue,
                COALESCE(SUM(
                    (SELECT COALESCE(SUM(quantity), 0) FROM order_items WHERE order_id = orders.id)
                ), 0) as items
            FROM orders
            WHERE store_id = $1 AND order_date >= $2
            GROUP BY DATE(order_date)
            ORDER BY date ASC
            """,
            uuid.UUID(store_id),
            start_date
        )

        # Create a map of date -> stats
        stats_map = {
            row['date'].strftime('%Y-%m-%d'): {
                'date': row['date'].strftime('%Y-%m-%d'),
                'orders': row['orders'],
                'revenue': row['revenue'],
                'items': row['items']
            }
            for row in daily_data
        }

        # Fill in missing days with zeros
        daily_stats = []
        for i in range(days):
            date = (datetime.utcnow() - timedelta(days=days-i-1)).strftime('%Y-%m-%d')
            if date in stats_map:
                daily_stats.append(stats_map[date])
            else:
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
    """Get top products by sales from orders"""
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

        # Get top products by sales (from order_items)
        products = await conn.fetch(
            """
            SELECT
                COALESCE(p.id, oi.product_id) as id,
                COALESCE(p.kaspi_sku, oi.sku) as kaspi_sku,
                COALESCE(p.name, oi.name) as name,
                COALESCE(p.price, 0) as current_price,
                SUM(oi.quantity) as sales_count,
                SUM(oi.quantity * oi.price) as revenue
            FROM order_items oi
            JOIN orders o ON o.id = oi.order_id
            LEFT JOIN products p ON p.id = oi.product_id
            WHERE o.store_id = $1
            GROUP BY COALESCE(p.id, oi.product_id), COALESCE(p.kaspi_sku, oi.sku),
                     COALESCE(p.name, oi.name), COALESCE(p.price, 0)
            ORDER BY sales_count DESC
            LIMIT $2
            """,
            uuid.UUID(store_id),
            limit
        )

        # If no order data, fallback to products list
        if not products:
            products = await conn.fetch(
                """
                SELECT
                    p.id, p.kaspi_sku, p.name, p.price as current_price,
                    0 as sales_count, 0 as revenue
                FROM products p
                WHERE p.store_id = $1
                ORDER BY p.name
                LIMIT $2
                """,
                uuid.UUID(store_id),
                limit
            )

        return [
            TopProduct(
                id=str(p['id']) if p['id'] else '',
                kaspi_sku=p['kaspi_sku'] or '',
                name=p['name'],
                current_price=p['current_price'],
                sales_count=p['sales_count'] or 0,
                revenue=p['revenue'] or 0
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


@router.post("/stores/{store_id}/sync-orders", status_code=status.HTTP_202_ACCEPTED)
async def sync_store_orders(
    store_id: str,
    background_tasks: BackgroundTasks,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
    days_back: int = 30
):
    """Sync orders for a specific store from Kaspi API"""
    async with pool.acquire() as conn:
        # Verify store ownership and get merchant_id
        store = await conn.fetchrow(
            "SELECT id, merchant_id, guid FROM kaspi_stores WHERE id = $1 AND user_id = $2",
            uuid.UUID(store_id),
            current_user['id']
        )

        if not store:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Store not found"
            )

        if not store['guid']:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Store not authenticated"
            )

    # Add background task to sync orders
    background_tasks.add_task(
        _sync_store_orders_task,
        store_id=store_id,
        merchant_id=store['merchant_id'],
        days_back=days_back
    )

    return {
        "status": "accepted",
        "message": "Orders sync started in background",
        "store_id": store_id
    }


async def _sync_store_orders_task(store_id: str, merchant_id: str, days_back: int = 30):
    """Background task to sync store orders from Kaspi API"""
    from ..services.api_parser import fetch_orders, sync_orders_to_db
    from ..services.kaspi_auth_service import get_active_session_with_refresh

    try:
        logger.info(f"Starting orders sync for store {store_id}, merchant {merchant_id}")

        # Get session with auto-refresh
        session = await get_active_session_with_refresh(merchant_id)
        if not session:
            logger.error(f"No valid session for merchant {merchant_id}")
            return

        # Fetch completed orders from Kaspi
        orders = await fetch_orders(
            merchant_id=merchant_id,
            session=session,
            status="ARCHIVE",  # Completed orders
            days_back=days_back
        )

        if orders:
            # Sync to database
            result = await sync_orders_to_db(store_id, orders)
            logger.info(f"Orders sync complete for store {store_id}: {result}")
        else:
            logger.info(f"No orders found for store {store_id}")

    except Exception as e:
        logger.error(f"Error syncing orders for store {store_id}: {e}")


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
