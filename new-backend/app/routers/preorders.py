"""Preorders router - manages preorder products"""

from fastapi import APIRouter, Depends, HTTPException, status
from typing import Annotated, List
import asyncpg
import uuid

from ..schemas.preorders import (
    PreorderResponse,
    PreorderCreate,
    PreorderUpdate,
    PreorderListResponse,
)
from ..core.database import get_db_pool
from ..dependencies import get_current_user

router = APIRouter()


@router.get("/", response_model=PreorderListResponse)
async def list_preorders(
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
    page: int = 1,
    page_size: int = 50
):
    """List all preorders for current user"""
    offset = (page - 1) * page_size

    async with pool.acquire() as conn:
        # Get total count
        total = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM preorders pr
            JOIN kaspi_stores k ON k.id = pr.store_id
            WHERE k.user_id = $1
            """,
            current_user['id']
        )

        # Get preorders
        preorders = await conn.fetch(
            """
            SELECT pr.id, pr.store_id, pr.product_id, pr.article, pr.name,
                   pr.price, pr.warehouses, pr.delivery_days, pr.status,
                   pr.created_at, pr.updated_at
            FROM preorders pr
            JOIN kaspi_stores k ON k.id = pr.store_id
            WHERE k.user_id = $1
            ORDER BY pr.created_at DESC
            LIMIT $2 OFFSET $3
            """,
            current_user['id'],
            page_size,
            offset
        )

        preorder_responses = [
            PreorderResponse(
                id=str(p['id']),
                store_id=str(p['store_id']),
                product_id=str(p['product_id']),
                article=p['article'],
                name=p['name'],
                price=p['price'],
                warehouses=p['warehouses'],
                delivery_days=p['delivery_days'],
                status=p['status'],
                created_at=p['created_at'],
                updated_at=p['updated_at']
            )
            for p in preorders
        ]

        return PreorderListResponse(
            preorders=preorder_responses,
            total=total,
            page=page,
            page_size=page_size
        )


@router.post("/", response_model=PreorderResponse, status_code=status.HTTP_201_CREATED)
async def create_preorder(
    preorder_data: PreorderCreate,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Create a new preorder"""
    async with pool.acquire() as conn:
        # Verify store ownership
        store = await conn.fetchrow(
            "SELECT id FROM kaspi_stores WHERE id = $1 AND user_id = $2",
            uuid.UUID(preorder_data.store_id),
            current_user['id']
        )

        if not store:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Store not found"
            )

        # Create preorder
        preorder = await conn.fetchrow(
            """
            INSERT INTO preorders (
                store_id, product_id, article, name, price,
                warehouses, delivery_days, status
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, 'added')
            RETURNING *
            """,
            uuid.UUID(preorder_data.store_id),
            uuid.UUID(preorder_data.product_id),
            preorder_data.article,
            preorder_data.name,
            preorder_data.price,
            preorder_data.warehouses,
            preorder_data.delivery_days
        )

        return PreorderResponse(
            id=str(preorder['id']),
            store_id=str(preorder['store_id']),
            product_id=str(preorder['product_id']),
            article=preorder['article'],
            name=preorder['name'],
            price=preorder['price'],
            warehouses=preorder['warehouses'],
            delivery_days=preorder['delivery_days'],
            status=preorder['status'],
            created_at=preorder['created_at'],
            updated_at=preorder['updated_at']
        )


@router.patch("/{preorder_id}", response_model=PreorderResponse)
async def update_preorder(
    preorder_id: str,
    update_data: PreorderUpdate,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Update a preorder"""
    async with pool.acquire() as conn:
        # Verify ownership
        preorder = await conn.fetchrow(
            """
            SELECT pr.* FROM preorders pr
            JOIN kaspi_stores k ON k.id = pr.store_id
            WHERE pr.id = $1 AND k.user_id = $2
            """,
            uuid.UUID(preorder_id),
            current_user['id']
        )

        if not preorder:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Preorder not found"
            )

        # Build update query
        updates = []
        params = []
        param_count = 0

        if update_data.price is not None:
            param_count += 1
            updates.append(f"price = ${param_count}")
            params.append(update_data.price)

        if update_data.warehouses is not None:
            param_count += 1
            updates.append(f"warehouses = ${param_count}")
            params.append(update_data.warehouses)

        if update_data.delivery_days is not None:
            param_count += 1
            updates.append(f"delivery_days = ${param_count}")
            params.append(update_data.delivery_days)

        if update_data.status is not None:
            param_count += 1
            updates.append(f"status = ${param_count}")
            params.append(update_data.status)

        if not updates:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No fields to update"
            )

        param_count += 1
        params.append(uuid.UUID(preorder_id))

        updated = await conn.fetchrow(
            f"""
            UPDATE preorders
            SET {', '.join(updates)}, updated_at = NOW()
            WHERE id = ${param_count}
            RETURNING *
            """,
            *params
        )

        return PreorderResponse(
            id=str(updated['id']),
            store_id=str(updated['store_id']),
            product_id=str(updated['product_id']),
            article=updated['article'],
            name=updated['name'],
            price=updated['price'],
            warehouses=updated['warehouses'],
            delivery_days=updated['delivery_days'],
            status=updated['status'],
            created_at=updated['created_at'],
            updated_at=updated['updated_at']
        )


@router.delete("/{preorder_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_preorder(
    preorder_id: str,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Delete a preorder"""
    async with pool.acquire() as conn:
        result = await conn.execute(
            """
            DELETE FROM preorders pr
            USING kaspi_stores k
            WHERE pr.id = $1 AND pr.store_id = k.id AND k.user_id = $2
            """,
            uuid.UUID(preorder_id),
            current_user['id']
        )

        if result == "DELETE 0":
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Preorder not found"
            )

    return None
