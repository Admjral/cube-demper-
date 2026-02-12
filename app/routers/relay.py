"""
Relay endpoints — used by VPS backend to proxy requests through Railway
to bypass Kaspi IP blocks on datacenter IPs.
"""
import re
import httpx
import logging
from fastapi import APIRouter, HTTPException, Header, status
from pydantic import BaseModel
from typing import Optional

from ..config import settings

logger = logging.getLogger(__name__)
router = APIRouter()


class RelayParseUrlRequest(BaseModel):
    url: str


class RelayParseUrlResponse(BaseModel):
    html: str
    status_code: int


@router.post("/parse-url", response_model=RelayParseUrlResponse)
async def relay_parse_url(
    request: RelayParseUrlRequest,
    authorization: str = Header(...),
):
    """
    Relay endpoint: fetch a Kaspi URL and return the HTML.
    Used by VPS backend for unit economics parsing.
    Protected by shared secret.
    """
    # Verify shared secret
    expected = f"Bearer {settings.offers_relay_secret}"
    if not settings.offers_relay_secret or authorization != expected:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid relay secret")

    # Validate URL is kaspi.kz
    if "kaspi.kz" not in request.url:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Only kaspi.kz URLs allowed")

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
            }
            response = await client.get(request.url, headers=headers, follow_redirects=True)
            return RelayParseUrlResponse(html=response.text, status_code=response.status_code)
    except httpx.TimeoutException:
        raise HTTPException(status_code=status.HTTP_504_GATEWAY_TIMEOUT, detail="Upstream timeout")
    except Exception as e:
        logger.error(f"Relay parse-url error: {e}")
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(e))


class RelayOffersRequest(BaseModel):
    product_id: str
    city_id: str


@router.post("/offers")
async def relay_offers(
    request: RelayOffersRequest,
    authorization: str = Header(...),
):
    """
    Relay endpoint: fetch Kaspi offers for a product.
    Used by VPS backend for demper worker.
    """
    expected = f"Bearer {settings.offers_relay_secret}"
    if not settings.offers_relay_secret or authorization != expected:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid relay secret")

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            url = f"https://kaspi.kz/yml/offer-view/offers/{request.product_id}"
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                "Accept": "application/json",
                "Accept-Language": "ru-RU,ru;q=0.9",
                "X-KS-City": request.city_id,
            }
            response = await client.get(url, headers=headers)

            if response.status_code == 403:
                raise HTTPException(status_code=403, detail="Kaspi returned 403 (IP ban)")

            response.raise_for_status()
            return response.json()
    except HTTPException:
        raise
    except httpx.TimeoutException:
        raise HTTPException(status_code=status.HTTP_504_GATEWAY_TIMEOUT, detail="Upstream timeout")
    except Exception as e:
        logger.error(f"Relay offers error: {e}")
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(e))
