"""Services - business logic layer"""

from .kaspi_auth_service import (
    authenticate_kaspi,
    verify_sms_code,
    get_active_session,
    validate_session,
    refresh_session,
    KaspiAuthError,
    KaspiSMSRequiredError,
    KaspiInvalidCredentialsError,
    KaspiSessionExpiredError,
)
from .api_parser import (
    get_products,
    parse_product_by_sku,
    sync_product,
    get_competitor_price,
    fetch_preorders,
    batch_sync_products,
    validate_merchant_session,
    get_merchant_session,
)
from .waha_service import (
    WahaService,
    WahaConfig,
    WahaError,
    WahaConnectionError,
    WahaSessionError,
    WahaMessageError,
    WahaSessionStatus,
    get_waha_service,
)
from .ai_salesman_service import (
    AISalesmanService,
    SalesmanTrigger,
    OrderContext,
    SalesmanMessage,
    process_order_for_upsell,
    get_ai_salesman,
)

__all__ = [
    # Kaspi Auth
    "authenticate_kaspi",
    "verify_sms_code",
    "get_active_session",
    "validate_session",
    "refresh_session",
    "KaspiAuthError",
    "KaspiSMSRequiredError",
    "KaspiInvalidCredentialsError",
    "KaspiSessionExpiredError",
    # API Parser
    "get_products",
    "parse_product_by_sku",
    "sync_product",
    "get_competitor_price",
    "fetch_preorders",
    "batch_sync_products",
    "validate_merchant_session",
    "get_merchant_session",
    # WAHA
    "WahaService",
    "WahaConfig",
    "WahaError",
    "WahaConnectionError",
    "WahaSessionError",
    "WahaMessageError",
    "WahaSessionStatus",
    "get_waha_service",
    # AI Salesman
    "AISalesmanService",
    "SalesmanTrigger",
    "OrderContext",
    "SalesmanMessage",
    "process_order_for_upsell",
    "get_ai_salesman",
]
