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
from .kaspi_mc_service import (
    KaspiMCService,
    KaspiMCError,
    get_kaspi_mc_service,
)
from .order_event_processor import (
    OrderEventProcessor,
    OrderEvent,
    get_order_event_processor,
    process_new_kaspi_order,
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
    # Kaspi MC
    "KaspiMCService",
    "KaspiMCError",
    "get_kaspi_mc_service",
    # Order Event Processor
    "OrderEventProcessor",
    "OrderEvent",
    "get_order_event_processor",
    "process_new_kaspi_order",
]
