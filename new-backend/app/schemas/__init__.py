"""API schemas - Pydantic models for request/response validation"""

from .auth import (
    UserRegister,
    UserLogin,
    Token,
    UserResponse,
    ForgotPasswordRequest,
    ResetPasswordRequest,
)
from .kaspi import (
    KaspiStoreResponse,
    KaspiAuthRequest,
    KaspiAuthSMSRequest,
    StoreSyncRequest,
    ProductUpdateRequest,
    BulkPriceUpdateRequest,
    DempingSettings,
    StoreCreateRequest,
)
from .products import (
    ProductResponse,
    ProductListResponse,
    PriceHistoryResponse,
    ProductFilters,
    ProductAnalytics,
)
from .whatsapp import (
    WhatsAppSessionResponse,
    WhatsAppSessionCreate,
    SendMessageRequest,
    SendPollRequest,
    SendLocationRequest,
    SendContactRequest,
    WhatsAppTemplateCreate,
    WhatsAppTemplateUpdate,
    WhatsAppTemplateResponse,
    WhatsAppWebhook,
)
from .ai import (
    AIChatMessage,
    AIChatRequest,
    AIChatResponse,
    AIChatHistoryResponse,
    AIChatConversation,
    ClearHistoryRequest,
)
from .billing import (
    SubscriptionPlan,
    SubscriptionResponse,
    CreateSubscriptionRequest,
    PaymentResponse,
    CreatePaymentRequest,
    TipTopPayWebhook,
    PaymentListResponse,
)
from .preorders import (
    PreorderResponse,
    PreorderCreate,
    PreorderUpdate,
    PreorderListResponse,
)
from .admin import (
    UserAdminResponse,
    SystemStats,
    UserListResponse,
    UpdateUserRoleRequest,
    DemperWorkerStatus,
)

__all__ = [
    # Auth
    "UserRegister",
    "UserLogin",
    "Token",
    "UserResponse",
    "ForgotPasswordRequest",
    "ResetPasswordRequest",
    # Kaspi
    "KaspiStoreResponse",
    "KaspiAuthRequest",
    "KaspiAuthSMSRequest",
    "StoreSyncRequest",
    "ProductUpdateRequest",
    "BulkPriceUpdateRequest",
    "DempingSettings",
    "StoreCreateRequest",
    # Products
    "ProductResponse",
    "ProductListResponse",
    "PriceHistoryResponse",
    "ProductFilters",
    "ProductAnalytics",
    # WhatsApp
    "WhatsAppSessionResponse",
    "WhatsAppSessionCreate",
    "SendMessageRequest",
    "SendPollRequest",
    "SendLocationRequest",
    "SendContactRequest",
    "WhatsAppTemplateCreate",
    "WhatsAppTemplateUpdate",
    "WhatsAppTemplateResponse",
    "WhatsAppWebhook",
    # AI
    "AIChatMessage",
    "AIChatRequest",
    "AIChatResponse",
    "AIChatHistoryResponse",
    "AIChatConversation",
    "ClearHistoryRequest",
    # Billing
    "SubscriptionPlan",
    "SubscriptionResponse",
    "CreateSubscriptionRequest",
    "PaymentResponse",
    "CreatePaymentRequest",
    "TipTopPayWebhook",
    "PaymentListResponse",
    # Preorders
    "PreorderResponse",
    "PreorderCreate",
    "PreorderUpdate",
    "PreorderListResponse",
    # Admin
    "UserAdminResponse",
    "SystemStats",
    "UserListResponse",
    "UpdateUserRoleRequest",
    "DemperWorkerStatus",
]
