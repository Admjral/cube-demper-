"""Database models - dataclasses for representing database records"""

from .user import User
from .kaspi_store import KaspiStore
from .product import Product
from .price_history import PriceHistory
from .preorder import Preorder
from .whatsapp import WhatsAppSession, WhatsAppTemplate
from .ai_chat import AIChatHistory
from .subscription import Subscription
from .payment import Payment
from .partner import Partner
from .niche import NicheCategory, NicheProduct, NicheProductHistory

__all__ = [
    "User",
    "KaspiStore",
    "Product",
    "PriceHistory",
    "Preorder",
    "WhatsAppSession",
    "WhatsAppTemplate",
    "AIChatHistory",
    "Subscription",
    "Payment",
    "Partner",
    "NicheCategory",
    "NicheProduct",
    "NicheProductHistory",
]
