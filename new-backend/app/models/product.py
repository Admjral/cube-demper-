from dataclasses import dataclass
from datetime import datetime
from typing import Optional
import uuid


@dataclass
class Product:
    """Product model - represents products in Kaspi stores

    Цены хранятся в ТЕНГЕ (целые числа).
    Пример: 199 KZT = 199
    """
    id: uuid.UUID
    store_id: uuid.UUID
    kaspi_product_id: str
    kaspi_sku: Optional[str]
    external_kaspi_id: Optional[str]
    name: str
    price: int  # В тенге (KZT)
    min_profit: int  # В тенге (KZT)
    bot_active: bool
    last_check_time: Optional[datetime]
    availabilities: Optional[dict]  # JSON with warehouse availabilities
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_row(cls, row):
        """Create Product from database row"""
        return cls(
            id=row['id'],
            store_id=row['store_id'],
            kaspi_product_id=row['kaspi_product_id'],
            kaspi_sku=row.get('kaspi_sku'),
            external_kaspi_id=row.get('external_kaspi_id'),
            name=row['name'],
            price=row['price'],
            min_profit=row.get('min_profit', 0),
            bot_active=row.get('bot_active', True),
            last_check_time=row.get('last_check_time'),
            availabilities=row.get('availabilities'),
            created_at=row['created_at'],
            updated_at=row['updated_at']
        )

    def to_dict(self) -> dict:
        """Convert to dictionary"""
        return {
            'id': str(self.id),
            'store_id': str(self.store_id),
            'kaspi_product_id': self.kaspi_product_id,
            'kaspi_sku': self.kaspi_sku,
            'external_kaspi_id': self.external_kaspi_id,
            'name': self.name,
            'price': self.price,
            'min_profit': self.min_profit,
            'bot_active': self.bot_active,
            'last_check_time': self.last_check_time.isoformat() if self.last_check_time else None,
            'availabilities': self.availabilities,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat(),
        }

    @property
    def price_kzt(self) -> int:
        """Get price in KZT (цена уже в тенге)"""
        return self.price

    @property
    def min_profit_kzt(self) -> int:
        """Get min profit in KZT (цена уже в тенге)"""
        return self.min_profit
