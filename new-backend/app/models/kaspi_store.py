from dataclasses import dataclass
from datetime import datetime
from typing import Optional
import uuid


@dataclass
class KaspiStore:
    """Kaspi Store model - represents user's Kaspi merchant stores"""
    id: uuid.UUID
    user_id: uuid.UUID
    merchant_id: str
    name: str
    api_key: Optional[str]
    guid: Optional[dict]  # Encrypted session data (GUID, cookies)
    products_count: int
    last_sync: Optional[datetime]
    is_active: bool
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_row(cls, row):
        """Create KaspiStore from database row"""
        return cls(
            id=row['id'],
            user_id=row['user_id'],
            merchant_id=row['merchant_id'],
            name=row['name'],
            api_key=row.get('api_key'),
            guid=row.get('guid'),
            products_count=row.get('products_count', 0),
            last_sync=row.get('last_sync'),
            is_active=row.get('is_active', True),
            created_at=row['created_at'],
            updated_at=row['updated_at']
        )

    def to_dict(self) -> dict:
        """Convert to dictionary"""
        return {
            'id': str(self.id),
            'user_id': str(self.user_id),
            'merchant_id': self.merchant_id,
            'name': self.name,
            'products_count': self.products_count,
            'last_sync': self.last_sync.isoformat() if self.last_sync else None,
            'is_active': self.is_active,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat(),
        }
