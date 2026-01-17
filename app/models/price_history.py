from dataclasses import dataclass
from datetime import datetime
from typing import Optional
import uuid


@dataclass
class PriceHistory:
    """Price History model - tracks price changes"""
    id: uuid.UUID
    product_id: uuid.UUID
    old_price: int  # In tiyns
    new_price: int  # In tiyns
    competitor_price: Optional[int]  # In tiyns
    change_reason: str  # 'manual', 'demper', 'competitor'
    created_at: datetime

    @classmethod
    def from_row(cls, row):
        """Create PriceHistory from database row"""
        return cls(
            id=row['id'],
            product_id=row['product_id'],
            old_price=row['old_price'],
            new_price=row['new_price'],
            competitor_price=row.get('competitor_price'),
            change_reason=row.get('change_reason', 'manual'),
            created_at=row['created_at']
        )

    def to_dict(self) -> dict:
        """Convert to dictionary"""
        return {
            'id': str(self.id),
            'product_id': str(self.product_id),
            'old_price': self.old_price,
            'new_price': self.new_price,
            'competitor_price': self.competitor_price,
            'change_reason': self.change_reason,
            'created_at': self.created_at.isoformat(),
        }
