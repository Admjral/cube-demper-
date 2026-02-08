from dataclasses import dataclass
from datetime import datetime
import uuid


@dataclass
class Preorder:
    """Preorder model - represents preorder products"""
    id: uuid.UUID
    store_id: uuid.UUID
    product_id: uuid.UUID
    article: str
    name: str
    price: int  # In tiyns
    warehouses: dict  # JSON with warehouse data
    delivery_days: int
    status: str  # 'added', 'removed'
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_row(cls, row):
        """Create Preorder from database row"""
        return cls(
            id=row['id'],
            store_id=row['store_id'],
            product_id=row['product_id'],
            article=row['article'],
            name=row['name'],
            price=row['price'],
            warehouses=row['warehouses'],
            delivery_days=row.get('delivery_days', 30),
            status=row.get('status', 'added'),
            created_at=row['created_at'],
            updated_at=row['updated_at']
        )

    def to_dict(self) -> dict:
        """Convert to dictionary"""
        return {
            'id': str(self.id),
            'store_id': str(self.store_id),
            'product_id': str(self.product_id),
            'article': self.article,
            'name': self.name,
            'price': self.price,
            'warehouses': self.warehouses,
            'delivery_days': self.delivery_days,
            'status': self.status,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat(),
        }
