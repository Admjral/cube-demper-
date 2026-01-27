from dataclasses import dataclass
from datetime import datetime
import uuid


@dataclass
class Subscription:
    """Subscription model - represents user subscription plans"""
    id: uuid.UUID
    user_id: uuid.UUID
    plan: str  # 'free', 'basic', 'pro'
    status: str  # 'active', 'expired', 'cancelled'
    products_limit: int
    current_period_start: datetime
    current_period_end: datetime
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_row(cls, row):
        """Create Subscription from database row"""
        return cls(
            id=row['id'],
            user_id=row['user_id'],
            plan=row['plan'],
            status=row.get('status', 'active'),
            products_limit=row['products_limit'],
            current_period_start=row['current_period_start'],
            current_period_end=row['current_period_end'],
            created_at=row['created_at'],
            updated_at=row['updated_at']
        )

    def to_dict(self) -> dict:
        """Convert to dictionary"""
        return {
            'id': str(self.id),
            'user_id': str(self.user_id),
            'plan': self.plan,
            'status': self.status,
            'products_limit': self.products_limit,
            'current_period_start': self.current_period_start.isoformat(),
            'current_period_end': self.current_period_end.isoformat(),
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat(),
        }

    def is_active(self) -> bool:
        """Check if subscription is currently active"""
        return (
            self.status == 'active' and
            self.current_period_start <= datetime.now() <= self.current_period_end
        )
