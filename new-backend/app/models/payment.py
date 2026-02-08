from dataclasses import dataclass
from datetime import datetime
from typing import Optional
import uuid


@dataclass
class Payment:
    """Payment model - represents payment transactions

    Суммы хранятся в ТЕНГЕ (целые числа).
    """
    id: uuid.UUID
    user_id: uuid.UUID
    amount: int  # В тенге (KZT)
    status: str  # 'pending', 'completed', 'failed', 'refunded'
    plan: Optional[str]  # Associated subscription plan
    tiptoppay_transaction_id: Optional[str]
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_row(cls, row):
        """Create Payment from database row"""
        return cls(
            id=row['id'],
            user_id=row['user_id'],
            amount=row['amount'],
            status=row.get('status', 'pending'),
            plan=row.get('plan'),
            tiptoppay_transaction_id=row.get('tiptoppay_transaction_id'),
            created_at=row['created_at'],
            updated_at=row['updated_at']
        )

    def to_dict(self) -> dict:
        """Convert to dictionary"""
        return {
            'id': str(self.id),
            'user_id': str(self.user_id),
            'amount': self.amount,
            'status': self.status,
            'plan': self.plan,
            'tiptoppay_transaction_id': self.tiptoppay_transaction_id,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat(),
        }

    @property
    def amount_kzt(self) -> int:
        """Get amount in KZT (сумма уже в тенге)"""
        return self.amount
