from dataclasses import dataclass
from datetime import datetime
from typing import Optional
import uuid


@dataclass
class Partner:
    """Partner model - represents referral partners"""
    id: uuid.UUID
    email: str
    password_hash: str
    full_name: Optional[str]
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_row(cls, row):
        """Create Partner from database row"""
        return cls(
            id=row['id'],
            email=row['email'],
            password_hash=row['password_hash'],
            full_name=row.get('full_name'),
            created_at=row['created_at'],
            updated_at=row['updated_at']
        )

    def to_dict(self, exclude_password: bool = True) -> dict:
        """Convert to dictionary, optionally excluding password_hash"""
        data = {
            'id': str(self.id),
            'email': self.email,
            'full_name': self.full_name,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat(),
        }
        if not exclude_password:
            data['password_hash'] = self.password_hash
        return data
