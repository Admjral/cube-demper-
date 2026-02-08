from dataclasses import dataclass
from datetime import datetime
from typing import Optional
import uuid


@dataclass
class WhatsAppSession:
    """WhatsApp Session model - represents WAHA containers (one per user)"""
    id: uuid.UUID
    user_id: uuid.UUID
    waha_container_name: str
    waha_api_key: str
    waha_port: Optional[int]
    session_name: str  # Always 'default' in WAHA Core
    phone_number: Optional[str]
    status: str  # 'qr_pending', 'stopped', 'starting', 'scan_qr_code', 'working', 'failed'
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_row(cls, row):
        """Create WhatsAppSession from database row"""
        return cls(
            id=row['id'],
            user_id=row['user_id'],
            waha_container_name=row['waha_container_name'],
            waha_api_key=row['waha_api_key'],
            waha_port=row.get('waha_port'),
            session_name=row.get('session_name', 'default'),
            phone_number=row.get('phone_number'),
            status=row.get('status', 'qr_pending'),
            created_at=row['created_at'],
            updated_at=row['updated_at']
        )

    def to_dict(self, exclude_api_key: bool = True) -> dict:
        """Convert to dictionary, optionally excluding API key"""
        data = {
            'id': str(self.id),
            'user_id': str(self.user_id),
            'waha_container_name': self.waha_container_name,
            'waha_port': self.waha_port,
            'session_name': self.session_name,
            'phone_number': self.phone_number,
            'status': self.status,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat(),
        }
        if not exclude_api_key:
            data['waha_api_key'] = self.waha_api_key
        return data


@dataclass
class WhatsAppTemplate:
    """WhatsApp Template model - message templates (text only in Core)"""
    id: uuid.UUID
    user_id: uuid.UUID
    name: str
    message: str
    variables: Optional[dict]  # JSON with template variables
    trigger_event: Optional[str]  # 'new_order', 'payment_received', etc.
    is_active: bool
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_row(cls, row):
        """Create WhatsAppTemplate from database row"""
        return cls(
            id=row['id'],
            user_id=row['user_id'],
            name=row['name'],
            message=row['message'],
            variables=row.get('variables'),
            trigger_event=row.get('trigger_event'),
            is_active=row.get('is_active', True),
            created_at=row['created_at'],
            updated_at=row['updated_at']
        )

    def to_dict(self) -> dict:
        """Convert to dictionary"""
        return {
            'id': str(self.id),
            'user_id': str(self.user_id),
            'name': self.name,
            'message': self.message,
            'variables': self.variables,
            'trigger_event': self.trigger_event,
            'is_active': self.is_active,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat(),
        }
