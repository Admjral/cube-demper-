from dataclasses import dataclass
from datetime import datetime
import uuid


@dataclass
class AIChatHistory:
    """AI Chat History model - stores conversation history with AI assistants"""
    id: uuid.UUID
    user_id: uuid.UUID
    assistant_type: str  # 'lawyer', 'salesman'
    role: str  # 'user' or 'assistant'
    content: str
    created_at: datetime

    @classmethod
    def from_row(cls, row):
        """Create AIChatHistory from database row"""
        return cls(
            id=row['id'],
            user_id=row['user_id'],
            assistant_type=row['assistant_type'],
            role=row['role'],
            content=row['content'],
            created_at=row['created_at']
        )

    def to_dict(self) -> dict:
        """Convert to dictionary"""
        return {
            'id': str(self.id),
            'user_id': str(self.user_id),
            'assistant_type': self.assistant_type,
            'role': self.role,
            'content': self.content,
            'created_at': self.created_at.isoformat(),
        }
