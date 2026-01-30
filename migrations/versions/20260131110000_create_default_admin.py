"""Create default admin account

Revision ID: 20260131110000
Revises: 20260131100000
Create Date: 2026-01-31 11:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '20260131110000'
down_revision: Union[str, None] = '20260131100000'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create default admin account
    # Password: admin123
    # Hash generated with: bcrypt.hashpw(b"admin123", bcrypt.gensalt(12))
    # ВАЖНО: Смените пароль после первого входа!
    op.execute("""
        INSERT INTO users (email, password_hash, full_name, role)
        VALUES (
            'admin@demper.kz',
            '$2b$12$rNCLmLqJJCVySGbfHOqwS.s4P6LHuxbLeWvhzOdW8GtRdqfaKEJaS',
            'Administrator',
            'admin'
        )
        ON CONFLICT (email) DO UPDATE SET role = 'admin', password_hash = '$2b$12$rNCLmLqJJCVySGbfHOqwS.s4P6LHuxbLeWvhzOdW8GtRdqfaKEJaS'
    """)


def downgrade() -> None:
    op.execute("DELETE FROM users WHERE email = 'admin@demper.kz' AND role = 'admin'")
