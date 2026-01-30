#!/usr/bin/env python3
"""
Скрипт для создания админ аккаунта.
Запуск: python scripts/create_admin.py

Или с аргументами:
python scripts/create_admin.py --email admin@demper.kz --password your_password
"""

import asyncio
import argparse
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from passlib.context import CryptContext
import asyncpg

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


async def create_admin(email: str, password: str, full_name: str = "Administrator"):
    """Создаёт админ аккаунт в базе данных."""

    # Get DATABASE_URL from environment
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        # Try to load from .env file
        try:
            from dotenv import load_dotenv
            load_dotenv()
            database_url = os.environ.get("DATABASE_URL")
        except ImportError:
            pass

    if not database_url:
        print("❌ DATABASE_URL не найден. Установите переменную окружения или создайте .env файл")
        return False

    # Hash password
    password_hash = pwd_context.hash(password)

    try:
        conn = await asyncpg.connect(database_url)

        # Check if admin already exists
        existing = await conn.fetchrow(
            "SELECT id, email FROM users WHERE email = $1",
            email
        )

        if existing:
            # Update to admin role
            await conn.execute(
                "UPDATE users SET role = 'admin', password_hash = $1 WHERE email = $2",
                password_hash, email
            )
            print(f"✅ Пользователь {email} обновлён до роли admin")
        else:
            # Create new admin
            await conn.execute(
                """
                INSERT INTO users (email, password_hash, full_name, role)
                VALUES ($1, $2, $3, 'admin')
                """,
                email, password_hash, full_name
            )
            print(f"✅ Админ аккаунт создан: {email}")

        await conn.close()
        return True

    except asyncpg.UniqueViolationError:
        print(f"❌ Пользователь с email {email} уже существует")
        return False
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Создание админ аккаунта")
    parser.add_argument("--email", default="admin@demper.kz", help="Email админа")
    parser.add_argument("--password", required=True, help="Пароль админа")
    parser.add_argument("--name", default="Administrator", help="Имя админа")

    args = parser.parse_args()

    if len(args.password) < 6:
        print("❌ Пароль должен быть минимум 6 символов")
        sys.exit(1)

    success = asyncio.run(create_admin(args.email, args.password, args.name))
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
