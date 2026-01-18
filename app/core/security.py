from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any
import uuid

from jose import JWTError, jwt
from passlib.context import CryptContext
from cryptography.fernet import Fernet
import json

from ..config import settings

# Password hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Session encryption - validate Fernet key
def _get_cipher():
    """Get Fernet cipher with validation"""
    key = settings.encryption_key.encode()
    if len(key) != 44:
        raise ValueError(
            f"ENCRYPTION_KEY must be exactly 44 characters (got {len(key)}). "
            "Generate a valid key with: python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\""
        )
    try:
        return Fernet(key)
    except Exception as e:
        raise ValueError(f"Invalid ENCRYPTION_KEY format: {e}")

cipher = _get_cipher()


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify password against hash"""
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password: str) -> str:
    """Hash password"""
    return pwd_context.hash(password)


def create_access_token(user_id: uuid.UUID, role: str, expires_delta: Optional[timedelta] = None) -> str:
    """Create JWT access token"""
    now = datetime.now(timezone.utc)
    if expires_delta:
        expire = now + expires_delta
    else:
        expire = now + timedelta(hours=settings.access_token_expire_hours)

    payload = {
        "sub": str(user_id),
        "role": role,
        "exp": expire,
        "iat": now,
    }

    encoded_jwt = jwt.encode(payload, settings.secret_key, algorithm=settings.jwt_algorithm)
    return encoded_jwt


def decode_access_token(token: str) -> Optional[Dict[str, Any]]:
    """Decode and verify JWT token"""
    try:
        payload = jwt.decode(token, settings.secret_key, algorithms=[settings.jwt_algorithm])
        return payload
    except JWTError:
        return None


def encrypt_session(session_data: dict) -> str:
    """Encrypt Kaspi session data (GUID, cookies, etc.)"""
    json_str = json.dumps(session_data)
    encrypted = cipher.encrypt(json_str.encode())
    return encrypted.decode()


def decrypt_session(encrypted_data: str) -> dict:
    """Decrypt Kaspi session data"""
    import logging
    logger = logging.getLogger(__name__)

    try:
        if not encrypted_data:
            logger.warning("decrypt_session: empty encrypted_data")
            return {}
        decrypted = cipher.decrypt(encrypted_data.encode())
        return json.loads(decrypted.decode())
    except Exception as e:
        logger.error(f"decrypt_session failed: {type(e).__name__}: {e}")
        logger.debug(f"encrypted_data (first 50 chars): {encrypted_data[:50] if encrypted_data else 'None'}...")
        return {}


def generate_api_key() -> str:
    """Generate secure API key for WAHA containers"""
    import secrets
    return secrets.token_urlsafe(32)
