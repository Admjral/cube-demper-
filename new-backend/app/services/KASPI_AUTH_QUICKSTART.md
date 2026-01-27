# Kaspi Auth Service - Quick Start Guide

## TL;DR

```python
from app.services.kaspi_auth_service import (
    authenticate_kaspi,
    verify_sms_code,
    validate_session,
    KaspiSMSRequiredError
)

# Direct login (no SMS)
result = await authenticate_kaspi("email@example.com", "password")
# → {'merchant_uid': 'X', 'shop_name': 'Y', 'guid': 'encrypted...', 'requires_sms': False}

# Login with SMS
try:
    result = await authenticate_kaspi("email@example.com", "password")
except KaspiSMSRequiredError as e:
    # Get SMS code from user
    result = await verify_sms_code("MERCH123", "123456", e.partial_session)
    # → {'merchant_uid': 'X', 'shop_name': 'Y', 'guid': 'encrypted...'}

# Check if session is valid
is_valid = await validate_session(encrypted_guid)
# → True/False
```

## Common Patterns

### Pattern 1: Login in FastAPI Endpoint

```python
from fastapi import APIRouter, HTTPException
from app.services.kaspi_auth_service import authenticate_kaspi, KaspiSMSRequiredError

@router.post("/kaspi/login")
async def kaspi_login(email: str, password: str):
    try:
        result = await authenticate_kaspi(email, password)
        # Save to database
        return {"success": True, "data": result}
    except KaspiSMSRequiredError as e:
        # Return partial session to client for SMS entry
        return {"requires_sms": True, "partial_session": e.partial_session}
    except Exception as e:
        raise HTTPException(400, detail=str(e))
```

### Pattern 2: Use Existing Session

```python
from app.services.kaspi_auth_service import get_active_session

async def fetch_kaspi_data(merchant_id: str):
    # Get and validate session
    session = await get_active_session(merchant_id)
    if not session:
        raise ValueError("No active session - re-authentication required")

    # Extract cookies
    cookies = {c['name']: c['value'] for c in session['cookies']}

    # Use in API calls
    import httpx
    async with httpx.AsyncClient() as client:
        response = await client.get(
            "https://mc.shop.kaspi.kz/api/endpoint",
            cookies=cookies,
            headers={"x-auth-version": "3"}
        )
        return response.json()
```

### Pattern 3: Session Validation Loop

```python
from app.services.kaspi_auth_service import validate_session, refresh_session

async def ensure_valid_session(merchant_id: str, email: str, password: str):
    # Get current session
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT guid FROM kaspi_stores WHERE merchant_id = $1",
            merchant_id
        )

    if not row or not row['guid']:
        # No session - authenticate
        return await authenticate_kaspi(email, password)

    # Validate existing session
    is_valid = await validate_session(row['guid'])
    if is_valid:
        return row['guid']

    # Refresh expired session
    return await refresh_session(row['guid'], email, password)
```

## Exception Handling

```python
from app.services.kaspi_auth_service import (
    KaspiAuthError,
    KaspiSMSRequiredError,
    KaspiInvalidCredentialsError,
    KaspiSessionExpiredError
)

try:
    result = await authenticate_kaspi(email, password)
except KaspiInvalidCredentialsError:
    # Wrong username/password
    return {"error": "Invalid credentials", "code": "AUTH_INVALID"}
except KaspiSMSRequiredError as e:
    # Need SMS verification
    return {"error": "SMS required", "code": "AUTH_SMS", "data": e.partial_session}
except KaspiAuthError as e:
    # Other auth errors (network, page changes, etc.)
    return {"error": str(e), "code": "AUTH_ERROR"}
```

## Database Schema

```sql
-- kaspi_stores table
CREATE TABLE kaspi_stores (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id),
    merchant_id VARCHAR(255) NOT NULL UNIQUE,
    name VARCHAR(255) NOT NULL,
    guid JSONB,  -- Encrypted session: {"encrypted": "gAAAAAB..."}
    api_key VARCHAR(255),
    products_count INTEGER DEFAULT 0,
    last_sync TIMESTAMP,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Index for GUID lookups
CREATE INDEX idx_kaspi_stores_guid ON kaspi_stores USING GIN (guid);
```

## Environment Variables

```bash
# Required in .env
ENCRYPTION_KEY=your-44-character-fernet-key  # Generate with: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=cube_demper
POSTGRES_USER=postgres
POSTGRES_PASSWORD=yourpassword
```

## API Response Formats

### Success (No SMS)
```json
{
    "merchant_uid": "12345678-abcd-1234-abcd-1234567890ab",
    "shop_name": "My Kaspi Store",
    "guid": "gAAAAABf8kj...",  // Encrypted
    "requires_sms": false
}
```

### SMS Required
```json
{
    "error": "KaspiSMSRequiredError",
    "partial_session": {
        "cookies": [...],
        "email": "merchant@example.com",
        "password": "password123",
        "page_url": "https://idmc.shop.kaspi.kz/verify"
    }
}
```

### Session Validation
```json
{
    "is_valid": true,
    "merchant_uid": "12345678-abcd-1234-abcd-1234567890ab",
    "shop_name": "My Kaspi Store"
}
```

## Testing

```bash
# Unit tests
pytest app/services/test_kaspi_auth.py -v

# Integration test (requires credentials)
export KASPI_TEST_EMAIL=your@email.com
export KASPI_TEST_PASSWORD=yourpassword
pytest app/services/test_kaspi_auth.py::test_full_auth_flow_integration -v

# Single test
pytest app/services/test_kaspi_auth.py::TestKaspiAuthService::test_direct_login_success -v
```

## Debugging

```python
# Enable debug logging
import logging
logging.getLogger('app.services.kaspi_auth_service').setLevel(logging.DEBUG)

# Check session structure
from app.core.security import decrypt_session
session_data = decrypt_session(encrypted_guid)
print(session_data)

# Validate manually
from app.services.kaspi_auth_service import validate_session
is_valid = await validate_session(encrypted_guid)
print(f"Session valid: {is_valid}")
```

## Performance Tips

1. **Reuse browser contexts**: BrowserFarmSharded handles this automatically
2. **Cache sessions**: Store in Redis for faster lookups
3. **Batch validation**: Check multiple sessions in parallel
4. **Background refresh**: Auto-refresh expiring sessions before they expire

```python
# Parallel session validation
import asyncio
from app.services.kaspi_auth_service import validate_session

merchants = ["MERCH1", "MERCH2", "MERCH3"]
guids = [...]  # Get from database

# Validate all in parallel
results = await asyncio.gather(*[validate_session(guid) for guid in guids])
```

## Common Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `Browser launch failed` | Playwright not installed | `playwright install chromium` |
| `Session validation failed: 401` | Cookies expired | Re-authenticate |
| `No merchants found` | Account has no merchants | Check account status |
| `SMS field not detected` | Page structure changed | Update selectors |
| `Invalid ENCRYPTION_KEY` | Wrong key format | Generate new key (44 chars) |

## Migration Checklist

If migrating from Django implementation:

- [ ] Install dependencies: `playwright`, `httpx`, `cryptography`
- [ ] Run `playwright install chromium`
- [ ] Set `ENCRYPTION_KEY` in environment
- [ ] Update database schema (add `guid JSONB` column)
- [ ] Replace Django auth calls with `authenticate_kaspi()`
- [ ] Replace Django ORM with asyncpg queries
- [ ] Update exception handling for new exception types
- [ ] Test both direct login and SMS flows
- [ ] Deploy and monitor logs

## Support

For issues or questions:
1. Check logs: `logging.getLogger('app.services.kaspi_auth_service')`
2. Review README: `app/services/KASPI_AUTH_README.md`
3. Check tests: `app/services/test_kaspi_auth.py`
4. Contact team lead

## Changelog

### v1.0.0 (2024-01-18)
- Initial FastAPI async implementation
- Ported from Django sync version
- Added dual-flow authentication (direct + SMS)
- Integrated BrowserFarmSharded
- Added session encryption with Fernet
- Added session validation and refresh
- Comprehensive test suite
- Full documentation
