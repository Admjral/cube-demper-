# Kaspi Authentication Service

FastAPI async implementation of Kaspi merchant authentication with dual-flow support and encrypted session management.

## Overview

This service handles authentication with Kaspi's merchant platform, supporting both direct login and SMS-verified login flows. It uses Playwright for browser automation, encrypts session data, and integrates with the BrowserFarmSharded for efficient resource management.

## Files

- **`kaspi_auth_service.py`** - Main service implementation
- **`kaspi_auth_usage_example.py`** - FastAPI endpoint integration examples
- **`test_kaspi_auth.py`** - Comprehensive test suite
- **`KASPI_AUTH_README.md`** - This documentation

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                   FastAPI Endpoint Layer                     │
│  /api/kaspi/auth/login, /verify-sms, /session/{id}          │
└────────────────────┬────────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────────┐
│              Kaspi Auth Service Layer                        │
│  • authenticate_kaspi()                                      │
│  • verify_sms_code()                                         │
│  • validate_session()                                        │
│  • get_active_session()                                      │
└────────┬───────────────────────────┬────────────────────────┘
         │                           │
┌────────▼──────────────┐   ┌────────▼──────────────┐
│  BrowserFarmSharded   │   │   Security Module     │
│  • Playwright mgmt    │   │   • Encryption        │
│  • Context pooling    │   │   • Decryption        │
│  • Rate limiting      │   │   • Fernet cipher     │
└───────────────────────┘   └───────────────────────┘
         │
┌────────▼──────────────────────────────────────────┐
│              Database (asyncpg)                    │
│  kaspi_stores.guid JSONB (encrypted session)      │
└───────────────────────────────────────────────────┘
```

## Authentication Flows

### Flow 1: Direct Login (No SMS)

```python
# Client calls authenticate_kaspi()
result = await authenticate_kaspi(email, password)

# Returns immediately with:
{
    'merchant_uid': 'MERCH123',
    'shop_name': 'My Store',
    'guid': 'encrypted_session_data',
    'requires_sms': False
}
```

**Sequence:**
1. Create browser page via BrowserFarmSharded
2. Navigate to Kaspi login page
3. Enter email → Submit
4. Enter email + password → Submit
5. Wait for navbar (successful login indicator)
6. Extract cookies
7. Fetch merchant info via API
8. Encrypt session data (cookies + credentials)
9. Return complete session

### Flow 2: Login with SMS Verification

```python
# Step 1: Initial authentication
try:
    result = await authenticate_kaspi(email, password)
except KaspiSMSRequiredError as e:
    # SMS verification required
    partial_session = e.partial_session
    # Store partial_session for step 2

# Step 2: User receives SMS and enters code
result = await verify_sms_code(
    merchant_id='MERCH123',
    sms_code='123456',
    partial_session=partial_session
)

# Returns complete session:
{
    'merchant_uid': 'MERCH123',
    'shop_name': 'My Store',
    'guid': 'encrypted_session_data'
}
```

**Sequence:**
1. Same as Flow 1 steps 1-4
2. Detect SMS code field on page
3. Raise `KaspiSMSRequiredError` with partial session
4. Client stores `partial_session` and prompts user for SMS
5. Client calls `verify_sms_code()` with user input
6. Service restores browser context with cookies
7. Enter SMS code → Submit
8. Wait for navbar (successful verification)
9. Fetch merchant info
10. Encrypt complete session data
11. Update database
12. Return complete session

## Key Components

### 1. Main Functions

#### `authenticate_kaspi(email, password, merchant_id=None)`

Initiates authentication with Kaspi.

**Parameters:**
- `email` (str): Kaspi account email
- `password` (str): Kaspi account password
- `merchant_id` (str, optional): Merchant ID for database updates

**Returns:**
- `dict`: Complete session data

**Raises:**
- `KaspiSMSRequiredError`: When SMS verification is needed (contains `partial_session`)
- `KaspiInvalidCredentialsError`: When credentials are invalid
- `KaspiAuthError`: For other authentication errors

#### `verify_sms_code(merchant_id, sms_code, partial_session)`

Completes authentication after SMS verification.

**Parameters:**
- `merchant_id` (str): Merchant ID
- `sms_code` (str): SMS verification code
- `partial_session` (dict): Partial session from `KaspiSMSRequiredError`

**Returns:**
- `dict`: Complete session data

#### `validate_session(guid)`

Validates if a session is still active.

**Parameters:**
- `guid` (dict|str): Session data (encrypted or decrypted)

**Returns:**
- `bool`: True if valid, False if expired/invalid

#### `get_active_session(merchant_id)`

Retrieves and validates session from database.

**Parameters:**
- `merchant_id` (str): Merchant ID

**Returns:**
- `dict|None`: Decrypted session data or None

### 2. Session Data Structure

**GUID (Session Data):**
```python
{
    'cookies': [
        {
            'name': 'cookie_name',
            'value': 'cookie_value',
            'domain': '.kaspi.kz',
            'path': '/',
            'expires': 1234567890,
            # ... other cookie fields
        },
        # ... more cookies
    ],
    'email': 'merchant@example.com',
    'password': 'encrypted_password',  # Stored for auto-refresh
    'merchant_uid': 'MERCH123',
    'authenticated_at': '2024-01-18T12:00:00',
    'sms_verified': True  # If SMS was used
}
```

This data is encrypted using Fernet (symmetric encryption) before storage:

```python
from app.core.security import encrypt_session, decrypt_session

# Encrypt before storage
encrypted_guid = encrypt_session(session_data)

# Decrypt when needed
session_data = decrypt_session(encrypted_guid)
```

**Database Storage:**
```sql
-- kaspi_stores.guid column (JSONB)
{
    "encrypted": "gAAAAABf8k..."  -- Fernet encrypted string
}
```

### 3. Exception Hierarchy

```
KaspiAuthError (Base)
├── KaspiSMSRequiredError
│   └── Contains: partial_session
├── KaspiInvalidCredentialsError
└── KaspiSessionExpiredError
```

### 4. Browser Management

The service uses `BrowserFarmSharded` for efficient browser context management:

```python
from app.core.browser_farm import get_browser_farm

browser_farm = await get_browser_farm()
shard = browser_farm.shards[0]  # Use first shard for auth
await shard.initialize()

context = await shard.get_context()
page = await context.new_page()

# ... perform authentication ...

await page.close()  # Context remains pooled
```

**Benefits:**
- Reuses browser contexts (faster subsequent auths)
- Automatic garbage collection of idle contexts
- Rate limiting at browser farm level
- Consistent user-agent and locale settings

## Integration Examples

### FastAPI Endpoint

```python
from fastapi import APIRouter, HTTPException
from .kaspi_auth_service import authenticate_kaspi, KaspiSMSRequiredError

router = APIRouter()

@router.post("/kaspi/login")
async def kaspi_login(email: str, password: str):
    try:
        result = await authenticate_kaspi(email, password)
        return {
            "success": True,
            "merchant_uid": result['merchant_uid'],
            "shop_name": result['shop_name']
        }
    except KaspiSMSRequiredError as e:
        return {
            "success": False,
            "requires_sms": True,
            "partial_session": e.partial_session
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
```

### Background Task (Session Validation)

```python
import asyncio
from .kaspi_auth_service import get_active_session, validate_session

async def validate_all_sessions():
    """Background task to validate all merchant sessions"""
    pool = await get_db_pool()

    async with pool.acquire() as conn:
        merchants = await conn.fetch(
            "SELECT merchant_id FROM kaspi_stores WHERE is_active = true"
        )

        for merchant in merchants:
            session = await get_active_session(merchant['merchant_id'])

            if session:
                is_valid = await validate_session(session)
                if not is_valid:
                    logger.warning(f"Session expired for {merchant['merchant_id']}")
                    # Mark for re-authentication
```

### Sync Service Integration

```python
from .kaspi_auth_service import get_active_session

async def parse_kaspi_orders(merchant_id: str):
    """Parse orders using authenticated session"""

    # Get active session
    session = await get_active_session(merchant_id)
    if not session:
        raise ValueError(f"No active session for {merchant_id}")

    # Use session cookies
    cookies = {c['name']: c['value'] for c in session['cookies']}

    # Make authenticated API calls
    import httpx
    async with httpx.AsyncClient() as client:
        response = await client.get(
            "https://mc.shop.kaspi.kz/api/orders",
            cookies=cookies,
            headers={
                "x-auth-version": "3",
                "User-Agent": "Mozilla/5.0..."
            }
        )

        return response.json()
```

## Security Considerations

### 1. Encryption

Session data is encrypted using Fernet (symmetric encryption):

- **Algorithm**: AES-128 in CBC mode with PKCS7 padding
- **Key**: 44-character base64-encoded key from `ENCRYPTION_KEY` env var
- **Format**: Encrypted data includes timestamp and signature

Generate a key:
```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### 2. Password Storage

Passwords are stored in the GUID for automatic session refresh. This is encrypted at rest.

**Alternatives:**
1. Don't store passwords (require manual re-auth)
2. Use refresh tokens if Kaspi provides them
3. Implement password vault with separate encryption

### 3. Database Security

The `guid` column uses JSONB with encrypted payload:

```sql
-- Migration
ALTER TABLE kaspi_stores
ADD COLUMN guid JSONB;

-- Index for faster lookups
CREATE INDEX idx_kaspi_stores_guid
ON kaspi_stores USING GIN (guid);
```

### 4. Session Validation

Sessions are validated by making test API calls:

```python
async def validate_session(guid: dict) -> bool:
    # Decrypt session
    # Make API call to Kaspi
    # Check if response is 200 with valid merchants
    # Return True/False
```

This ensures cookies haven't expired and credentials are still valid.

## Error Handling

### 1. Authentication Errors

```python
try:
    result = await authenticate_kaspi(email, password)
except KaspiInvalidCredentialsError:
    # Wrong email/password
    return {"error": "Invalid credentials"}
except KaspiSMSRequiredError as e:
    # SMS verification needed
    return {
        "requires_sms": True,
        "partial_session": e.partial_session
    }
except KaspiAuthError as e:
    # Generic auth error (network, page changes, etc.)
    return {"error": str(e)}
```

### 2. SMS Verification Errors

```python
try:
    result = await verify_sms_code(merchant_id, sms_code, partial_session)
except KaspiAuthError as e:
    # Invalid SMS code or expired session
    return {"error": f"SMS verification failed: {e}"}
```

### 3. Session Validation Errors

```python
is_valid = await validate_session(guid)
if not is_valid:
    # Session expired - need re-authentication
    # Trigger re-auth flow
    pass
```

## Testing

### Unit Tests

```bash
# Run unit tests
pytest app/services/test_kaspi_auth.py -v

# Run specific test
pytest app/services/test_kaspi_auth.py::TestKaspiAuthService::test_direct_login_success -v
```

### Integration Tests

```bash
# Set credentials
export KASPI_TEST_EMAIL=your@email.com
export KASPI_TEST_PASSWORD=yourpassword

# Run integration test
pytest app/services/test_kaspi_auth.py::test_full_auth_flow_integration -v
```

### Manual Testing

```python
# In Python REPL or Jupyter notebook
import asyncio
from app.services.kaspi_auth_service import authenticate_kaspi

async def test():
    result = await authenticate_kaspi(
        email="your@email.com",
        password="yourpassword"
    )
    print(result)

asyncio.run(test())
```

## Performance Optimization

### 1. Browser Context Pooling

BrowserFarmSharded reuses browser contexts:
- First auth: ~5-10 seconds (browser launch + auth)
- Subsequent auths: ~2-3 seconds (reuse context)

### 2. Rate Limiting

Global rate limiter prevents overwhelming Kaspi servers:
```python
# In browser_farm.py
rate_limiter = get_global_rate_limiter()
await rate_limiter.acquire()
```

### 3. Database Connection Pooling

asyncpg pool configuration:
```python
# In config.py
db_pool_min_size = 5
db_pool_max_size = 20
```

## Migration from Django

### Changes from Django Implementation

1. **Async/Await**: All functions are `async def`
2. **Playwright Import**: `from playwright.async_api import ...`
3. **Browser Management**: Use `BrowserFarmSharded` instead of direct playwright
4. **Database**: asyncpg instead of Django ORM
5. **Encryption**: Explicit encryption functions from `app.core.security`
6. **Error Handling**: Custom exception hierarchy
7. **SMS Support**: Added dual-flow authentication
8. **Session Validation**: Added validation and refresh functions

### Django Code (Old)

```python
def login_and_get_merchant_info(email, password, user_id):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()
        # ... rest of code
```

### FastAPI Code (New)

```python
async def authenticate_kaspi(email, password, merchant_id=None):
    browser_farm = await get_browser_farm()
    shard = browser_farm.shards[0]
    await shard.initialize()
    context = await shard.get_context()
    page = await context.new_page()
    # ... rest of code
```

## Troubleshooting

### Common Issues

#### 1. Browser Launch Fails

```
Error: Failed to initialize browser shard
```

**Solution:**
- Install Playwright browsers: `playwright install chromium`
- Check system dependencies: `playwright install-deps`

#### 2. Session Validation Always Returns False

```
Warning: Session validation failed: status=401
```

**Solution:**
- Check if cookies have expired (Kaspi sessions ~24 hours)
- Verify encryption key is correct
- Re-authenticate to get fresh session

#### 3. SMS Code Field Not Detected

```
Error: SMS verification required but field not found
```

**Solution:**
- Kaspi may have changed page structure
- Update selectors in `_login_to_kaspi_page()`
- Check browser screenshots for debugging

#### 4. Merchant Info Fetch Fails

```
Error: No merchants found in response
```

**Solution:**
- Account may have no associated merchants
- Check API response structure (may have changed)
- Verify cookies are being sent correctly

### Debug Mode

Enable debug logging:

```python
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('app.services.kaspi_auth_service')
logger.setLevel(logging.DEBUG)
```

### Browser Screenshots

Add screenshot capture for debugging:

```python
# In kaspi_auth_service.py
await page.screenshot(path=f'/tmp/kaspi_login_{datetime.now().timestamp()}.png')
```

## Future Enhancements

1. **Captcha Handling**: Detect and handle captcha challenges
2. **Proxy Support**: Rotate IPs for rate limit avoidance
3. **Multi-Account**: Support multiple Kaspi accounts per user
4. **Session Pooling**: Pre-authenticated session pool
5. **Webhook Support**: Kaspi webhook verification
6. **Analytics**: Track authentication success rates
7. **Auto-Refresh**: Background task to refresh expiring sessions
8. **2FA Support**: Handle additional authentication methods

## License

Internal use only - Cube Demper project.
