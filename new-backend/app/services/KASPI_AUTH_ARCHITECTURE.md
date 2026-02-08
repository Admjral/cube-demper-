# Kaspi Auth Service - Architecture Diagram

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          Client Application                              │
│  (React/Vue/Mobile App)                                                  │
└────────────────────────────┬────────────────────────────────────────────┘
                             │ HTTPS/JSON
                             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                          FastAPI Application                             │
│                                                                          │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │              Endpoint Layer (kaspi_auth_complete_example.py)       │ │
│  │                                                                    │ │
│  │  POST /api/v1/kaspi/stores/link                                   │ │
│  │  POST /api/v1/kaspi/stores/verify-sms                             │ │
│  │  GET  /api/v1/kaspi/stores                                        │ │
│  │  GET  /api/v1/kaspi/stores/{id}/orders                            │ │
│  │  DELETE /api/v1/kaspi/stores/{id}                                 │ │
│  └──────────────────────────┬─────────────────────────────────────────┘ │
│                             │                                            │
│  ┌──────────────────────────▼─────────────────────────────────────────┐ │
│  │           Service Layer (kaspi_auth_service.py)                    │ │
│  │                                                                    │ │
│  │  • authenticate_kaspi()      → Initial authentication             │ │
│  │  • verify_sms_code()         → SMS verification                   │ │
│  │  • validate_session()        → Session validation                 │ │
│  │  • refresh_session()         → Session refresh                    │ │
│  │  • get_active_session()      → Retrieve from DB                   │ │
│  └────┬────────────────────┬────────────────────┬────────────────────┘ │
│       │                    │                    │                       │
└───────┼────────────────────┼────────────────────┼───────────────────────┘
        │                    │                    │
        │                    │                    │
┌───────▼──────────┐  ┌──────▼──────────┐  ┌─────▼─────────────┐
│                  │  │                 │  │                   │
│  BrowserFarm     │  │    Security     │  │    Database       │
│  Sharded         │  │    Module       │  │    (asyncpg)      │
│                  │  │                 │  │                   │
│ ┌──────────────┐ │  │  • Fernet      │  │  ┌──────────────┐ │
│ │ Shard 0      │ │  │    encryption  │  │  │ kaspi_stores │ │
│ │ ┌──────────┐ │ │  │  • AES-128     │  │  │              │ │
│ │ │ Context1 │ │ │  │  • HMAC        │  │  │ - id         │ │
│ │ │ Context2 │ │ │  │    signature   │  │  │ - user_id    │ │
│ │ │ Context3 │ │ │  │                │  │  │ - merchant_id│ │
│ │ └──────────┘ │ │  │  encrypt_      │  │  │ - name       │ │
│ │              │ │  │  session()     │  │  │ - guid JSONB │ │
│ ├──────────────┤ │  │                │  │  │ - is_active  │ │
│ │ Shard 1      │ │  │  decrypt_      │  │  │ - last_sync  │ │
│ │ ┌──────────┐ │ │  │  session()     │  │  └──────────────┘ │
│ │ │ Contexts │ │ │  │                │  │                   │
│ │ └──────────┘ │ │  └─────────────────┘  └───────────────────┘
│ └──────────────┘ │
│                  │
│  • Playwright    │
│  • Rate limiting │
│  • GC task       │
└────────┬─────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         Kaspi Merchant Platform                          │
│                                                                          │
│  ┌────────────────────┐   ┌────────────────────┐   ┌──────────────────┐│
│  │  Login Page        │   │  SMS Verification  │   │  Merchant API    ││
│  │  idmc.shop.kaspi.kz│   │  (if required)     │   │  mc.shop.kaspi.kz││
│  └────────────────────┘   └────────────────────┘   └──────────────────┘│
└─────────────────────────────────────────────────────────────────────────┘
```

## Authentication Flow Diagram

### Flow 1: Direct Login (No SMS)

```
┌────────┐         ┌─────────┐         ┌────────────┐         ┌────────┐
│ Client │         │FastAPI  │         │  Service   │         │ Kaspi  │
└───┬────┘         └────┬────┘         └─────┬──────┘         └───┬────┘
    │                   │                    │                    │
    │ POST /link        │                    │                    │
    │ {email, password} │                    │                    │
    ├──────────────────>│                    │                    │
    │                   │ authenticate_kaspi()                    │
    │                   ├───────────────────>│                    │
    │                   │                    │ Navigate to login  │
    │                   │                    ├───────────────────>│
    │                   │                    │                    │
    │                   │                    │ Enter email        │
    │                   │                    ├───────────────────>│
    │                   │                    │                    │
    │                   │                    │ Enter password     │
    │                   │                    ├───────────────────>│
    │                   │                    │                    │
    │                   │                    │ Wait for navbar    │
    │                   │                    │<───────────────────┤
    │                   │                    │                    │
    │                   │                    │ Get cookies        │
    │                   │                    │<───────────────────┤
    │                   │                    │                    │
    │                   │                    │ Fetch merchant info│
    │                   │                    ├───────────────────>│
    │                   │                    │<───────────────────┤
    │                   │                    │                    │
    │                   │ Session data       │                    │
    │                   │<───────────────────┤                    │
    │                   │                    │                    │
    │                   │ Encrypt & store    │                    │
    │                   │ in database        │                    │
    │                   │                    │                    │
    │ 200 OK            │                    │                    │
    │ {store info}      │                    │                    │
    │<──────────────────┤                    │                    │
    │                   │                    │                    │
```

### Flow 2: Login with SMS Verification

```
┌────────┐    ┌─────────┐    ┌────────────┐    ┌────────┐    ┌────────┐
│ Client │    │FastAPI  │    │  Service   │    │ Kaspi  │    │  User  │
└───┬────┘    └────┬────┘    └─────┬──────┘    └───┬────┘    └───┬────┘
    │              │               │               │              │
    │ POST /link   │               │               │              │
    ├─────────────>│               │               │              │
    │              │ authenticate_ │               │              │
    │              │ kaspi()       │               │              │
    │              ├──────────────>│               │              │
    │              │               │ Navigate      │              │
    │              │               ├──────────────>│              │
    │              │               │               │              │
    │              │               │ Enter creds   │              │
    │              │               ├──────────────>│              │
    │              │               │               │              │
    │              │               │ Detect SMS    │              │
    │              │               │ field         │              │
    │              │               │<──────────────┤              │
    │              │               │               │              │
    │              │ Raise         │               │              │
    │              │ SMSRequired   │               │              │
    │              │<──────────────┤               │              │
    │              │               │               │              │
    │ 202 Accepted │               │               │              │
    │ {partial_    │               │               │              │
    │  session}    │               │               │              │
    │<─────────────┤               │               │              │
    │              │               │               │              │
    │ Show SMS     │               │               │  Send SMS    │
    │ input        │               │               ├─────────────>│
    ├─────────────────────────────────────────────────────────────>│
    │              │               │               │              │
    │              │               │               │  Enter code  │
    │<─────────────────────────────────────────────────────────────┤
    │              │               │               │              │
    │ POST /verify-sms             │               │              │
    │ {sms_code}   │               │               │              │
    ├─────────────>│               │               │              │
    │              │ verify_sms_   │               │              │
    │              │ code()        │               │              │
    │              ├──────────────>│               │              │
    │              │               │ Restore page  │              │
    │              │               │ with cookies  │              │
    │              │               │               │              │
    │              │               │ Enter SMS code│              │
    │              │               ├──────────────>│              │
    │              │               │               │              │
    │              │               │ Wait navbar   │              │
    │              │               │<──────────────┤              │
    │              │               │               │              │
    │              │               │ Get merchant  │              │
    │              │               ├──────────────>│              │
    │              │               │<──────────────┤              │
    │              │               │               │              │
    │              │ Complete      │               │              │
    │              │ session       │               │              │
    │              │<──────────────┤               │              │
    │              │               │               │              │
    │ 200 OK       │               │               │              │
    │ {store info} │               │               │              │
    │<─────────────┤               │               │              │
```

## Data Flow Diagram

### Session Data Lifecycle

```
┌──────────────────────────────────────────────────────────────────────┐
│                        1. AUTHENTICATION                              │
│                                                                       │
│  Email + Password  ──────────────>  Kaspi Login Page                 │
│                                                                       │
│                              ↓                                        │
│                                                                       │
│                        2. COOKIE CAPTURE                              │
│                                                                       │
│  Playwright Browser  <───────────  Kaspi Session Cookies             │
│                                                                       │
└────────────────────────────────────┬──────────────────────────────────┘
                                     │
                                     ▼
┌──────────────────────────────────────────────────────────────────────┐
│                     3. SESSION DATA ASSEMBLY                          │
│                                                                       │
│  {                                                                    │
│    cookies: [...],           ← From Playwright                       │
│    email: "...",             ← User input                            │
│    password: "...",          ← User input                            │
│    merchant_uid: "...",      ← From Kaspi API                        │
│    authenticated_at: "..."   ← Current timestamp                     │
│  }                                                                    │
│                                                                       │
└────────────────────────────────────┬──────────────────────────────────┘
                                     │
                                     ▼
┌──────────────────────────────────────────────────────────────────────┐
│                        4. ENCRYPTION                                  │
│                                                                       │
│  Plain JSON  ──────>  Fernet Cipher  ──────>  Encrypted String       │
│                       (AES-128)                                       │
│                                                                       │
│  "gAAAAABf8kj9x..."                                                   │
│                                                                       │
└────────────────────────────────────┬──────────────────────────────────┘
                                     │
                                     ▼
┌──────────────────────────────────────────────────────────────────────┐
│                     5. DATABASE STORAGE                               │
│                                                                       │
│  kaspi_stores.guid = {                                                │
│    "encrypted": "gAAAAABf8kj9x..."                                    │
│  }                                                                    │
│                                                                       │
└────────────────────────────────────┬──────────────────────────────────┘
                                     │
                                     │
                                     ▼
┌──────────────────────────────────────────────────────────────────────┐
│                     6. SESSION USAGE                                  │
│                                                                       │
│  When API call needed:                                                │
│                                                                       │
│  1. Fetch encrypted GUID from DB                                     │
│  2. Decrypt to get session data                                      │
│  3. Extract cookies                                                  │
│  4. Make authenticated request to Kaspi                              │
│                                                                       │
└───────────────────────────────────────────────────────────────────────┘
```

## Component Interaction Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                       kaspi_auth_service.py                          │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │            Public API Functions                                │ │
│  │                                                                │ │
│  │  authenticate_kaspi()    verify_sms_code()                     │ │
│  │  validate_session()      refresh_session()                     │ │
│  │  get_active_session()                                          │ │
│  └─────────┬──────────────────────────┬────────────────────────────┘ │
│            │                          │                              │
│  ┌─────────▼──────────┐   ┌───────────▼──────────┐                  │
│  │  Internal Helpers  │   │  Exception Classes   │                  │
│  │                    │   │                      │                  │
│  │  _login_to_kaspi_  │   │  KaspiAuthError      │                  │
│  │  page()            │   │  ├─ KaspiSMSRequired │                  │
│  │                    │   │  ├─ KaspiInvalidCreds│                  │
│  │  _verify_sms_on_   │   │  └─ KaspiSessionExp. │                  │
│  │  page()            │   │                      │                  │
│  │                    │   └──────────────────────┘                  │
│  │  _get_merchant_    │                                              │
│  │  info()            │                                              │
│  │                    │                                              │
│  │  _format_cookies() │                                              │
│  └────────────────────┘                                              │
│                                                                      │
└──────┬─────────────────────────┬─────────────────────┬───────────────┘
       │                         │                     │
       │ Uses                    │ Uses                │ Uses
       │                         │                     │
┌──────▼──────────┐   ┌──────────▼──────────┐   ┌─────▼─────────┐
│ browser_farm.py │   │   security.py       │   │ database.py   │
│                 │   │                     │   │               │
│ BrowserFarm     │   │ encrypt_session()   │   │ get_db_pool() │
│ Sharded         │   │ decrypt_session()   │   │ execute_one() │
│                 │   │                     │   │ execute_query()│
│ • get_browser_  │   │ Fernet cipher       │   │               │
│   farm()        │   │ AES-128 CBC         │   │ asyncpg Pool  │
│ • shards[]      │   │ HMAC signature      │   │               │
│ • get_context() │   │                     │   │               │
└─────────────────┘   └─────────────────────┘   └───────────────┘
```

## Error Handling Flow

```
                          ┌──────────────────┐
                          │  authenticate_   │
                          │  kaspi()         │
                          └────────┬─────────┘
                                   │
                    ┌──────────────┼──────────────┐
                    │              │              │
         ┌──────────▼──────┐  ┌────▼─────┐  ┌────▼──────────┐
         │  Login Success  │  │SMS       │  │ Login Error   │
         │  (No SMS)       │  │Required  │  │               │
         └────────┬────────┘  └────┬─────┘  └────┬──────────┘
                  │                │             │
                  │                │             │
         ┌────────▼────────┐  ┌────▼─────────┐  │
         │ Get merchant    │  │Raise         │  │
         │ info            │  │KaspiSMS      │  │
         │                 │  │RequiredError │  │
         └────────┬────────┘  └────┬─────────┘  │
                  │                │             │
         ┌────────▼────────┐       │             │
         │ Encrypt session │       │             │
         └────────┬────────┘       │             │
                  │                │             │
         ┌────────▼────────┐       │      ┌──────▼──────────┐
         │ Store in DB     │       │      │ Check error type│
         └────────┬────────┘       │      └──────┬──────────┘
                  │                │             │
         ┌────────▼────────┐       │      ┌──────▼──────────┐
         │ Return session  │       │      │ Invalid creds?  │
         │ data            │       │      └──────┬──────────┘
         └─────────────────┘       │             │
                                   │      ┌──────▼──────────┐
                  ┌────────────────┤      │ Raise           │
                  │                │      │ KaspiInvalid    │
                  │                │      │ CredentialsError│
                  │                │      └──────┬──────────┘
                  │                │             │
         ┌────────▼────────┐       │      ┌──────▼──────────┐
         │ Client stores   │       │      │ Other errors    │
         │ partial_session │       │      └──────┬──────────┘
         └────────┬────────┘       │             │
                  │                │      ┌──────▼──────────┐
         ┌────────▼────────┐       │      │ Raise           │
         │ User enters SMS │       │      │ KaspiAuthError  │
         └────────┬────────┘       │      └─────────────────┘
                  │                │
         ┌────────▼────────┐       │
         │ verify_sms_code()       │
         └────────┬────────┘       │
                  │                │
         ┌────────▼────────────────▼────┐
         │  Complete authentication     │
         │  flow (same as login success)│
         └──────────────────────────────┘
```

## Session Validation Flow

```
┌─────────────────────────────────────────────────────────────┐
│                    validate_session(guid)                    │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
                  ┌────────────────┐
                  │ Decrypt GUID   │
                  │ (if encrypted) │
                  └────────┬───────┘
                           │
                           ▼
                  ┌────────────────┐
                  │ Extract cookies│
                  └────────┬───────┘
                           │
                           ▼
                  ┌────────────────┐
                  │ Make test API  │
                  │ call to Kaspi  │
                  └────────┬───────┘
                           │
              ┌────────────┼────────────┐
              │            │            │
         ┌────▼────┐  ┌────▼────┐  ┌───▼──────┐
         │ 200 OK  │  │ 401     │  │ Network  │
         │ + data  │  │ Unauth  │  │ Error    │
         └────┬────┘  └────┬────┘  └───┬──────┘
              │            │           │
         ┌────▼────┐  ┌────▼────┐  ┌───▼──────┐
         │ Return  │  │ Return  │  │ Return   │
         │ True    │  │ False   │  │ False    │
         └─────────┘  └─────────┘  └──────────┘
```

## File Organization

```
new-backend/
└── app/
    ├── core/
    │   ├── browser_farm.py      → Browser management
    │   ├── security.py          → Encryption/decryption
    │   └── database.py          → Database connection
    │
    ├── models/
    │   └── kaspi_store.py       → Data models
    │
    ├── services/
    │   ├── __init__.py          → Exports public API
    │   │
    │   ├── kaspi_auth_service.py              → Core service ⭐
    │   ├── test_kaspi_auth.py                 → Test suite
    │   │
    │   ├── kaspi_auth_usage_example.py        → Endpoint examples
    │   ├── kaspi_auth_complete_example.py     → Full implementation
    │   │
    │   ├── KASPI_AUTH_README.md               → Full documentation
    │   ├── KASPI_AUTH_QUICKSTART.md           → Quick reference
    │   ├── IMPLEMENTATION_SUMMARY.md          → Migration guide
    │   └── KASPI_AUTH_ARCHITECTURE.md         → This file
    │
    └── routers/
        └── kaspi.py             → Production endpoints (to be created)
```

## Deployment Architecture

```
┌───────────────────────────────────────────────────────────────────┐
│                         Load Balancer                              │
│                         (nginx/HAProxy)                            │
└────────────────────────┬──────────────────────────────────────────┘
                         │
         ┌───────────────┼───────────────┐
         │               │               │
┌────────▼────────┐ ┌───▼──────────┐ ┌─▼─────────────┐
│  FastAPI Pod 1  │ │ FastAPI Pod 2│ │ FastAPI Pod 3 │
│                 │ │              │ │               │
│ • Playwright    │ │ • Playwright │ │ • Playwright  │
│ • Browser Farm  │ │ • Browser Farm│ │ • Browser Farm│
│ • Service Layer │ │ • Service Layer│ │ • Service Layer│
└────────┬────────┘ └───┬──────────┘ └─┬─────────────┘
         │              │              │
         └──────────────┼──────────────┘
                        │
         ┌──────────────┼──────────────┐
         │              │              │
┌────────▼────────┐ ┌───▼──────────┐ ┌▼──────────────┐
│  PostgreSQL     │ │   Redis      │ │  S3/Storage   │
│  (asyncpg)      │ │  (cache)     │ │  (logs/dumps) │
│                 │ │              │ │               │
│ • kaspi_stores  │ │ • sessions   │ │ • screenshots │
│ • users         │ │ • rate limits│ │ • audit logs  │
└─────────────────┘ └──────────────┘ └───────────────┘
```

## Security Layers

```
┌─────────────────────────────────────────────────────────────┐
│                    Layer 7: Network                          │
│  • HTTPS/TLS                                                 │
│  • CORS policies                                             │
│  • Rate limiting                                             │
└──────────────────────────┬──────────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                    Layer 6: Application                      │
│  • JWT authentication                                        │
│  • API key validation                                        │
│  • Input sanitization                                        │
└──────────────────────────┬──────────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                    Layer 5: Service                          │
│  • Session validation                                        │
│  • Exception handling                                        │
│  • Audit logging                                             │
└──────────────────────────┬──────────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                    Layer 4: Data                             │
│  • Fernet encryption (AES-128)                               │
│  • HMAC signatures                                           │
│  • Key rotation                                              │
└──────────────────────────┬──────────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                    Layer 3: Database                         │
│  • Connection encryption                                     │
│  • Row-level security                                        │
│  • Backup encryption                                         │
└──────────────────────────┬──────────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                    Layer 2: Infrastructure                   │
│  • VPC isolation                                             │
│  • Security groups                                           │
│  • Firewall rules                                            │
└──────────────────────────┬──────────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                    Layer 1: Physical                         │
│  • Data center security                                      │
│  • Encrypted disks                                           │
│  • Secure boot                                               │
└─────────────────────────────────────────────────────────────┘
```

---

This architecture document provides a visual understanding of how all components interact in the Kaspi Authentication Service implementation.
