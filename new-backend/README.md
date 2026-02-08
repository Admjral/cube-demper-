# Cube Demper - New Backend

Production-ready backend combining best practices from three existing implementations:
- **kaspi-demper**: Scalability patterns (sharding, browser farm, rate limiting)
- **django-backend**: Clean architecture and error handling
- **backend**: Complete feature set (WhatsApp, AI, Billing, Admin)

## Tech Stack

### Core
- **FastAPI 0.115+** - Modern async web framework
- **Python 3.11+** - Latest Python with performance improvements
- **Uvicorn** - ASGI server with multiple workers

### Database
- **asyncpg** - Pure async PostgreSQL driver (3x faster than SQLAlchemy)
- **PostgreSQL 15+** - Primary database
- **Alembic** - Database migrations
- **Redis 7+** - Caching, sessions, proxy coordination, rate limiting

### Browser & Parsing
- **Playwright** - Modern browser automation (async)
- **httpx** - Async HTTP client for external APIs

### External Integrations
- **WAHA Core** - WhatsApp HTTP API (free version, one container per user)
- **OpenAI SDK** - AI assistants (Lawyer, Accountant, Salesman)
- **TipTopPay** - Payment processing
- **Docker SDK** - WAHA container orchestration

## Project Structure

```
new-backend/
├── app/
│   ├── main.py                    # FastAPI application
│   ├── config.py                  # Pydantic Settings
│   ├── dependencies.py            # Dependency injection
│   │
│   ├── core/                      # Infrastructure
│   │   ├── database.py           # asyncpg pool
│   │   ├── redis.py              # Redis client
│   │   ├── logger.py             # Logging setup
│   │   ├── security.py           # JWT, bcrypt, encryption
│   │   └── exceptions.py         # Custom exceptions
│   │
│   ├── models/                    # Database models
│   ├── schemas/                   # Pydantic API schemas
│   ├── routers/                   # API endpoints
│   ├── services/                  # Business logic
│   ├── workers/                   # Background workers
│   └── utils/                     # Utilities
│
├── migrations/                    # Alembic migrations
├── tests/                         # Test suite
├── scripts/                       # Deployment scripts
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
└── .env.example
```

## Key Features

### 1. Tiyns Pricing System
Prices are stored as integers (tiyns) to avoid float precision errors:
- 1 KZT = 100 tiyns
- Example: 199.99 KZT = 19999 tiyns

### 2. Sharded Price Demper
- 4 worker instances running in parallel
- Hash-based product distribution
- 100 concurrent tasks per instance
- Global rate limiting: 120 RPS

### 3. Browser Farm
- Playwright-based automation
- Browser context pooling
- Garbage collection for idle contexts
- Token bucket rate limiting

### 4. WAHA Core Integration
- One Docker container per user (Core limitation)
- Text messages only (no media in Core version)
- Dynamic container orchestration
- QR code authentication

### 5. Multi-Tenant Architecture
- Complete user isolation
- All queries filtered by user_id
- Role-based access control (user/admin)

## Quick Start

### 1. Clone and Setup

```bash
cd /Users/adilhamitov/Desktop/Cube\ Demper/new-backend

# Create .env from template
cp .env.example .env

# Edit .env with your configuration
nano .env
```

### 2. Local Development (with Docker)

```bash
# Build and start all services
docker-compose up --build

# The API will be available at http://localhost:8010
# API docs at http://localhost:8010/docs
```

### 3. Local Development (without Docker)

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Install Playwright browsers
playwright install chromium

# Run migrations
alembic upgrade head

# Start API server
uvicorn app.main:app --reload --port 8010
```

## Database Migrations

```bash
# Create a new migration
alembic revision -m "description"

# Apply migrations
alembic upgrade head

# Rollback one migration
alembic downgrade -1

# View migration history
alembic history
```

## Architecture Highlights

### Sharded Demper Logic
```python
# Each instance processes a subset of products
INSTANCE_INDEX = 0  # 0, 1, 2, 3
INSTANCE_COUNT = 4

# Hash-based sharding
SELECT * FROM products
WHERE bot_active = true
  AND mod(abs(hashtext(id::text)), 4) = 0
```

### WAHA Container Management
```python
# One container per user
container_name = f"waha-user-{user_id}"
container = docker_client.containers.run(
    image="devlikeapro/waha:latest",
    ports={"3000/tcp": assigned_port},
    ...
)
```

### Rate Limiting
```python
# Token bucket: 120 RPS globally
bucket = TokenBucket(120)
await bucket.acquire()  # Blocks until token available
```

## Environment Variables

Key environment variables (see `.env.example` for complete list):

```bash
# Database
POSTGRES_HOST=postgres
POSTGRES_DB=cube_demper
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres

# Redis
REDIS_HOST=redis

# Security
SECRET_KEY=your-secret-key-min-32-characters
ENCRYPTION_KEY=your-fernet-encryption-key

# Demper Configuration
INSTANCE_INDEX=0
INSTANCE_COUNT=4
MAX_CONCURRENT_TASKS=100
GLOBAL_RPS=120

# WAHA
WAHA_BASE_IMAGE=devlikeapro/waha:latest
WAHA_BASE_PORT=3100
```

## API Endpoints (Planned)

```
POST   /auth/register         # User registration
POST   /auth/login            # User login
GET    /auth/me               # Current user info

GET    /kaspi/stores          # List stores
POST   /kaspi/stores/sync     # Sync store products
POST   /kaspi/auth            # Kaspi authentication

GET    /products              # List products
PATCH  /products/{id}         # Update product
POST   /products/bulk-update  # Bulk price update

GET    /preorders             # List preorders
POST   /preorders             # Create preorder

POST   /whatsapp/session/create   # Create WAHA container
GET    /whatsapp/session/qr       # Get QR code
POST   /whatsapp/send             # Send message

POST   /ai/chat               # Chat with AI assistant

GET    /billing/plans         # List subscription plans
POST   /billing/subscribe     # Create subscription

GET    /admin/stats           # Admin statistics
GET    /admin/users           # List all users
```

## Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=app --cov-report=html

# Run specific test file
pytest tests/test_auth.py
```

## Production Deployment

### Docker Compose (Recommended)

```bash
# Build images
docker-compose build

# Start services
docker-compose up -d

# View logs
docker-compose logs -f api

# Scale demper workers
docker-compose up -d --scale demper-0=2
```

### Manual Deployment

1. Setup PostgreSQL and Redis
2. Configure environment variables
3. Run migrations: `alembic upgrade head`
4. Start API: `uvicorn app.main:app --host 0.0.0.0 --port 8010 --workers 4`
5. Start demper workers with different INSTANCE_INDEX values

## Security Considerations

- **JWT Authentication**: All protected endpoints require Bearer token
- **Session Encryption**: Kaspi sessions encrypted with Fernet
- **Rate Limiting**: Global 120 RPS across all instances
- **Password Hashing**: bcrypt with cost factor 12
- **SQL Injection**: Protected by asyncpg parameter binding
- **CORS**: Configure allowed origins in production

## Performance Targets

- Database queries: < 50ms (p95)
- API response time: < 200ms (p95)
- Browser farm: 120 RPS sustained
- Demper throughput: 400+ products/minute (4 workers)

## License

Proprietary - All rights reserved

## Support

For issues and questions, contact the development team.
