# Claude Code Instructions for Cube Demper Project

## Project Overview
Cube Demper - сервис для автоматического демпинга цен на Kaspi.kz маркетплейсе.

## Repository Structure

### Main Monorepo (Development)
- **Path**: `/Users/adilhamitov/Desktop/cube-demper-full`
- **GitHub**: https://github.com/Admjral/cube-demper-full
- **Contains**: frontend + new-backend + sync scripts

### Separate Deployment Repos
После разработки изменения синхронизируются в отдельные репозитории:

#### Frontend
- **Path**: `/Users/adilhamitov/Desktop/Cube Demper/frontend`
- **GitHub**: https://github.com/Admjral/Demper_front
- **Branch**: `master`
- **Deploy**: Railway (service: `frontend`)

#### Backend
- **Path**: `/Users/adilhamitov/Desktop/Cube Demper/new-backend`
- **GitHub**: https://github.com/Admjral/cube-demper-
- **Branch**: `main`
- **Deploy**: Railway (service: `backemd`)

## Tech Stack

### Frontend
- Next.js 14 (App Router)
- TypeScript
- Tailwind CSS
- shadcn/ui components
- Zustand (state management)

### Backend
- FastAPI
- PostgreSQL (asyncpg)
- Redis
- Alembic (migrations)
- Playwright (web scraping)

## Deployment (Railway)

### Project: `proud-vision`
Services:
- `frontend` - Next.js app
- `backemd` - FastAPI backend (typo in name, don't change)
- `Postgres` - PostgreSQL database
- `Redis` - Redis cache
- `worker-1`, `worker-2`, `worker-3`, `worker-4` - Demping workers

### Backend Dockerfile
```dockerfile
# Main backend uses: Dockerfile
# Workers use: Dockerfile.worker
```

### Backend Start Command
```bash
sh -c 'alembic upgrade head && exec uvicorn app.main:app --host 0.0.0.0 --port $PORT'
```

### Worker Environment Variables
Each worker needs:
- `INSTANCE_INDEX` - 0, 1, 2, or 3
- `INSTANCE_COUNT` - 4

## Git Workflow

### Committing to Monorepo
```bash
cd /Users/adilhamitov/Desktop/cube-demper-full
git add <files>
git commit -m "feat: description"
git push origin main
```

### Syncing to Separate Repos
Use `sync.sh` script or manual rsync:

```bash
# Frontend sync
rsync -av --exclude='.git' --exclude='node_modules' --exclude='.next' \
  /Users/adilhamitov/Desktop/cube-demper-full/frontend/ \
  "/Users/adilhamitov/Desktop/Cube Demper/frontend/"

# Backend sync
rsync -av --exclude='.git' --exclude='__pycache__' --exclude='.venv' --exclude='venv' \
  /Users/adilhamitov/Desktop/cube-demper-full/new-backend/ \
  "/Users/adilhamitov/Desktop/Cube Demper/new-backend/"
```

### After Sync - Push to Deployment Repos
```bash
# Frontend
cd "/Users/adilhamitov/Desktop/Cube Demper/frontend"
git add -A
git commit -m "feat: description"
git push origin master

# Backend
cd "/Users/adilhamitov/Desktop/Cube Demper/new-backend"
git add -A
git commit -m "feat: description"
git push origin main
```

## Database Migrations

### Creating New Migration
```bash
cd /Users/adilhamitov/Desktop/cube-demper-full/new-backend
alembic revision -m "description"
```

### Migration Naming Convention
`YYYYMMDDHHMMSS_description.py` (e.g., `20260121100000_add_product_city_prices.py`)

### Important: Migration Chain
Always check `down_revision` points to the latest existing migration. Use:
```bash
alembic heads
alembic history
```

## Key Files

### Backend
- `app/main.py` - FastAPI app entry point
- `app/config.py` - Configuration and env vars
- `app/routers/` - API endpoints
- `app/services/api_parser.py` - Kaspi API integration
- `app/workers/demper_instance.py` - Worker for price demping
- `migrations/versions/` - Alembic migrations

### Frontend
- `src/app/(dashboard)/` - Dashboard pages (App Router)
- `src/components/` - React components
- `src/hooks/api/` - API hooks (React Query)
- `src/store/` - Zustand stores
- `src/types/api.ts` - TypeScript types

## Common Issues & Fixes

### Multiple Alembic Heads
Fix by updating `down_revision` in the problematic migration to point to correct parent.

### Railway CLI in Non-TTY Mode
Cannot use interactive commands. Use Railway Dashboard for worker configuration.

### Kaspi API Returns Null Fields
Always use fallback values, e.g.:
```python
kaspi_product_id = raw_offer.get("offerId") or raw_offer.get("sku")
```

### Demper Worker не видит товары ("Fetched 0 active products")
Проверь по порядку:
1. `bot_active = TRUE` у товаров? (включён демпинг)
2. `kaspi_stores.is_active = TRUE`?
3. `kaspi_stores.needs_reauth = FALSE`? ← **Частая проблема!**
4. `kaspi_stores.guid IS NOT NULL`? (есть сессия)
5. `products.external_kaspi_id IS NOT NULL`?
6. Текущее время в рабочих часах (`demping_settings.work_hours_*`)?

**Быстрый фикс needs_reauth:**
```sql
UPDATE kaspi_stores SET needs_reauth = FALSE WHERE guid IS NOT NULL;
```

## Git Config
- Email: `hamitov.adil04@gmail.com`
- Username: `Admjral`

## Railway CLI
- Path: `/opt/homebrew/bin/railway`
- To check status: `railway status`
- To view logs: `railway logs -s <service>`

## Direct Database Access
Для прямого доступа к БД Railway без CLI (Claude может использовать это):

```bash
# Получить публичный DATABASE_URL:
/opt/homebrew/bin/railway variables -s Postgres --json 2>&1 | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('DATABASE_PUBLIC_URL',''))"

# Запустить SQL через Python:
python3 -c "
import psycopg2
conn = psycopg2.connect('DATABASE_URL_HERE')
cur = conn.cursor()
cur.execute('SELECT ...')
print(cur.fetchall())
conn.close()
"
```

### Полезные диагностические запросы
```sql
-- Проверить почему воркер не видит товары
SELECT
    COUNT(*) as total_with_demping,
    COUNT(*) FILTER (WHERE ks.is_active = TRUE) as store_active,
    COUNT(*) FILTER (WHERE ks.needs_reauth = FALSE OR ks.needs_reauth IS NULL) as no_reauth_needed,
    COUNT(*) FILTER (WHERE ks.guid IS NOT NULL) as has_session,
    COUNT(*) FILTER (WHERE p.external_kaspi_id IS NOT NULL) as has_external_id
FROM products p
JOIN kaspi_stores ks ON ks.id = p.store_id
WHERE p.bot_active = TRUE;

-- Сбросить needs_reauth если сессия валидна
UPDATE kaspi_stores SET needs_reauth = FALSE WHERE guid IS NOT NULL;

-- Проверить историю цен
SELECT * FROM price_history ORDER BY created_at DESC LIMIT 20;
```

## Important Notes
1. **Never force push to main/master branches**
2. **Always sync both frontend and backend after changes**
3. **Check Railway deploy logs after pushing**
4. **Migrations run automatically on backend deploy**
5. **Workers need manual configuration in Railway Dashboard**
