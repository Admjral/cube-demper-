FROM python:3.11-slim

WORKDIR /app

# Install system dependencies including Playwright deps
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    curl \
    # Playwright system dependencies
    libnss3 \
    libnspr4 \
    libdbus-1-3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libxshmfence1 \
    libpango-1.0-0 \
    libcairo2 \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python packages FIRST
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# NOW install Playwright browsers (AFTER pip install playwright)
RUN playwright install chromium

# Copy application code
COPY . .

# Create non-root user
RUN useradd -m -r appuser && \
    mkdir -p logs && \
    chown -R appuser:appuser /app && \
    mkdir -p /home/appuser/.cache && \
    chown -R appuser:appuser /home/appuser

USER appuser

# Expose port
EXPOSE 8010

# Start command
CMD ["sh", "-c", "alembic upgrade head && exec uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8010}"]
