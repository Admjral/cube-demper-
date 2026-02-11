from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import model_validator
from typing import Optional


class Settings(BaseSettings):
    """Application configuration using Pydantic Settings"""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )

    # Application
    app_name: str = "Cube Demper"
    app_version: str = "1.0.0"
    debug: bool = False
    backend_url: str = "http://localhost:8010"

    # Server
    host: str = "0.0.0.0"
    port: int = 8010
    workers: int = 4

    # Database
    postgres_host: str = "localhost"
    postgres_port: Optional[int] = 5432
    postgres_db: str = "cube_demper"
    postgres_user: str = "postgres"
    postgres_password: str = "postgres"
    db_pool_min_size: Optional[int] = 5  # Balance between startup speed and availability
    db_pool_max_size: Optional[int] = 50

    @property
    def database_url(self) -> str:
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    # Redis
    redis_host: str = "localhost"
    redis_port: Optional[int] = 6379
    redis_db: Optional[int] = 0
    redis_password: Optional[str] = None

    @property
    def redis_url(self) -> str:
        if self.redis_password:
            return f"redis://:{self.redis_password}@{self.redis_host}:{self.redis_port}/{self.redis_db}"
        return f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"

    # Security
    secret_key: str = "your-secret-key-min-32-characters-change-in-production"
    encryption_key: str = "your-encryption-key-must-be-32-bytes-fernet-compatible"
    jwt_algorithm: str = "HS256"
    access_token_expire_hours: int = 24
    allowed_origins: str = "http://localhost:3000,http://localhost:3001"

    @property
    def cors_origins(self) -> list[str]:
        """Get list of allowed CORS origins"""
        return [origin.strip() for origin in self.allowed_origins.split(",")]

    # Price Demper Configuration
    instance_index: int = 0
    instance_count: int = 4
    max_concurrent_tasks: int = 100
    sync_stores_mode: str = "leader"  # "leader" or "shard"

    # Browser Farm
    browser_shards: int = 4
    max_concurrency_per_proxy: int = 8
    request_timeout_ms: int = 15000
    idle_context_ttl: int = 300
    global_rps: int = 60  # Used by browser_farm (legacy)

    # Per-endpoint rate limits (based on rate limit testing 2026-02-08)
    offers_rps: float = 8.0                  # Offers API (public, per IP) - safe 10, ban ~15
    pricefeed_rps: float = 1.5               # Pricefeed API (per merchant account!) - safe 2, ban ~3, 30-min ban!
    pricefeed_cooldown_seconds: int = 1800    # 30-min cooldown after pricefeed 429
    offers_ban_pause_seconds: int = 15        # Pause after 403 from offers API

    # Kaspi API
    kaspi_api_base_url: str = "https://kaspi.kz/shop/api"
    kaspi_auth_url: str = "https://idmc.shop.kaspi.kz"
    kaspi_api_proxy: Optional[str] = None  # KZ proxy for geo-restricted REST API

    # WAHA Configuration (shared container from docker-compose)
    waha_url: str = "http://waha:3000"  # WAHA API URL inside Docker network
    waha_api_key: Optional[str] = None  # API key for WAHA (if set in WAHA config)
    waha_webhook_url: Optional[str] = None  # Will use {backend_url}/whatsapp/webhook if not set
    waha_enabled: bool = True  # Enable WAHA by default for Docker deployment
    waha_plus: bool = True  # WAHA Plus activated (supports multiple sessions, NOWEB engine)
    waha_otp_session: str = "default"  # WAHA session for OTP codes (active session on WAHA Plus)

    # Railway Integration (optional, for per-user WAHA containers)
    railway_api_token: Optional[str] = None
    railway_project_id: Optional[str] = None

    # Google Gemini (API key from env: GEMINI_API_KEY)
    gemini_api_key: Optional[str] = None
    gemini_model: str = "gemini-2.5-flash"  # Fast and cheap for general tasks
    gemini_lawyer_model: str = "gemini-2.5-flash"  # Model for AI Lawyer
    gemini_max_tokens: int = 4000
    gemini_embedding_model: str = "text-embedding-004"  # For RAG embeddings

    # Proxy6.net (Proxy Provider)
    proxy6_api_key: Optional[str] = None
    proxy_pool_min_size: int = 500  # Minimum proxies in pool before auto-purchase
    proxy_auto_purchase: bool = True  # Enable automatic proxy purchasing

    # TipTopPay (Billing)
    tiptoppay_public_id: Optional[str] = None
    tiptoppay_api_secret: Optional[str] = None
    tiptoppay_webhook_secret: Optional[str] = None

    # Subscription Plans (prices in tiyns)
    plan_free_products_limit: int = 100
    plan_basic_products_limit: int = 500
    plan_pro_products_limit: int = 5000
    plan_basic_price_tiyns: int = 999900  # 9999.00 KZT
    plan_pro_price_tiyns: int = 2999900   # 29999.00 KZT

    # Logging
    log_level: str = "INFO"
    log_file: str = "logs/app.log"

    @model_validator(mode='after')
    def validate_secrets(self):
        if 'change-in-production' in self.secret_key:
            raise ValueError("SECRET_KEY must be set via environment variable")
        if 'fernet-compatible' in self.encryption_key:
            raise ValueError("ENCRYPTION_KEY must be set via environment variable")
        return self


settings = Settings()
