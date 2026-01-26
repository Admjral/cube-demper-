from pydantic_settings import BaseSettings, SettingsConfigDict
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
    db_pool_min_size: Optional[int] = 10
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
    browser_shards: int = 2
    max_concurrency_per_proxy: int = 8
    request_timeout_ms: int = 15000
    idle_context_ttl: int = 300
    global_rps: int = 60  # ✅ Reduced from 120 to be more conservative (anti-ban)

    # Kaspi API
    kaspi_api_base_url: str = "https://kaspi.kz/shop/api"
    kaspi_auth_url: str = "https://idmc.shop.kaspi.kz"

    # WAHA Configuration (shared container from docker-compose)
    waha_url: str = "http://waha:3000"  # WAHA API URL inside Docker network
    waha_api_key: Optional[str] = None  # API key for WAHA (if set in WAHA config)
    waha_webhook_url: Optional[str] = None  # Will use {backend_url}/whatsapp/webhook if not set
    waha_enabled: bool = True  # Enable WAHA by default for Docker deployment
    waha_plus: bool = False  # Set to True when WAHA Plus is activated (supports multiple sessions)
    
    # Legacy WAHA Docker settings (kept for reference, not used with shared container)
    waha_base_image: str = "devlikeapro/waha:latest"
    waha_base_port: int = 3100
    waha_network: str = "cube-demper-network"
    waha_volume_prefix: str = "waha-user"

    # Railway Integration (optional, for per-user WAHA containers)
    railway_api_token: Optional[str] = None
    railway_project_id: Optional[str] = None

    # OpenAI
    openai_api_key: Optional[str] = None
    openai_model: str = "gpt-4-turbo-preview"
    openai_max_tokens: int = 2000

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


settings = Settings()
