# =============================================================================
# config.py — centralised configuration via Pydantic BaseSettings
# =============================================================================
# All values are read from environment variables (or .env for local dev).
# Never hard-code secrets — this pattern is CI/CD and Docker-friendly.
# =============================================================================

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # Application
    APP_NAME: str     = "Retail Sales Analytics API"
    APP_VERSION: str  = "1.0.0"
    DEBUG: bool       = False

    # Database (asyncpg driver)
    POSTGRES_USER: str     = "retail_user"
    POSTGRES_PASSWORD: str = "retail_pass"
    POSTGRES_HOST: str     = "localhost"
    POSTGRES_PORT: int     = 5432
    POSTGRES_DB: str       = "retail_analytics"

    @property
    def DATABASE_URL(self) -> str:
        return (
            f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    # Pagination defaults
    DEFAULT_PAGE_SIZE: int = 20
    MAX_PAGE_SIZE: int     = 100


settings = Settings()