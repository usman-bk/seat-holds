from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/seat_holds"
    redis_url: str = "redis://127.0.0.1:6379/0"
    pool_size: int = 10
    jwt_secret: str = "dev-only-not-a-secret-at-least-32-bytes-long"
    jwt_minutes: int = 30
    hold_seconds: float = 600
    sweep_seconds: float = 2
    client_queue_size: int = 256


settings = Settings()
