"""Application settings and configuration."""

from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional
from pathlib import Path


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(Path.home() / ".chronos_lab" / ".env"),
        env_file_encoding="utf-8",
        extra="ignore"
    )

    intrinio_api_key: Optional[str] = None
    log_level: str = "INFO"

    arcticdb_local_path: Optional[str] = None
    arcticdb_s3_bucket: Optional[str] = None


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
