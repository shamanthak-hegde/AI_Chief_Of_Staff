from functools import lru_cache
import os
from dotenv import load_dotenv

load_dotenv()

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    app_name: str = "Chief of Staff"
    environment: str = "development"
    log_level: str = "INFO"

    database_url: str = "postgresql://postgres:postgres@localhost:5432/chief_of_staff"

    openai_api_key: str = os.getenv("OPENAI_API_KEY")
    openai_model: str = os.getenv("OPENAI_MODEL", "gpt-5-nano")
    embeddings_model: str = os.getenv("EMBEDDINGS_MODEL", "text-embedding-3-small")
    openai_timeout_seconds: float = os.getenv("OPENAI_TIMEOUT_SECONDS", 30.0)


@lru_cache
def get_settings() -> Settings:
    return Settings()
