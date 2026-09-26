"""Configuration management for Yudiaz Virtual HQ backend.

Loads environment variables from .env with fallback defaults.
"""

from functools import lru_cache
from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment or defaults."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    host: str = Field(
        default="127.0.0.1",
        validation_alias=AliasChoices("OFFICE_HOST", "host"),
        description="Bind host for the API server",
    )
    port: int = Field(
        default=9449,
        validation_alias=AliasChoices("OFFICE_PORT", "port"),
        description="Bind port for the API server",
    )
    pin: str = Field(
        default="2609",
        validation_alias=AliasChoices("OFFICE_PIN", "pin"),
        description="Security PIN for command and executive authentication",
    )
    title: str = Field(
        default="Yudiaz Virtual HQ",
        validation_alias=AliasChoices("OFFICE_TITLE", "title"),
        description="Application title",
    )
    version: str = Field(
        default="1.0.0",
        description="API and service version",
    )
    simulation_interval: float = Field(
        default=3.0,
        validation_alias=AliasChoices("SIMULATION_INTERVAL", "simulation_interval"),
        description="Background simulation tick interval in seconds",
    )
    sse_interval: float = Field(
        default=2.0,
        validation_alias=AliasChoices("SSE_INTERVAL", "sse_interval"),
        description="Server-Sent Events streaming interval in seconds",
    )


@lru_cache
def get_settings() -> Settings:
    """Retrieve cached application settings instance."""
    return Settings()
