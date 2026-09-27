"""Typed, validated application configuration.

Every tunable in the platform is declared here exactly once. Nothing reads
``os.environ`` directly — that is what lets the workers, the API and the tests
share one source of truth and fail fast on a bad deployment rather than three
hours into a demo.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Annotated, Literal

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

Probability = Annotated[float, Field(ge=0.0, le=1.0)]


class Settings(BaseSettings):
    """Runtime configuration, hydrated from environment or ``.env``."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ---------------------------------------------------------------- app ---
    app_name: str = "SIH26069 Weather Intelligence Platform"
    app_env: Literal["local", "dev", "staging", "prod"] = "local"
    api_v1_prefix: str = "/api/v1"
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    log_json: bool = True

    # CORS origins for the Phase 5 React dashboard.
    cors_origins: list[str] = Field(
        default_factory=lambda: ["http://localhost:5173", "http://localhost:3000"]
    )

    # -------------------------------------------------------------- kafka ---
    kafka_enabled: bool = True
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_client_id_prefix: str = "sih26069"
    kafka_producer_acks: Literal["0", "1", "all"] = "all"
    kafka_producer_linger_ms: int = Field(default=20, ge=0, le=5_000)
    kafka_producer_max_batch_size: int = Field(default=65_536, ge=1_024)
    kafka_producer_compression: Literal["gzip", "snappy", "lz4", "none"] = "gzip"
    kafka_producer_retry_attempts: int = Field(default=5, ge=1, le=20)
    kafka_producer_retry_base_delay_s: float = Field(default=0.25, gt=0)
    kafka_consumer_group_prefix: str = "sih26069"
    kafka_consumer_max_poll_records: int = Field(default=200, ge=1, le=5_000)
    kafka_consumer_session_timeout_ms: int = Field(default=30_000, ge=6_000)

    # ----------------------------------------------------------- postgres ---
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "weatherdb"
    postgres_user: str = "weather"
    postgres_password: str = "weather_dev_pw"
    postgres_pool_min: int = Field(default=2, ge=1)
    postgres_pool_max: int = Field(default=10, ge=1)

    # -------------------------------------------------------------- redis ---
    redis_url: str = "redis://localhost:6379/0"
    redis_enabled: bool = True
    # Ingest-side idempotency window: a byte-identical report replayed inside
    # this window is dropped at the door rather than duplicated downstream.
    idempotency_ttl_s: int = Field(default=900, ge=60)

    # ------------------------------------------------------------ security --
    # Comma-separated in the environment, parsed into a set below.
    ingest_api_keys: str = "dev-citizen-key"
    admin_api_keys: str = "dev-admin-key"
    rate_limit_per_minute: int = Field(default=60, ge=1)
    rate_limit_burst: int = Field(default=20, ge=1)
    max_upload_bytes: int = Field(default=8 * 1024 * 1024, ge=1024)
    allowed_media_types: list[str] = Field(
        default_factory=lambda: ["image/jpeg", "image/png", "image/webp"]
    )
    media_root: Path = Path("/data/media")

    # ------------------------------------------------------------ geocoding --
    geocoder_remote_enabled: bool = False
    nominatim_base_url: str = "https://nominatim.openstreetmap.org"
    nominatim_user_agent: str = "sih26069-weather-platform/1.0 (disaster-management)"
    geocoder_timeout_s: float = Field(default=6.0, gt=0)
    # Gazetteer fuzzy-match floor. Below this we would rather emit UNRESOLVED
    # than pin a report to the wrong district.
    gazetteer_min_similarity: Probability = 0.82

    # India bounding box — reports outside it are kept but flagged, since the
    # platform's mandate is national.
    india_bbox_min_lat: float = 6.0
    india_bbox_max_lat: float = 37.6
    india_bbox_min_lon: float = 68.0
    india_bbox_max_lon: float = 97.5

    # ---------------------------------------------------------- providers ---
    imd_mock_mode: bool = True
    imd_base_url: str = "https://mausam.imd.gov.in"
    imd_poll_interval_s: int = Field(default=180, ge=15)
    imd_request_timeout_s: float = Field(default=15.0, gt=0)

    openweather_mock_mode: bool = True
    openweather_api_key: str = ""
    openweather_base_url: str = "https://api.openweathermap.org/data/2.5"
    openweather_poll_interval_s: int = Field(default=300, ge=30)
    openweather_request_timeout_s: float = Field(default=12.0, gt=0)

    # ------------------------------------------------- social simulator -----
    social_sim_rate_per_min: int = Field(default=90, ge=1, le=100_000)
    social_sim_duplicate_ratio: Probability = 0.22
    social_sim_missing_geo_ratio: Probability = 0.35
    social_sim_fake_ratio: Probability = 0.12
    social_sim_seed: int | None = None

    # --------------------------------------------------------- validators ---
    @field_validator("cors_origins", "allowed_media_types", mode="before")
    @classmethod
    def _split_csv(cls, value: object) -> object:
        """Accept both a JSON list and a plain comma-separated env string."""
        if isinstance(value, str) and not value.strip().startswith("["):
            return [item.strip() for item in value.split(",") if item.strip()]
        return value

    @field_validator("media_root", mode="before")
    @classmethod
    def _coerce_path(cls, value: object) -> object:
        return Path(value) if isinstance(value, str) else value

    @model_validator(mode="after")
    def _guard_production_defaults(self) -> "Settings":
        """Refuse to boot a non-local deployment on shipped demo credentials."""
        if self.app_env in ("staging", "prod"):
            weak = {"dev-citizen-key", "dev-admin-key", "dev-partner-key"}
            if weak & (self.ingest_api_key_set | self.admin_api_key_set):
                raise ValueError(
                    "Default development API keys are present while APP_ENV="
                    f"{self.app_env!r}. Set INGEST_API_KEYS and ADMIN_API_KEYS."
                )
            if self.postgres_password == "weather_dev_pw":
                raise ValueError("Default Postgres password used outside local env.")
        if self.postgres_pool_min > self.postgres_pool_max:
            raise ValueError("POSTGRES_POOL_MIN cannot exceed POSTGRES_POOL_MAX.")
        return self

    # ------------------------------------------------------- derived views --
    @property
    def ingest_api_key_set(self) -> frozenset[str]:
        return frozenset(k.strip() for k in self.ingest_api_keys.split(",") if k.strip())

    @property
    def admin_api_key_set(self) -> frozenset[str]:
        return frozenset(k.strip() for k in self.admin_api_keys.split(",") if k.strip())

    @property
    def postgres_dsn(self) -> str:
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def kafka_bootstrap_list(self) -> list[str]:
        return [s.strip() for s in self.kafka_bootstrap_servers.split(",") if s.strip()]

    def is_within_india(self, lat: float, lon: float) -> bool:
        return (
            self.india_bbox_min_lat <= lat <= self.india_bbox_max_lat
            and self.india_bbox_min_lon <= lon <= self.india_bbox_max_lon
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Process-wide singleton. Cached so validation runs exactly once."""
    return Settings()
