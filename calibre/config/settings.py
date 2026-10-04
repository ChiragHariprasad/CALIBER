"""
CALIBRE – Application Settings
===============================
Centralised configuration loaded from environment variables / .env file.
"""

from __future__ import annotations

from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    """All tunable knobs live here.  Override via env-vars or a .env file."""

    # -- Kafka -----------------------------------------------------------------
    kafka_bootstrap_servers: str = "localhost:9092"

    # -- PostgreSQL ------------------------------------------------------------
    database_url: str = "postgresql+asyncpg://calibre:calibre@localhost:5432/calibre"
    database_url_sync: str = "postgresql://calibre:calibre@localhost:5432/calibre"

    # -- Redis (optional) ------------------------------------------------------
    redis_url: str = "redis://localhost:6379/0"

    # -- API -------------------------------------------------------------------
    api_host: str = "0.0.0.0"
    api_port: int = 8000

    # -- Replay ----------------------------------------------------------------
    replay_speed: float = 10.0
    replay_data_path: str = "data/sample"

    # -- Rules -----------------------------------------------------------------
    pit_lane_speed_limit_kmh: float = 60.0

    # -- OpenF1 Live Data ------------------------------------------------------
    openf1_base_url: str = "https://api.openf1.org/v1"
    openf1_car_data_interval_s: float = 3.0
    openf1_lap_interval_s: float = 5.0
    openf1_position_interval_s: float = 5.0
    openf1_race_control_interval_s: float = 3.0
    openf1_weather_interval_s: float = 10.0

    # -- Open-Meteo Weather ----------------------------------------------------
    open_meteo_base_url: str = "https://api.open-meteo.com/v1/forecast"

    # -- Race Day Defaults -----------------------------------------------------
    default_session_key: int = 11731
    default_target_driver: str = "VER"
    default_total_laps: int = 56
    race_log_dir: str = "data/race_logs"

    # -- Uncertainty -----------------------------------------------------------
    measurement_uncertainty: float = Field(
        default=1.5,
        description="Sensor measurement uncertainty (km/h, 1-σ)",
    )
    parameter_uncertainty: float = Field(
        default=0.5,
        description="Regulatory-parameter uncertainty (km/h, 1-σ)",
    )
    uncertainty_k: float = Field(
        default=2.0,
        description="Coverage factor k for BREACH / CLEAR / DEFER thresholds",
    )

    # -- Integrity (CUSUM) -----------------------------------------------------
    cusum_threshold: float = 5.0
    cusum_drift: float = 0.5

    # -- Logging ---------------------------------------------------------------
    log_level: str = "INFO"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


# Singleton – import this everywhere
settings = Settings()
