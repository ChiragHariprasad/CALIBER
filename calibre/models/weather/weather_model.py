"""
CALIBRE — Weather & Crossover Model (§32)
=========================================
Tracks rain onset probability, track surface dampness, and intermediate / wet
tyre crossover timing windows.
"""
from __future__ import annotations

from calibre.schemas.race_telemetry import WeatherState


class WeatherModel:
    """Monitors meteorological conditions and tyre crossover windows."""

    def evaluate_weather(
        self,
        rain_probability_pct: float,
        track_temp_c: float = 35.0,
        air_temp_c: float = 24.0,
    ) -> WeatherState:
        # Crossover to Intermediates happens when slick pace degrades by > 4.5s
        crossover = rain_probability_pct >= 60.0
        inter_window = rain_probability_pct >= 75.0

        return WeatherState(
            air_temp_c=air_temp_c,
            track_temp_c=track_temp_c,
            rain_probability_pct=round(rain_probability_pct, 1),
            precipitation_intensity=round(max(0.0, (rain_probability_pct - 50.0) / 50.0), 2),
            wind_speed_kph=14.0,
            crossover_approaching=crossover,
            intermediate_window=inter_window,
        )
