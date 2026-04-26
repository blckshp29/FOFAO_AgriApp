from __future__ import annotations

from typing import Any, Dict, List

import httpx

from config import config


class AsyncOpenWeatherService:
    def __init__(self) -> None:
        self.base_url = getattr(config, "OPENWEATHER_BASE_URL", "https://api.openweathermap.org/data/2.5")
        self.api_key = getattr(config, "OPENWEATHER_API_KEY", "")
        self.timeout = httpx.Timeout(15.0)

    async def _get(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        if not self.api_key:
            raise RuntimeError("OPENWEATHER_API_KEY is not configured")

        query = {
            **params,
            "appid": self.api_key,
            "units": "metric",
        }

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(f"{self.base_url}{path}", params=query)
            response.raise_for_status()
            return response.json()

    def format_current_weather(self, data: Dict[str, Any]) -> Dict[str, Any]:
        weather = ((data.get("weather") or [{}])[0]) or {}
        main = data.get("main") or {}
        return {
            "temperature": main.get("temp"),
            "condition": weather.get("main"),
            "description": weather.get("description"),
            "icon": weather.get("icon"),
        }

    async def get_current_weather(self, lat: float, lon: float) -> Dict[str, Any]:
        data = await self._get("/weather", {"lat": lat, "lon": lon})
        return self.format_current_weather(data)

    async def get_forecast(self, lat: float, lon: float) -> Dict[str, Any]:
        return await self._get("/forecast", {"lat": lat, "lon": lon})

    def detect_forecast_alerts(self, forecast: Dict[str, Any]) -> List[str]:
        alerts: List[str] = []
        for item in forecast.get("list", []):
            weather = ((item.get("weather") or [{}])[0]) or {}
            weather_main = (weather.get("main") or "").lower()
            weather_desc = weather.get("description") or weather_main
            rain_3h = ((item.get("rain") or {}).get("3h", 0)) or 0
            temp = (item.get("main") or {}).get("temp")
            wind_speed = ((item.get("wind") or {}).get("speed", 0)) or 0

            if rain_3h > 0 or weather_main in {"rain", "drizzle"}:
                alerts.append(f"Rain expected: {weather_desc}")

            if weather_main in {"thunderstorm", "tornado", "squall"}:
                alerts.append(f"Extreme weather expected: {weather_desc}")
            elif temp is not None and (temp >= 36 or temp <= 5):
                alerts.append(f"Extreme temperature expected: {temp}C")
            elif wind_speed >= 15:
                alerts.append(f"Strong wind expected: {wind_speed} m/s")

            if alerts:
                break

        return alerts
