from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from typing import List

from sqlalchemy.orm import Session

from ..database import SessionLocal
from ..models import Field, Notification
from ..notifications.service import send_push_to_user
from .openweather_async import AsyncOpenWeatherService


logger = logging.getLogger(__name__)


def _recent_duplicate_exists(db: Session, user_id: int, message: str) -> bool:
    cutoff = datetime.utcnow() - timedelta(hours=6)
    existing = (
        db.query(Notification)
        .filter(
            Notification.user_id == user_id,
            Notification.type == "weather_alert",
            Notification.message == message,
            Notification.created_at >= cutoff,
        )
        .first()
    )
    return existing is not None


def trigger_weather_notification(db: Session, field: Field, alerts: List[str]) -> None:
    if not field.owner_id:
        return

    message = f"Field '{field.name}': {'; '.join(alerts)}"
    if _recent_duplicate_exists(db, field.owner_id, message):
        return

    send_push_to_user(
        db=db,
        user_id=field.owner_id,
        title="Weather Alert",
        body=message,
        notification_type="weather_alert",
        notification_data={"field_id": field.id, "alerts": alerts},
        data={"type": "weather_alert", "field_id": str(field.id)},
    )


async def process_field_weather_alerts(db: Session, weather_service: AsyncOpenWeatherService) -> None:
    fields = (
        db.query(Field)
        .filter(
            Field.location_lat.isnot(None),
            Field.location_lon.isnot(None),
            Field.is_deleted.is_(False),
        )
        .all()
    )

    for field in fields:
        try:
            forecast = await weather_service.get_forecast(field.location_lat, field.location_lon)
            alerts = weather_service.detect_forecast_alerts(forecast)
            if alerts:
                trigger_weather_notification(db, field, alerts)
        except Exception as exc:
            logger.warning("Weather alert scan failed for field %s: %s", field.id, exc)


async def weather_alert_loop(poll_seconds: int = 60 * 30) -> None:
    weather_service = AsyncOpenWeatherService()
    while True:
        db = SessionLocal()
        try:
            await process_field_weather_alerts(db, weather_service)
        except Exception as exc:
            logger.warning("Weather alert loop iteration failed: %s", exc)
        finally:
            db.close()

        await asyncio.sleep(poll_seconds)
