import importlib
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from config import config

# 1. Define the Base here. 
# Your models.py should import this Base from here.
Base = declarative_base() 

# 2. Create SQLite database engine
engine = create_engine(
    config.DATABASE_URL, 
    connect_args={"check_same_thread": False} if "sqlite" in config.DATABASE_URL else {}
)

# 3. Create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# 4. Dependency
def get_db():
    """Dependency to get database session"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# 5. Create tables
def init_db():
    # 1. Force the import of ALL models here
    from .models import (
        User,
        Farm,
        Field,
        Inventory,
        FinancialRecord,
        ScheduledTask,
        WeatherData,
        WeatherCache,
        DecisionTreeModel,
        CropProject,
        OtpCode,
        UserPreference,
        Notification,
        FCMDeviceToken,
    )
    from .database import engine, Base
    
    # 2. This command only creates tables that DON'T exist yet
    print("Creating database tables...")
    Base.metadata.create_all(bind=engine)
    _ensure_field_corn_columns()
    _ensure_scheduled_task_cycle_columns()
    _ensure_scheduled_task_notification_columns()
    _ensure_weather_data_columns()
    _ensure_notification_data_column()
    print("Done!")

    # Verification check
    import sqlalchemy
    inspector = sqlalchemy.inspect(engine)
    print(f"Tables currently in DB: {inspector.get_table_names()}")


def _ensure_scheduled_task_cycle_columns():
    """Backfill schema updates for existing deployments without a migration tool."""
    import sqlalchemy

    inspector = sqlalchemy.inspect(engine)
    if "scheduled_tasks" not in inspector.get_table_names():
        return

    existing_cols = {c["name"] for c in inspector.get_columns("scheduled_tasks")}
    alter_sql = []
    if "cycle_number" not in existing_cols:
        alter_sql.append("ALTER TABLE scheduled_tasks ADD COLUMN cycle_number INTEGER")
    if "cycle_day" not in existing_cols:
        alter_sql.append("ALTER TABLE scheduled_tasks ADD COLUMN cycle_day INTEGER")

    if not alter_sql:
        return

    with engine.begin() as conn:
        for stmt in alter_sql:
            conn.execute(text(stmt))


def _ensure_field_corn_columns():
    """Backfill corn-specific columns on fields for richer crop interfaces."""
    import sqlalchemy

    inspector = sqlalchemy.inspect(engine)
    if "fields" not in inspector.get_table_names():
        return

    existing_cols = {c["name"] for c in inspector.get_columns("fields")}
    desired_columns = {
        "corn_type": "VARCHAR",
        "intended_use": "VARCHAR",
        "planting_method": "VARCHAR",
        "seed_rate_kg_per_ha": "FLOAT",
        "seed_bags_count": "FLOAT",
        "row_spacing_cm": "FLOAT",
        "hill_spacing_cm": "FLOAT",
        "target_plant_population": "INTEGER",
        "irrigation_source": "VARCHAR",
        "drainage_condition": "VARCHAR",
        "previous_crop": "VARCHAR",
        "basal_fertilizer_type": "VARCHAR",
        "basal_fertilizer_rate": "FLOAT",
        "side_dress_fertilizer_type": "VARCHAR",
        "side_dress_fertilizer_rate": "FLOAT",
        "pest_observations": "TEXT",
        "disease_observations": "TEXT",
        "infestation_severity": "VARCHAR",
        "last_spray_date": "DATETIME",
        "pesticide_used": "VARCHAR",
        "moisture_content_at_harvest": "FLOAT",
        "expected_yield_per_ha": "FLOAT",
        "actual_yield": "FLOAT",
        "market_price_per_unit": "FLOAT",
    }

    alter_sql = [
        f"ALTER TABLE fields ADD COLUMN {column_name} {column_type}"
        for column_name, column_type in desired_columns.items()
        if column_name not in existing_cols
    ]

    if not alter_sql:
        return

    with engine.begin() as conn:
        for stmt in alter_sql:
            conn.execute(text(stmt))


def _ensure_weather_data_columns():
    """Backfill weather_data columns used by offline forecast reconstruction."""
    import sqlalchemy

    inspector = sqlalchemy.inspect(engine)
    if "weather_data" not in inspector.get_table_names():
        return

    existing_cols = {c["name"] for c in inspector.get_columns("weather_data")}
    alter_sql = []
    if "wind_speed_10m" not in existing_cols:
        alter_sql.append("ALTER TABLE weather_data ADD COLUMN wind_speed_10m FLOAT")
    if "weather_main" not in existing_cols:
        alter_sql.append("ALTER TABLE weather_data ADD COLUMN weather_main VARCHAR")

    if not alter_sql:
        return

    with engine.begin() as conn:
        for stmt in alter_sql:
            conn.execute(text(stmt))


def _ensure_scheduled_task_notification_columns():
    """Backfill scheduled_tasks columns used by tomorrow notification workflow."""
    import sqlalchemy

    inspector = sqlalchemy.inspect(engine)
    if "scheduled_tasks" not in inspector.get_table_names():
        return

    existing_cols = {c["name"] for c in inspector.get_columns("scheduled_tasks")}
    alter_sql = []
    if "tomorrow_check_at" not in existing_cols:
        alter_sql.append("ALTER TABLE scheduled_tasks ADD COLUMN tomorrow_check_at DATETIME")
    if "tomorrow_notification_sent_at" not in existing_cols:
        alter_sql.append("ALTER TABLE scheduled_tasks ADD COLUMN tomorrow_notification_sent_at DATETIME")
    if "tomorrow_notification_type" not in existing_cols:
        alter_sql.append("ALTER TABLE scheduled_tasks ADD COLUMN tomorrow_notification_type VARCHAR")

    if not alter_sql:
        return

    with engine.begin() as conn:
        for stmt in alter_sql:
            conn.execute(text(stmt))


def _ensure_notification_data_column():
    """Backfill notifications.data for structured frontend actions."""
    import sqlalchemy

    inspector = sqlalchemy.inspect(engine)
    if "notifications" not in inspector.get_table_names():
        return

    existing_cols = {c["name"] for c in inspector.get_columns("notifications")}
    if "data" in existing_cols:
        return

    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE notifications ADD COLUMN data TEXT"))
