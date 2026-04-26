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
        CompletedOperationHistory,
        CoconutAllocation,
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
    _ensure_field_gross_revenue_column()
    _ensure_field_operation_columns()
    _ensure_scheduled_task_cycle_columns()
    _ensure_scheduled_task_early_completion_columns()
    _ensure_scheduled_task_notification_columns()
    _ensure_weather_data_columns()
    _ensure_notification_data_column()
    _ensure_crop_project_completion_columns()
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


def _ensure_field_gross_revenue_column():
    """Backfill gross_revenue on fields for coconut salary allocation."""
    import sqlalchemy

    inspector = sqlalchemy.inspect(engine)
    if "fields" not in inspector.get_table_names():
        return

    existing_cols = {c["name"] for c in inspector.get_columns("fields")}
    if "gross_revenue" in existing_cols:
        return

    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE fields ADD COLUMN gross_revenue FLOAT"))


def _ensure_field_operation_columns():
    """Backfill field completion tracking columns for existing databases."""
    import sqlalchemy

    inspector = sqlalchemy.inspect(engine)
    if "fields" not in inspector.get_table_names():
        return

    existing_cols = {c["name"] for c in inspector.get_columns("fields")}
    alter_sql = []
    if "operation_status" not in existing_cols:
        alter_sql.append("ALTER TABLE fields ADD COLUMN operation_status VARCHAR")
    if "status" not in existing_cols:
        alter_sql.append("ALTER TABLE fields ADD COLUMN status VARCHAR")
    if "completed_at" not in existing_cols:
        alter_sql.append("ALTER TABLE fields ADD COLUMN completed_at DATETIME")

    with engine.begin() as conn:
        for stmt in alter_sql:
            conn.execute(text(stmt))

        if "operation_status" not in existing_cols:
            conn.execute(text("UPDATE fields SET operation_status = 'ongoing' WHERE operation_status IS NULL"))
        if "status" not in existing_cols:
            conn.execute(text("UPDATE fields SET status = 'ongoing' WHERE status IS NULL"))


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


def _ensure_scheduled_task_early_completion_columns():
    """Backfill scheduled_tasks early completion fields for existing databases."""
    import sqlalchemy

    inspector = sqlalchemy.inspect(engine)
    if "scheduled_tasks" not in inspector.get_table_names():
        return

    existing_cols = {c["name"] for c in inspector.get_columns("scheduled_tasks")}
    alter_sql = []
    if "early_completed" not in existing_cols:
        alter_sql.append("ALTER TABLE scheduled_tasks ADD COLUMN early_completed BOOLEAN")
    if "early_completion_reason" not in existing_cols:
        alter_sql.append("ALTER TABLE scheduled_tasks ADD COLUMN early_completion_reason TEXT")
    if "early_completion_warning_acknowledged" not in existing_cols:
        alter_sql.append("ALTER TABLE scheduled_tasks ADD COLUMN early_completion_warning_acknowledged BOOLEAN")
    if "early_completion_days" not in existing_cols:
        alter_sql.append("ALTER TABLE scheduled_tasks ADD COLUMN early_completion_days INTEGER")

    with engine.begin() as conn:
        for stmt in alter_sql:
            conn.execute(text(stmt))

        if "early_completed" not in existing_cols:
            conn.execute(text("UPDATE scheduled_tasks SET early_completed = 0 WHERE early_completed IS NULL"))
        if "early_completion_warning_acknowledged" not in existing_cols:
            conn.execute(
                text(
                    "UPDATE scheduled_tasks SET early_completion_warning_acknowledged = 0 "
                    "WHERE early_completion_warning_acknowledged IS NULL"
                )
            )
        if "early_completion_days" not in existing_cols:
            conn.execute(text("UPDATE scheduled_tasks SET early_completion_days = 0 WHERE early_completion_days IS NULL"))


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


def _ensure_crop_project_completion_columns():
    """Backfill project completion tracking for existing databases."""
    import sqlalchemy

    inspector = sqlalchemy.inspect(engine)
    if "crop_projects" not in inspector.get_table_names():
        return

    existing_cols = {c["name"] for c in inspector.get_columns("crop_projects")}
    alter_sql = []
    if "completed_at" not in existing_cols:
        alter_sql.append("ALTER TABLE crop_projects ADD COLUMN completed_at DATETIME")

    with engine.begin() as conn:
        for stmt in alter_sql:
            conn.execute(text(stmt))

        conn.execute(
            text(
                """
                UPDATE crop_projects
                SET status = 'planned'
                WHERE status IS NULL
                """
            )
        )
