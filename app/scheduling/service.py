from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from sqlalchemy.orm import Session
import pandas as pd

# Renamed 'Field' to 'FarmField' to avoid conflict with pydantic.Field
from ..models import Field as FarmField, ScheduledTask, WeatherData 
from ..schemas import ScheduledTaskCreate, WeatherForecastRequest, DecisionTreeRequest
from ..weather.service import WeatherService
from ..decision_tree.engine import DecisionTreeEngine
from ..notifications.service import send_push_to_user

class SchedulingService:
    RICE_VARIETY_HARVEST_WINDOWS: Dict[str, Dict[str, Any]] = {
        "NSIC RC222": {"label": "NSIC Rc222", "min_days": 107, "max_days": 111},
        "NSIC RC216": {"label": "NSIC Rc216", "min_days": 105, "max_days": 110},
        "NSIC RC160": {"label": "NSIC Rc160", "min_days": 110, "max_days": 110},
        "NSIC RC300": {"label": "NSIC Rc300", "min_days": 110, "max_days": 110},
        "NSIC RC130": {"label": "NSIC Rc130", "min_days": 107, "max_days": 107},
        "NSIC RC480": {"label": "NSIC Rc480", "min_days": 107, "max_days": 107},
        "NSIC RC508": {"label": "NSIC Rc508", "min_days": 105, "max_days": 110},
        "NSIC RC120": {"label": "NSIC Rc120", "min_days": 92, "max_days": 107},
        "BIGANTE PLUS": {"label": "BIGANTE PLUS", "min_days": 104, "max_days": 118},
        "QUADRO CLASS": {"label": "QUADRO CLASS", "min_days": 104, "max_days": 110},
        "SL-8H": {"label": "SL-8H", "min_days": 105, "max_days": 115},
        "MESTISO 20(MISC RC 204H)": {"label": "MESTISO 20(MISC RC 204H)", "min_days": 111, "max_days": 114},
        "TH-82": {"label": "TH-82", "min_days": 110, "max_days": 110},
        "PHB 71/79": {"label": "PHB 71/79", "min_days": 110, "max_days": 115},
    }
    CORN_PROFILE_WINDOWS: Dict[str, Dict[str, Any]] = {
        "yellow": {
            "label": "Yellow Corn (Hybrid)",
            "prep_days": 14,
            "emergence": (0, 5),
            "first_weeding": (15, 20),
            "side_dress_hilling": (25, 30),
            "tasseling_silking": (50, 60),
            "grain_fill": (65, 90),
            "harvest": (100, 115),
        },
        "white": {
            "label": "White Corn (Laguna/IPB)",
            "prep_days": 14,
            "emergence": (0, 5),
            "first_weeding": (18, 22),
            "side_dress_hilling": (25, 30),
            "tasseling_silking": (45, 55),
            "grain_fill": (60, 85),
            "harvest": (95, 105),
        },
        "sweet": {
            "label": "Sweet Corn (Sugar King/Honey)",
            "prep_days": 10,
            "emergence": (0, 4),
            "first_weeding": (15, 18),
            "side_dress_hilling": (20, 25),
            "tasseling_silking": (40, 50),
            "grain_fill": None,
            "harvest": (65, 75),
        },
    }

    def __init__(self):
        self.weather_service = WeatherService()
        self.decision_tree = DecisionTreeEngine()

    def _normalize_rice_variety(self, crop_variety: Optional[str]) -> str:
        if not crop_variety:
            return "NSIC RC222"
        normalized = " ".join(crop_variety.strip().upper().split())
        normalized = normalized.replace("RC ", "RC")
        return normalized

    def _get_harvest_window(self, crop_variety: Optional[str]) -> Dict[str, Any]:
        variety_key = self._normalize_rice_variety(crop_variety)
        if variety_key not in self.RICE_VARIETY_HARVEST_WINDOWS:
            supported = ", ".join(sorted(self.RICE_VARIETY_HARVEST_WINDOWS.keys()))
            raise Exception(f"Unsupported rice variety: '{crop_variety}'. Supported varieties: {supported}")
        return self.RICE_VARIETY_HARVEST_WINDOWS[variety_key]

    def _normalize_corn_profile(self, corn_type: Optional[str], crop_variety: Optional[str]) -> str:
        candidates = [corn_type, crop_variety]
        for raw in candidates:
            if not raw:
                continue
            normalized = raw.strip().lower()
            if "sweet" in normalized or "sugar king" in normalized or "honey" in normalized:
                return "sweet"
            if "white" in normalized or "laguna" in normalized or "ipb" in normalized:
                return "white"
            if "yellow" in normalized or "hybrid" in normalized:
                return "yellow"
        return "yellow"

    def _get_corn_profile(self, corn_type: Optional[str], crop_variety: Optional[str]) -> Dict[str, Any]:
        profile_key = self._normalize_corn_profile(corn_type, crop_variety)
        return self.CORN_PROFILE_WINDOWS[profile_key]

    def _format_harvest_window(self, min_days: int, max_days: int) -> str:
        if min_days == max_days:
            return f"{min_days} days after transplanting"
        return f"{min_days}-{max_days} days after transplanting"

    def _describe_cycle_day(self, cycle_label: str, offset_day: int) -> str:
        return f"{cycle_label} Day {offset_day}"

    def _forecast_bounds(self, weather_data: Dict[str, Any]) -> Optional[Dict[str, datetime]]:
        daily = weather_data.get("daily", [])
        if not daily:
            return None
        dates: List[datetime] = []
        for entry in daily:
            raw_date = entry.get("date")
            if not raw_date:
                continue
            try:
                dates.append(datetime.fromisoformat(raw_date))
            except Exception:
                continue
        if not dates:
            return None
        return {"start": min(dates), "end": max(dates)}

    def _optimize_task_date_with_decision_tree(
        self,
        db: Session,
        field: FarmField,
        op_type: str,
        proposed_start: datetime,
        proposed_end: datetime,
        requires_dry_weather: bool,
        weather_data: Dict[str, Any]
    ) -> Optional[datetime]:
        bounds = self._forecast_bounds(weather_data)
        if not bounds:
            return None

        # Forecast APIs only cover a short horizon; optimize only when task window overlaps that range.
        if proposed_end < bounds["start"] or proposed_start > bounds["end"]:
            return None

        window_start = max(proposed_start, bounds["start"])
        window_end = min(proposed_end, bounds["end"])
        if window_start > window_end:
            return None

        try:
            dt_request = DecisionTreeRequest(
                field_id=field.id,
                operation_type=op_type
            )
            recommendation = self.decision_tree.predict_optimal_date(
                db=db,
                request=dt_request,
                weather_data=weather_data,
                current_budget=10**12,
                window_start=window_start,
                window_end=window_end,
                requires_dry_weather=requires_dry_weather
            )
            return recommendation.recommended_date
        except Exception:
            return None

    def generate_rice_variety_schedule(
        self,
        db: Session,
        field: FarmField,
        user_id: int,
        land_prep_start_date: Optional[datetime] = None
    ) -> List[ScheduledTask]:
        """Generate full rice schedule using fixed operations and variety-based harvest window."""
        if field.crop_type.value != "rice":
            raise Exception("Rice schedule is only for rice fields.")

        # Idempotency guard:
        # If a cycle schedule already exists for this user+field, reuse it instead of creating duplicates.
        existing_tasks = db.query(ScheduledTask).filter(
            ScheduledTask.user_id == user_id,
            ScheduledTask.field_id == field.id,
            ScheduledTask.is_deleted == False,
            ScheduledTask.cycle_number.in_([1, 2])
        ).order_by(ScheduledTask.scheduled_date.asc()).all()
        if existing_tasks:
            return existing_tasks

        harvest_window = self._get_harvest_window(field.crop_variety)
        variety_label = harvest_window["label"]
        min_harvest_days = harvest_window["min_days"]
        max_harvest_days = harvest_window["max_days"]

        start_date = land_prep_start_date or field.land_prep_start_date or field.planting_date or datetime.now()
        if field.land_prep_start_date is None:
            field.land_prep_start_date = start_date
            db.commit()

        # Land preparation (0-21 days)
        land_prep_cycle_label = "Cycle 1 (Land Preparation)"
        land_prep_cycle_number = 1
        land_prep_tasks = [
            ("irrigation", "Irrigation", 0, 0, False, "0 day (start)"),
            ("land_preparation", "Plowing", 6, 6, True, "6 days after start"),
            ("land_preparation", "Harrowing", 14, 14, True, "14 days after start"),
            ("land_preparation", "Levelling", 21, 21, True, "21 days after start"),
        ]

        # Planting to harvesting after land preparation. Operations are fixed; windows are optimized by weather.
        planting_cycle_label = "Cycle 2 (Planting to Harvest)"
        planting_cycle_number = 2
        planting_start = start_date + timedelta(days=21)
        planting_tasks = [
            ("planting", "Transplanting", 0, 0, True, "0 day (start)"),
            ("fertilization", "First Fertilizer (Basal)", 10, 14, True, "10-14 days after transplanting"),
            ("fertilization", "Second Fertilizer (Top Dressing)", 30, 45, True, "30-45 days after transplanting"),
            ("fertilization", "Third Fertilizer", 55, 65, True, "55-65 days after transplanting"),
            ("pest_control", "Pest and Weed Control", 67, 80, True, "67-80 days after transplanting"),
            ("irrigation", "Terminal Drainage", 90, 95, False, "90-95 days after transplanting"),
            (
                "harvesting",
                "Harvesting",
                min_harvest_days,
                max_harvest_days,
                True,
                self._format_harvest_window(min_harvest_days, max_harvest_days),
            ),
        ]

        scheduled_tasks: List[ScheduledTask] = []
        weather_request = WeatherForecastRequest(
            latitude=field.location_lat or 13.0,
            longitude=field.location_lon or 123.0,
            days=5
        )
        weather_data = self.weather_service.get_weather_forecast(db, weather_request)

        def add_task(base_date: datetime, task_tuple, cycle_label: str, cycle_number: int):
            op_type, name, min_offset_days, max_offset_days, requires_dry, window = task_tuple
            proposed_start = base_date + timedelta(days=min_offset_days)
            proposed_end = base_date + timedelta(days=max_offset_days)
            scheduled_date = self._optimize_task_date_with_decision_tree(
                db=db,
                field=field,
                op_type=op_type,
                proposed_start=proposed_start,
                proposed_end=proposed_end,
                requires_dry_weather=requires_dry,
                weather_data=weather_data
            ) or proposed_start
            description = f"{variety_label} rice schedule: {name}"
            if window:
                description += f" (window: {window})"
            description += f" [{self._describe_cycle_day(cycle_label, min_offset_days)}]"

            estimated_cost = self.decision_tree._estimate_operation_cost(
                op_type, field.area_hectares
            )

            task_data = ScheduledTaskCreate(
                task_type=op_type,
                task_name=f"{name} - {field.name}",
                description=description,
                scheduled_date=scheduled_date,
                original_scheduled_date=proposed_start,
                estimated_cost=estimated_cost,
                requires_dry_weather=requires_dry,
                priority=self._calculate_priority(op_type),
                cycle_number=cycle_number,
                cycle_day=min_offset_days,
                field_id=field.id
            )
            task = self.create_scheduled_task(db, task_data, user_id)
            task.decision_tree_recommendation = True
            db.commit()
            scheduled_tasks.append(task)

        for t in land_prep_tasks:
            add_task(start_date, t, land_prep_cycle_label, land_prep_cycle_number)
        for t in planting_tasks:
            add_task(planting_start, t, planting_cycle_label, planting_cycle_number)

        # Keep expected harvest date aligned with the selected variety window
        field.expected_harvest_date = planting_start + timedelta(days=max_harvest_days)
        db.commit()

        return scheduled_tasks

    def generate_rice_rc222_schedule(
        self,
        db: Session,
        field: FarmField,
        user_id: int,
        land_prep_start_date: Optional[datetime] = None
    ) -> List[ScheduledTask]:
        """Backward-compatible alias for older callers."""
        return self.generate_rice_variety_schedule(
            db=db,
            field=field,
            user_id=user_id,
            land_prep_start_date=land_prep_start_date,
        )

    def generate_corn_schedule(
        self,
        db: Session,
        field: FarmField,
        user_id: int,
        planting_date: Optional[datetime] = None
    ) -> List[ScheduledTask]:
        """Generate corn schedule using fixed land prep and profile-based stage windows."""
        if field.crop_type.value != "corn":
            raise Exception("Corn schedule is only for corn fields.")

        existing_tasks = db.query(ScheduledTask).filter(
            ScheduledTask.user_id == user_id,
            ScheduledTask.field_id == field.id,
            ScheduledTask.is_deleted == False,
            ScheduledTask.cycle_number.in_([1, 2])
        ).order_by(ScheduledTask.scheduled_date.asc()).all()
        if existing_tasks:
            return existing_tasks

        profile = self._get_corn_profile(field.corn_type, field.crop_variety)
        profile_label = profile["label"]
        prep_days = profile["prep_days"]

        planting_base = planting_date or field.planting_date
        land_prep_start = field.land_prep_start_date

        if land_prep_start and planting_base:
            minimum_planting_date = land_prep_start + timedelta(days=prep_days)
            if planting_base < minimum_planting_date:
                planting_base = minimum_planting_date
        elif land_prep_start and not planting_base:
            planting_base = land_prep_start + timedelta(days=prep_days)
        elif planting_base and not land_prep_start:
            land_prep_start = planting_base - timedelta(days=prep_days)
        else:
            land_prep_start = datetime.now()
            planting_base = land_prep_start + timedelta(days=prep_days)

        field.planting_date = planting_base
        field.land_prep_start_date = land_prep_start
        db.commit()

        land_prep_cycle_label = "Cycle 1 (Land Preparation to Planting)"
        planting_cycle_label = "Cycle 2 (Planting to Harvest)"

        if prep_days == 10:
            land_prep_tasks = [
                ("land_preparation", "Intensive Land Preparation", 0, 4, True, "Intensive land prep for loose soil and strong root health."),
                ("land_preparation", "Furrowing and Basal Fertilizer Prep", 5, 9, True, "Prepare final rows and basal fertilizer before sowing."),
            ]
        else:
            land_prep_tasks = [
                ("land_preparation", "Primary Plowing and Harrowing", 0, 7, True, "Primary plowing and harrowing during land preparation."),
                ("land_preparation", "Furrowing and Basal Fertilizer Prep", 7, 13, True, "Prepare 75 cm rows and basal fertilizer before sowing."),
            ]

        stage_tasks = [
            ("planting", "Planting (Sowing)", 0, 0, True, "Plant corn on Day 0 of the crop cycle."),
            ("planting", "Emergence Monitoring", *profile["emergence"], False, "Monitor emergence and stand establishment."),
            ("land_preparation", "Off-barring and Early Weeding", *profile["first_weeding"], True, "Perform early cultivation and weed control."),
            ("fertilization", "Side-dress Fertilizer and Hilling-up", *profile["side_dress_hilling"], True, "Apply nitrogen/urea and perform hilling-up."),
            ("pest_control", "Tasseling and Silking Check", *profile["tasseling_silking"], False, "Monitor pollination phase and field condition."),
        ]

        if profile["grain_fill"] is not None:
            stage_tasks.append(
                ("pest_control", "Grain Filling Check", *profile["grain_fill"], False, "Monitor grain or kernel development before harvest.")
            )

        stage_tasks.append(
            ("harvesting", "Harvesting", *profile["harvest"], True, "Harvest at the recommended maturity window.")
        )

        scheduled_tasks: List[ScheduledTask] = []
        weather_request = WeatherForecastRequest(
            latitude=field.location_lat or 13.0,
            longitude=field.location_lon or 123.0,
            days=5
        )
        weather_data = self.weather_service.get_weather_forecast(db, weather_request)

        def add_task(base_date: datetime, task_tuple, cycle_label: str, cycle_number: int):
            op_type, name, min_offset_days, max_offset_days, requires_dry, details = task_tuple
            proposed_start = base_date + timedelta(days=min_offset_days)
            proposed_end = base_date + timedelta(days=max_offset_days)
            scheduled_date = self._optimize_task_date_with_decision_tree(
                db=db,
                field=field,
                op_type=op_type,
                proposed_start=proposed_start,
                proposed_end=proposed_end,
                requires_dry_weather=requires_dry,
                weather_data=weather_data
            ) or proposed_start

            description = f"{profile_label} corn schedule: {name}"
            if details:
                description += f" ({details})"
            description += f" [{self._describe_cycle_day(cycle_label, min_offset_days)}]"

            estimated_cost = self.decision_tree._estimate_operation_cost(op_type, field.area_hectares)
            task_data = ScheduledTaskCreate(
                task_type=op_type,
                task_name=f"{name} - {field.name}",
                description=description,
                scheduled_date=scheduled_date,
                original_scheduled_date=proposed_start,
                estimated_cost=estimated_cost,
                requires_dry_weather=requires_dry,
                priority=self._calculate_priority(op_type),
                cycle_number=cycle_number,
                cycle_day=min_offset_days,
                field_id=field.id
            )
            task = self.create_scheduled_task(db, task_data, user_id)
            task.decision_tree_recommendation = True
            db.commit()
            scheduled_tasks.append(task)

        for task_tuple in land_prep_tasks:
            add_task(land_prep_start, task_tuple, land_prep_cycle_label, 1)
        for task_tuple in stage_tasks:
            add_task(planting_base, task_tuple, planting_cycle_label, 2)

        harvest_max = profile["harvest"][1]
        field.expected_harvest_date = planting_base + timedelta(days=harvest_max)
        db.commit()

        return scheduled_tasks

    def check_and_reschedule_task(
        self,
        db: Session,
        task: ScheduledTask,
        latitude: float,
        longitude: float
    ) -> ScheduledTask:
        """Check weather for a task and reschedule if unsuitable."""
        weather_request = WeatherForecastRequest(
            latitude=latitude,
            longitude=longitude,
            days=5
        )
        weather_data = self.weather_service.get_weather_forecast(db, weather_request)

        suitability = self.weather_service.check_weather_suitability(
            weather_data, task.scheduled_date, requires_dry_weather=task.requires_dry_weather
        )

        task.weather_check_date = datetime.utcnow()
        task.weather_status = "suitable" if suitability["is_suitable"] else "unsuitable"

        if not suitability["is_suitable"]:
            delay_days = max(1, suitability.get("recommended_delay_days", 1))
            window_start = task.scheduled_date + timedelta(days=delay_days)
            window_end = task.scheduled_date + timedelta(days=14)
            optimal_windows = self.weather_service.get_optimal_weather_window(
                weather_data, window_start, window_end, requires_dry_weather=task.requires_dry_weather
            )
            if optimal_windows:
                best = optimal_windows[0]
                if not task.original_scheduled_date:
                    task.original_scheduled_date = task.scheduled_date
                task.scheduled_date = best["date"]
                task.status = "rescheduled"
                task.rescheduled_reason = "; ".join(suitability.get("reasons", [])) or "Weather forecast unsuitable"
            else:
                if not task.original_scheduled_date:
                    task.original_scheduled_date = task.scheduled_date
                task.scheduled_date = task.scheduled_date + timedelta(days=delay_days)
                task.status = "rescheduled"
                task.rescheduled_reason = (
                    "; ".join(suitability.get("reasons", []))
                    or f"No suitable weather window found. Delayed by {delay_days} day(s)."
                )

        db.commit()
        db.refresh(task)
        return task

    def check_tasks_for_date(
        self,
        db: Session,
        user_id: int,
        target_date: datetime
    ) -> List[ScheduledTask]:
        """Run day-before checks for tasks scheduled on target_date."""
        start = datetime(target_date.year, target_date.month, target_date.day)
        end = start + timedelta(days=1)

        tasks = db.query(ScheduledTask).filter(
            ScheduledTask.user_id == user_id,
            ScheduledTask.scheduled_date >= start,
            ScheduledTask.scheduled_date < end,
            ScheduledTask.status == "pending"
        ).all()

        updated_tasks = []
        for task in tasks:
            field = db.query(FarmField).filter(FarmField.id == task.field_id).first()
            if not field:
                continue
            updated = self.check_and_reschedule_task(
                db,
                task,
                latitude=field.location_lat or 13.0,
                longitude=field.location_lon or 123.0
            )
            updated_tasks.append(updated)

        return updated_tasks
    
    def create_scheduled_task(self, db: Session, task_data: ScheduledTaskCreate, user_id: int) -> ScheduledTask:
        """Create a new scheduled task"""
        # Keep DB defaults intact and force a deterministic initial status.
        payload = task_data.model_dump(exclude_none=True)
        if "status" not in payload:
            payload["status"] = "pending"

        task = ScheduledTask(
            **payload,
            user_id=user_id
        )
        
        db.add(task)
        db.commit()
        db.refresh(task)
        
        return task
    
    def generate_optimized_schedule(self, db: Session, field_id: int, user_id: int, 
                                    operations: List[str] = None) -> List[ScheduledTask]:
        """Generate optimized schedule for field operations"""
        # Using the renamed FarmField model here
        field = db.query(FarmField).filter(FarmField.id == field_id).first()
        
        if not field:
            raise Exception(f"Field with id {field_id} not found")
        
        if not operations:
            operations = ["land_preparation", "planting", "fertilization", 
                          "irrigation", "pest_control", "harvesting"]
        else:
            allowed = {"land_preparation", "planting", "fertilization", "irrigation", "pest_control", "harvesting"}
            normalized = []
            for op in operations:
                if not isinstance(op, str):
                    continue
                op_norm = op.strip().lower()
                if op_norm in allowed:
                    normalized.append(op_norm)
            if not normalized:
                raise Exception("Invalid operations list. Allowed: land_preparation, planting, fertilization, irrigation, pest_control, harvesting")
            operations = normalized
        
        scheduled_tasks = []
        current_date = field.planting_date if field.planting_date else datetime.now()
        
        crop_params = self.decision_tree.crop_parameters.get(
            field.crop_type, 
            self.decision_tree.crop_parameters["corn"]
        )
        
        # Get weather forecast
        weather_request = WeatherForecastRequest(
            latitude=field.location_lat or 13.0,
            longitude=field.location_lon or 123.0,
            days=5
        )
        
        weather_data = self.weather_service.get_weather_forecast(db, weather_request)
        
        for operation in operations:
            days_to_add = crop_params["growth_stages"].get(operation, 7)
            proposed_date = current_date + timedelta(days=days_to_add)
            
            weather_suitability = self.weather_service.check_weather_suitability(
                weather_data, proposed_date, requires_dry_weather=True
            )
            
            if not weather_suitability["is_suitable"]:
                start_window = proposed_date - timedelta(days=7)
                end_window = proposed_date + timedelta(days=7)
                
                optimal_windows = self.weather_service.get_optimal_weather_window(
                    weather_data, start_window, end_window, requires_dry_weather=True
                )
                
                if optimal_windows:
                    optimal_window = next(
                        (w for w in optimal_windows if w["is_suitable"]), 
                        optimal_windows[0]
                    )
                    optimal_date = optimal_window["date"]
                else:
                    optimal_date = proposed_date
            else:
                optimal_date = proposed_date
            
            estimated_cost = self.decision_tree._estimate_operation_cost(
                operation, field.area_hectares
            )
            
            task_data = ScheduledTaskCreate(
                task_type=operation,
                task_name=f"{operation.replace('_', ' ').title()} - {field.name}",
                description=f"Automatically scheduled {operation} for {field.crop_type}",
                scheduled_date=optimal_date,
                estimated_cost=estimated_cost,
                requires_dry_weather=True,
                priority=self._calculate_priority(operation),
                field_id=field_id
            )
            
            task = self.create_scheduled_task(db, task_data, user_id)
            task.decision_tree_recommendation = True
            db.commit()
            
            scheduled_tasks.append(task)
            current_date = optimal_date
        
        return scheduled_tasks
    
    def _calculate_priority(self, operation: str) -> int:
        """Calculate priority level for operation"""
        priorities = {
            "land_preparation": 1,
            "planting": 2,
            "fertilization": 3,
            "irrigation": 4,
            "pest_control": 3,
            "harvesting": 1
        }
        return priorities.get(operation, 3)

    def _recommended_followup_date(
        self,
        db: Session,
        task: ScheduledTask,
        field: FarmField
    ) -> datetime:
        weather_request = WeatherForecastRequest(
            latitude=field.location_lat or 13.0,
            longitude=field.location_lon or 123.0,
            days=5
        )
        weather_data = self.weather_service.get_weather_forecast(db, weather_request)
        window_start = task.scheduled_date + timedelta(days=1)
        window_end = task.scheduled_date + timedelta(days=7)
        optimal_windows = self.weather_service.get_optimal_weather_window(
            weather_data, window_start, window_end, requires_dry_weather=task.requires_dry_weather
        )
        if optimal_windows:
            return optimal_windows[0]["date"]
        return task.scheduled_date + timedelta(days=1)

    def _build_tomorrow_notification_payload(
        self,
        db: Session,
        task: ScheduledTask,
        field: FarmField
    ) -> Dict[str, Any]:
        weather_request = WeatherForecastRequest(
            latitude=field.location_lat or 13.0,
            longitude=field.location_lon or 123.0,
            days=5
        )
        weather_data = self.weather_service.get_weather_forecast(db, weather_request)
        suitability = self.weather_service.check_weather_suitability(
            weather_data, task.scheduled_date, requires_dry_weather=task.requires_dry_weather
        )

        task.weather_check_date = datetime.utcnow()
        task.weather_status = "suitable" if suitability["is_suitable"] else "unsuitable"
        task.tomorrow_check_at = datetime.utcnow()

        scheduled_iso = task.scheduled_date.isoformat()
        base_payload = {
            "task_id": task.id,
            "field_id": task.field_id,
            "task_name": task.task_name,
            "scheduled_date": scheduled_iso,
            "weather_status": task.weather_status,
            "can_delay": True,
            "can_move": True,
        }

        if suitability["is_suitable"]:
            return {
                "kind": "task_upcoming_reminder",
                "title": "Upcoming Activity Reminder",
                "body": f"{task.task_name} is scheduled tomorrow.",
                "data": {
                    **base_payload,
                    "notification_kind": "task_upcoming_reminder",
                },
            }

        suggested_date = self._recommended_followup_date(db, task, field)
        reason_text = "; ".join(suitability.get("reasons", [])) or "Weather forecast is not suitable for this task."
        return {
            "kind": "task_weather_warning",
            "title": "Weather Warning For Tomorrow's Task",
            "body": f"{task.task_name} may be unsafe tomorrow: {reason_text}",
            "data": {
                **base_payload,
                "notification_kind": "task_weather_warning",
                "reason": reason_text,
                "risks": suitability.get("risks", []),
                "suggested_new_date": suggested_date.isoformat(),
                "recommended_delay_days": suitability.get("recommended_delay_days", 1),
            },
        }

    def process_tomorrow_task_notifications(
        self,
        db: Session,
        user_id: int,
        reference_time: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        now = reference_time or datetime.utcnow()
        target_date = now + timedelta(days=1)
        start = datetime(target_date.year, target_date.month, target_date.day)
        end = start + timedelta(days=1)

        tasks = db.query(ScheduledTask).filter(
            ScheduledTask.user_id == user_id,
            ScheduledTask.scheduled_date >= start,
            ScheduledTask.scheduled_date < end,
            ScheduledTask.status == "pending",
            ScheduledTask.is_deleted == False
        ).all()

        results: List[Dict[str, Any]] = []
        for task in tasks:
            field = db.query(FarmField).filter(
                FarmField.id == task.field_id,
                FarmField.owner_id == user_id,
                FarmField.is_deleted == False
            ).first()
            if not field:
                continue

            payload = self._build_tomorrow_notification_payload(db, task, field)
            notification_kind = payload["kind"]

            if (
                task.tomorrow_notification_sent_at
                and task.tomorrow_notification_sent_at >= start
                and task.tomorrow_notification_type == notification_kind
            ):
                results.append(
                    {
                        "task_id": task.id,
                        "notification_kind": notification_kind,
                        "status": "already_sent",
                        "scheduled_date": task.scheduled_date.isoformat(),
                    }
                )
                continue

            send_result = send_push_to_user(
                db=db,
                user_id=user_id,
                title=payload["title"],
                body=payload["body"],
                data={k: str(v) for k, v in payload["data"].items() if v is not None and not isinstance(v, list)},
                notification_type=notification_kind,
                notification_data=payload["data"],
            )
            task.tomorrow_notification_sent_at = datetime.utcnow()
            task.tomorrow_notification_type = notification_kind
            db.commit()

            results.append(
                {
                    "task_id": task.id,
                    "notification_kind": notification_kind,
                    "status": "sent",
                    "scheduled_date": task.scheduled_date.isoformat(),
                    "notification": payload,
                    "delivery": send_result,
                }
            )

        return results

    def process_tomorrow_task_notifications_for_all_users(
        self,
        db: Session,
        reference_time: Optional[datetime] = None
    ) -> Dict[int, List[Dict[str, Any]]]:
        user_ids = [
            row[0]
            for row in db.query(ScheduledTask.user_id).filter(
                ScheduledTask.status == "pending",
                ScheduledTask.is_deleted == False
            ).distinct().all()
            if row[0] is not None
        ]
        return {
            user_id: self.process_tomorrow_task_notifications(db, user_id, reference_time=reference_time)
            for user_id in user_ids
        }

    def delay_task(
        self,
        db: Session,
        task: ScheduledTask,
        delay_days: int
    ) -> ScheduledTask:
        if delay_days < 1:
            raise Exception("delay_days must be at least 1")

        if not task.original_scheduled_date:
            task.original_scheduled_date = task.scheduled_date
        task.scheduled_date = task.scheduled_date + timedelta(days=delay_days)
        task.status = "rescheduled"
        task.rescheduled_reason = f"Delayed by user action for {delay_days} day(s)."
        task.tomorrow_notification_sent_at = None
        task.tomorrow_notification_type = None
        db.commit()
        db.refresh(task)
        return task

    def move_task(
        self,
        db: Session,
        task: ScheduledTask,
        new_date: datetime
    ) -> ScheduledTask:
        if not task.original_scheduled_date:
            task.original_scheduled_date = task.scheduled_date
        task.scheduled_date = new_date
        task.status = "rescheduled"
        task.rescheduled_reason = "Moved by user action from notification workflow."
        task.tomorrow_notification_sent_at = None
        task.tomorrow_notification_type = None
        db.commit()
        db.refresh(task)
        return task

    def _task_to_timeline_item(self, task: ScheduledTask) -> Dict[str, Any]:
        return {
            "id": task.id,
            "task_type": task.task_type,
            "task_name": task.task_name,
            "description": task.description,
            "scheduled_date": task.scheduled_date,
            "original_scheduled_date": task.original_scheduled_date,
            "status": task.status,
            "priority": task.priority,
            "cycle_number": task.cycle_number,
            "cycle_day": task.cycle_day,
            "requires_dry_weather": task.requires_dry_weather,
            "rescheduled_reason": task.rescheduled_reason,
            "weather_status": task.weather_status,
            "completed_at": task.completed_at,
            "early_completed": task.early_completed,
            "early_completion_reason": task.early_completion_reason,
            "early_completion_warning_acknowledged": task.early_completion_warning_acknowledged,
            "early_completion_days": task.early_completion_days,
        }

    def calculate_farm_cycle_timeline(self, db: Session, field_id: int, user_id: Optional[int] = None) -> Dict[str, Any]:
        """Return frontend-ready grouped timeline for crop cycles."""
        field = db.query(FarmField).filter(FarmField.id == field_id).first()
        if not field:
            raise Exception(f"Field with id {field_id} not found")

        query = db.query(ScheduledTask).filter(
            ScheduledTask.field_id == field_id,
            ScheduledTask.is_deleted == False
        )
        if user_id is not None:
            query = query.filter(ScheduledTask.user_id == user_id)

        tasks = query.order_by(ScheduledTask.scheduled_date.asc()).all()

        cycle_1: List[Dict[str, Any]] = []
        cycle_2: List[Dict[str, Any]] = []
        ungrouped: List[Dict[str, Any]] = []

        for task in tasks:
            item = self._task_to_timeline_item(task)
            if task.cycle_number == 1:
                cycle_1.append(item)
            elif task.cycle_number == 2:
                cycle_2.append(item)
            else:
                ungrouped.append(item)

        def _sort_key(t: Dict[str, Any]):
            cycle_day = t.get("cycle_day")
            scheduled_date = t.get("scheduled_date")
            return (cycle_day if cycle_day is not None else 10**6, scheduled_date)

        cycle_1.sort(key=_sort_key)
        cycle_2.sort(key=_sort_key)
        ungrouped.sort(key=_sort_key)

        crop_type = field.crop_type.value if field.crop_type else None
        corn_profile = self._get_corn_profile(field.corn_type, field.crop_variety) if crop_type == "corn" else None
        cycle_1_end_day = corn_profile["prep_days"] if corn_profile else 21
        cycle_1_label = "Land Preparation to Planting" if crop_type == "corn" else "Land Preparation"
        cycle_2_label = "Planting to Harvest" if crop_type == "corn" else "Planting to Harvest"

        return {
            "field_id": field_id,
            "field_name": field.name,
            "crop_type": crop_type,
            "crop_variety": field.crop_variety,
            "expected_harvest_date": field.expected_harvest_date,
            "cycle_1": {
                "label": cycle_1_label,
                "start_day": 0,
                "end_day": cycle_1_end_day,
                "tasks": cycle_1
            },
            "cycle_2": {
                "label": cycle_2_label,
                "start_day": 0,
                "end_day": max([t.get("cycle_day", 0) or 0 for t in cycle_2], default=0),
                "tasks": cycle_2
            },
            "ungrouped_tasks": ungrouped
        }
