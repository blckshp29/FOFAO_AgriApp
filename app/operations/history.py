import json
from datetime import datetime
from typing import Dict, List, Optional

from sqlalchemy.orm import Session

from ..models import CompletedOperationHistory, CropProject, Field, FinancialRecord, ProjectStatus, ScheduledTask


def _normalize_category(category: Optional[str]) -> str:
    if not category:
        return "Miscellaneous"
    value = category.strip().lower()
    if value in {"fertilizer", "fertilizers"}:
        return "Fertilizers"
    if value in {"chemical", "chemicals"}:
        return "Chemicals"
    if value in {"seed", "seeds"}:
        return "Seeds"
    if value in {"labor", "labour"}:
        return "Labor"
    if value in {"land prep", "land preparation", "land_preparation"}:
        return "Land Preparation"
    if value in {"irrigation"}:
        return "Irrigation"
    if value in {"misc", "miscellaneous", "others"}:
        return "Miscellaneous"
    return category.strip().title()


def _serialize_task_history(tasks: List[ScheduledTask]) -> List[Dict]:
    history: List[Dict] = []
    for task in tasks:
        history.append(
            {
                "id": task.id,
                "task_type": task.task_type.value if hasattr(task.task_type, "value") else task.task_type,
                "task_name": task.task_name,
                "description": task.description,
                "scheduled_date": task.scheduled_date.isoformat() if task.scheduled_date else None,
                "original_scheduled_date": task.original_scheduled_date.isoformat() if task.original_scheduled_date else None,
                "status": task.status,
                "estimated_cost": task.estimated_cost,
                "actual_cost": task.actual_cost,
                "priority": task.priority,
                "cycle_number": task.cycle_number,
                "cycle_day": task.cycle_day,
                "completed_at": task.completed_at.isoformat() if task.completed_at else None,
                "decision_tree_recommendation": task.decision_tree_recommendation,
                "early_completed": task.early_completed,
                "early_completion_reason": task.early_completion_reason,
                "early_completion_warning_acknowledged": task.early_completion_warning_acknowledged,
                "early_completion_days": task.early_completion_days,
            }
        )
    return history


def _location_string(field: Field) -> Optional[str]:
    if field.farm:
        parts = [
            field.farm.barangay,
            field.farm.city_municipality,
            field.farm.province,
            field.farm.location,
        ]
        resolved = ", ".join(part for part in parts if part)
        if resolved:
            return resolved
    if field.location_lat is not None and field.location_lon is not None:
        return f"{field.location_lat}, {field.location_lon}"
    return None


def _field_financial_snapshot(
    db: Session,
    owner_id: int,
    field_id: int,
    project: Optional[CropProject],
) -> Dict:
    expense_query = db.query(FinancialRecord).filter(
        FinancialRecord.owner_id == owner_id,
        FinancialRecord.is_history == False,
        FinancialRecord.transaction_type == "expense",
        FinancialRecord.field_id == field_id,
    )
    income_query = db.query(FinancialRecord).filter(
        FinancialRecord.owner_id == owner_id,
        FinancialRecord.is_history == False,
        FinancialRecord.transaction_type == "income",
        FinancialRecord.field_id == field_id,
    )

    if project:
        expense_query = expense_query.filter(FinancialRecord.project_id == project.id)
        income_query = income_query.filter(FinancialRecord.project_id == project.id)

    expenses = expense_query.all()
    incomes = income_query.all()

    category_costs: Dict[str, float] = {}
    for record in expenses:
        category = _normalize_category(record.category)
        category_costs[category] = round(category_costs.get(category, 0.0) + (record.amount or 0.0), 2)

    return {
        "planned_budget": round((project.budget_total if project else 0.0) or 0.0, 2),
        "budget_remaining": round((project.budget_remaining if project else 0.0) or 0.0, 2),
        "actual_cost": round(sum(record.amount or 0.0 for record in expenses), 2),
        "actual_revenue": round(sum(record.amount or 0.0 for record in incomes), 2),
        "category_costs": category_costs,
        "income_records": len(incomes),
        "expense_records": len(expenses),
    }


def _resolve_project(db: Session, field: Field) -> Optional[CropProject]:
    return (
        db.query(CropProject)
        .filter(
            CropProject.field_id == field.id,
            CropProject.owner_id == field.owner_id,
            CropProject.is_deleted == False,
        )
        .order_by(CropProject.created_at.desc())
        .first()
    )


def create_completed_operation_history(
    db: Session,
    field: Field,
    completed_at: Optional[datetime] = None,
) -> CompletedOperationHistory:
    completion_time = completed_at or field.completed_at or datetime.utcnow()

    existing = (
        db.query(CompletedOperationHistory)
        .filter(
            CompletedOperationHistory.field_id == field.id,
            CompletedOperationHistory.completed_at == completion_time,
        )
        .first()
    )
    if existing:
        return existing

    tasks = (
        db.query(ScheduledTask)
        .filter(
            ScheduledTask.field_id == field.id,
            ScheduledTask.is_deleted == False,
        )
        .order_by(ScheduledTask.scheduled_date.asc(), ScheduledTask.id.asc())
        .all()
    )
    project = _resolve_project(db, field)
    financial_snapshot = _field_financial_snapshot(db, field.owner_id, field.id, project)

    start_candidates = [
        field.land_prep_start_date,
        field.planting_date,
        project.start_date if project else None,
        tasks[0].scheduled_date if tasks else None,
    ]
    start_date = next((candidate for candidate in start_candidates if candidate is not None), None)
    season_year = (start_date or completion_time).year
    sequence = (
        db.query(CompletedOperationHistory)
        .filter(CompletedOperationHistory.field_id == field.id)
        .count()
        + 1
    )
    crop_type = field.crop_type.value if hasattr(field.crop_type, "value") else field.crop_type
    history = CompletedOperationHistory(
        owner_id=field.owner_id,
        field_id=field.id,
        farm_id=field.farm_id,
        project_id=project.id if project else None,
        crop_type=field.crop_type,
        crop_variety=field.crop_variety,
        season_label=f"{crop_type.title()} Season {sequence} ({season_year})" if crop_type else f"Season {sequence} ({season_year})",
        season_year=season_year,
        start_date=start_date,
        completed_at=completion_time,
        planned_budget=financial_snapshot["planned_budget"],
        actual_cost=financial_snapshot["actual_cost"],
        actual_yield=field.actual_yield or 0.0,
        actual_revenue=financial_snapshot["actual_revenue"] or field.gross_revenue or 0.0,
        location=_location_string(field),
        task_history=json.dumps(_serialize_task_history(tasks)),
        category_costs=json.dumps(financial_snapshot["category_costs"]),
        financial_snapshot=json.dumps(financial_snapshot),
    )
    db.add(history)
    return history


def _latest_task_completion(tasks: List[ScheduledTask]) -> Optional[datetime]:
    timestamps = [task.completed_at or task.scheduled_date for task in tasks if (task.completed_at or task.scheduled_date)]
    return max(timestamps) if timestamps else None


def mark_related_projects_completed(db: Session, field: Field, completed_at: datetime) -> None:
    projects = (
        db.query(CropProject)
        .filter(
            CropProject.field_id == field.id,
            CropProject.owner_id == field.owner_id,
            CropProject.is_deleted == False,
            CropProject.status.in_([ProjectStatus.PLANNED, ProjectStatus.ACTIVE]),
        )
        .all()
    )
    for project in projects:
        project.status = ProjectStatus.COMPLETED
        project.completed_at = project.completed_at or completed_at
        project.end_date = project.end_date or completed_at


def sync_field_completion_from_tasks(db: Session, field: Field) -> bool:
    tasks = (
        db.query(ScheduledTask)
        .filter(
            ScheduledTask.field_id == field.id,
            ScheduledTask.is_deleted == False,
        )
        .all()
    )
    if not tasks or any(task.status != "completed" for task in tasks):
        return False

    completed_at = _latest_task_completion(tasks) or datetime.utcnow()
    field.operation_status = "completed"
    field.status = "completed"
    field.completed_at = field.completed_at or completed_at
    mark_related_projects_completed(db, field, field.completed_at)
    create_completed_operation_history(db, field, field.completed_at)
    return True


def mark_field_completed(
    db: Session,
    field: Field,
    completed_at: Optional[datetime] = None,
) -> CompletedOperationHistory:
    field.operation_status = "completed"
    field.status = "completed"
    field.completed_at = completed_at or field.completed_at or datetime.utcnow()
    mark_related_projects_completed(db, field, field.completed_at)
    return create_completed_operation_history(db, field, field.completed_at)


def reopen_field_operation(field: Field) -> None:
    field.operation_status = "ongoing"
    if field.status == "completed":
        field.status = "ongoing"
    field.completed_at = None
