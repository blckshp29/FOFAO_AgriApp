from fastapi import APIRouter, Depends, HTTPException, status
from fastapi import Query
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime, timedelta

from .. import models, schemas
from ..database import get_db
from ..models import FinancialRecord, User, Field as FieldModel, CropProject
from ..schemas import (
    FinancialRecordCreate,
    FinancialRecord as FinancialRecordSchema,
    CropProjectCreate,
    CropProjectUpdate,
    CropProject as CropProjectSchema,
)
from ..financial.partial_budgeting import PartialBudgeting
from ..notifications.service import send_push_to_user
from ..schemas import PartialBudgetingInput, PartialBudgetingResponse
from .auth import get_current_user

router = APIRouter()
partial_budgeting = PartialBudgeting()
SEEDED_HISTORY_DESCRIPTION = "Seeded historical budget record"

DEFAULT_HISTORICAL_CATEGORIES = [
    "Land Preparation",
    "Seeds",
    "Fertilizers",
    "Chemicals",
    "Labor",
    "Miscellaneous",
]

RICE_BASE_COST_COMPONENTS = [
    ("Land Preparation", 11000.0, 14000.0),
    ("Seeds", 2500.0, 3500.0),
    ("Fertilizers", 18000.0, 22000.0),
    ("Chemicals", 13000.0, 17000.0),
    ("Labor", 11000.0, 14000.0),
    ("Miscellaneous", 3000.0, 5000.0),
]

CORN_COST_TEMPLATES = {
    "yellow": {
        "label": "Yellow Corn",
        "seeds": (4500.0, 6000.0),
        "returns": {
            "unit": "kg",
            "yield_min": 4000.0,
            "yield_max": 6000.0,
            "price_min": 13.0,
            "price_max": 16.0,
        },
    },
    "white": {
        "label": "White Corn",
        "seeds": (3000.0, 4500.0),
        "returns": {
            "unit": "kg",
            "yield_min": 3000.0,
            "yield_max": 5000.0,
            "price_min": 14.0,
            "price_max": 18.0,
        },
    },
    "sweet": {
        "label": "Sweet Corn",
        "seeds": (5000.0, 7000.0),
        "returns": {
            "unit": "ear",
            "yield_min": 15000.0,
            "yield_max": 20000.0,
            "price_min": 5.0,
            "price_max": 10.0,
        },
    },
}

CORN_BASE_COST_COMPONENTS = [
    ("Land Preparation", 5000.0, 7000.0),
    ("Fertilizers", 8000.0, 12000.0),
    ("Chemicals", 2500.0, 4500.0),
    ("Labor", 8500.0, 13500.0),
    ("Irrigation", 1500.0, 3000.0),
]

COCONUT_HARVEST_COST_COMPONENTS = [
    ("Coconut Climber", 500.0, 1600.0),
    ("Helper / Tagakuha", 300.0, 1000.0),
    ("Manual Hauling", 300.0, 800.0),
    ("Tricycle / Small Truck", 500.0, 1200.0),
]

VEGETABLE_BUDGET_TEMPLATES = {
    "eggplant": {
        "label": "Eggplant",
        "budget_min": 30000.0,
        "budget_max": 40000.0,
        "costs": [
            ("Land Preparation", 6000.0, 6000.0),
            ("Seeds / Seedlings", 3000.0, 3000.0),
            ("Fertilizer", 10000.0, 10000.0),
            ("Pesticides", 5000.0, 5000.0),
            ("Labor", 12000.0, 12000.0),
        ],
    },
    "tomato": {
        "label": "Tomato",
        "budget_min": 35000.0,
        "budget_max": 50000.0,
        "costs": [
            ("Land Preparation", 6000.0, 6000.0),
            ("Seeds", 2500.0, 2500.0),
            ("Fertilizer", 12000.0, 12000.0),
            ("Pesticides", 6000.0, 6000.0),
            ("Labor", 15000.0, 15000.0),
        ],
    },
    "ampalaya": {
        "label": "Ampalaya",
        "budget_min": 45000.0,
        "budget_max": 65000.0,
        "costs": [
            ("Land Preparation", 6000.0, 6000.0),
            ("Seeds", 3000.0, 3000.0),
            ("Trellis Materials", 10000.0, 10000.0),
            ("Fertilizer", 10000.0, 10000.0),
            ("Labor", 15000.0, 15000.0),
        ],
    },
    "okra": {
        "label": "Okra",
        "budget_min": 25000.0,
        "budget_max": 35000.0,
        "costs": [
            ("Land Preparation", 5000.0, 5000.0),
            ("Seeds", 2000.0, 2000.0),
            ("Fertilizer", 8000.0, 8000.0),
            ("Pesticides", 3000.0, 3000.0),
            ("Labor", 10000.0, 10000.0),
        ],
    },
    "string beans": {
        "label": "String Beans",
        "budget_min": 38000.0,
        "budget_max": 55000.0,
        "costs": [
            ("Land Preparation", 6000.0, 6000.0),
            ("Seeds", 3000.0, 3000.0),
            ("Trellis", 8000.0, 8000.0),
            ("Fertilizer", 9000.0, 9000.0),
            ("Labor", 12000.0, 12000.0),
        ],
    },
    "squash": {
        "label": "Squash",
        "budget_min": 25000.0,
        "budget_max": 35000.0,
        "costs": [
            ("Land Preparation", 5000.0, 5000.0),
            ("Seeds", 2500.0, 2500.0),
            ("Fertilizer", 8000.0, 8000.0),
            ("Pesticides", 3000.0, 3000.0),
            ("Labor", 10000.0, 10000.0),
        ],
    },
    "chili": {
        "label": "Chili",
        "budget_min": 40000.0,
        "budget_max": 60000.0,
        "costs": [
            ("Land Preparation", 6000.0, 6000.0),
            ("Seeds", 3000.0, 3000.0),
            ("Fertilizer", 10000.0, 10000.0),
            ("Pesticides", 6000.0, 6000.0),
            ("Labor", 15000.0, 15000.0),
        ],
    },
    "leafy vegetables": {
        "label": "Leafy Vegetables",
        "budget_min": 20000.0,
        "budget_max": 30000.0,
        "costs": [
            ("Land Preparation", 4000.0, 4000.0),
            ("Seeds", 1500.0, 1500.0),
            ("Fertilizer", 6000.0, 6000.0),
            ("Pesticides", 2000.0, 2000.0),
            ("Labor", 8000.0, 8000.0),
        ],
    },
}

def _normalize_corn_type(raw: Optional[str]) -> str:
    if not raw:
        return "yellow"
    value = raw.strip().lower()
    if "sweet" in value or "sugar king" in value or "honey" in value:
        return "sweet"
    if "white" in value or "laguna" in value or "ipb" in value:
        return "white"
    return "yellow"

def _normalize_vegetable_type(raw: Optional[str]) -> str:
    if not raw:
        return "eggplant"
    value = raw.strip().lower()
    if "eggplant" in value or "talong" in value:
        return "eggplant"
    if "tomato" in value or "kamatis" in value:
        return "tomato"
    if "ampalaya" in value or "bitter gourd" in value:
        return "ampalaya"
    if "okra" in value:
        return "okra"
    if "string beans" in value or "string bean" in value or "sitao" in value:
        return "string beans"
    if "squash" in value or "kalabasa" in value:
        return "squash"
    if "chili" in value or "sili" in value or "chilli" in value:
        return "chili"
    if "leafy" in value or "pechay" in value or "mustasa" in value:
        return "leafy vegetables"
    return "eggplant"

def _normalize_rice_variety(raw: Optional[str]) -> str:
    if not raw:
        return "NSIC RC222"
    return " ".join(raw.strip().upper().split())

def _build_rice_budget_template(crop_variety: Optional[str], hectares: float) -> schemas.RiceBudgetTemplateResponse:
    hectares = hectares if hectares and hectares > 0 else 1.0
    normalized_variety = _normalize_rice_variety(crop_variety)

    allocations = []
    budget_min = 0.0
    budget_max = 0.0
    budget_recommended = 0.0

    for category, min_amount, max_amount in RICE_BASE_COST_COMPONENTS:
        min_scaled = round(min_amount * hectares, 2)
        max_scaled = round(max_amount * hectares, 2)
        recommended_scaled = round(((min_amount + max_amount) / 2.0) * hectares, 2)
        budget_min += min_scaled
        budget_max += max_scaled
        budget_recommended += recommended_scaled
        allocations.append(
            {
                "category": category,
                "min_amount": min_scaled,
                "max_amount": max_scaled,
                "recommended_amount": recommended_scaled,
            }
        )

    allocations = [
        {
            **item,
            "percent_of_total": round(
                (item["recommended_amount"] / budget_recommended * 100.0) if budget_recommended > 0 else 0.0,
                2,
            ),
        }
        for item in allocations
    ]

    return schemas.RiceBudgetTemplateResponse(
        crop_type="rice",
        crop_variety=normalized_variety,
        hectares=round(hectares, 2),
        currency="PHP",
        budget_min=round(budget_min, 2),
        budget_max=round(budget_max, 2),
        budget_recommended=round(budget_recommended, 2),
        allocations=allocations,
    )

def _build_corn_budget_template(corn_type: Optional[str], hectares: float) -> schemas.CornBudgetTemplateResponse:
    hectares = hectares if hectares and hectares > 0 else 1.0
    profile_key = _normalize_corn_type(corn_type)
    profile = CORN_COST_TEMPLATES[profile_key]

    raw_allocations = [("Seeds", *profile["seeds"]), *CORN_BASE_COST_COMPONENTS]
    scaled_allocations = []
    budget_min = 0.0
    budget_max = 0.0
    budget_recommended = 0.0

    for category, min_amount, max_amount in raw_allocations:
        min_scaled = round(min_amount * hectares, 2)
        max_scaled = round(max_amount * hectares, 2)
        recommended_scaled = round(((min_amount + max_amount) / 2.0) * hectares, 2)
        budget_min += min_scaled
        budget_max += max_scaled
        budget_recommended += recommended_scaled
        scaled_allocations.append(
            {
                "category": category,
                "min_amount": min_scaled,
                "max_amount": max_scaled,
                "recommended_amount": recommended_scaled,
            }
        )

    allocations = []
    for item in scaled_allocations:
        percent = (item["recommended_amount"] / budget_recommended * 100.0) if budget_recommended > 0 else 0.0
        allocations.append(
            {
                **item,
                "percent_of_total": round(percent, 2),
            }
        )

    returns = profile["returns"]
    yield_min = round(returns["yield_min"] * hectares, 2)
    yield_max = round(returns["yield_max"] * hectares, 2)
    yield_recommended = round(((returns["yield_min"] + returns["yield_max"]) / 2.0) * hectares, 2)
    price_recommended = round((returns["price_min"] + returns["price_max"]) / 2.0, 2)

    return schemas.CornBudgetTemplateResponse(
        crop_type="corn",
        corn_type=profile["label"],
        hectares=round(hectares, 2),
        currency="PHP",
        budget_min=round(budget_min, 2),
        budget_max=round(budget_max, 2),
        budget_recommended=round(budget_recommended, 2),
        allocations=allocations,
        expected_returns=schemas.CornYieldIncomeEstimate(
            unit=returns["unit"],
            yield_min=yield_min,
            yield_max=yield_max,
            yield_recommended=yield_recommended,
            price_min=returns["price_min"],
            price_max=returns["price_max"],
            price_recommended=price_recommended,
            income_min=round(yield_min * returns["price_min"], 2),
            income_max=round(yield_max * returns["price_max"], 2),
            income_recommended=round(yield_recommended * price_recommended, 2),
        ),
    )

def _build_coconut_budget_template(hectares: float) -> schemas.CoconutBudgetTemplateResponse:
    hectares = hectares if hectares and hectares > 0 else 1.0

    allocations = []
    budget_min = 0.0
    budget_max = 0.0
    budget_recommended = 0.0

    for category, min_amount, max_amount in COCONUT_HARVEST_COST_COMPONENTS:
        min_scaled = round(min_amount * hectares, 2)
        max_scaled = round(max_amount * hectares, 2)
        recommended_scaled = round(((min_amount + max_amount) / 2.0) * hectares, 2)
        budget_min += min_scaled
        budget_max += max_scaled
        budget_recommended += recommended_scaled
        allocations.append(
            {
                "category": category,
                "min_amount": min_scaled,
                "max_amount": max_scaled,
                "recommended_amount": recommended_scaled,
            }
        )

    allocations = [
        {
            **item,
            "percent_of_total": round(
                (item["recommended_amount"] / budget_recommended * 100.0) if budget_recommended > 0 else 0.0,
                2,
            ),
        }
        for item in allocations
    ]

    return schemas.CoconutBudgetTemplateResponse(
        crop_type="coconut",
        hectares=round(hectares, 2),
        currency="PHP",
        budget_min=round(budget_min, 2),
        budget_max=round(budget_max, 2),
        budget_recommended=round(budget_recommended, 2),
        allocations=allocations,
    )

def _build_vegetable_budget_template(vegetable_type: Optional[str], hectares: float) -> schemas.VegetableBudgetTemplateResponse:
    hectares = hectares if hectares and hectares > 0 else 1.0
    vegetable_key = _normalize_vegetable_type(vegetable_type)
    template = VEGETABLE_BUDGET_TEMPLATES[vegetable_key]

    recommended_base_total = sum(((min_amount + max_amount) / 2.0) for _, min_amount, max_amount in template["costs"])
    if recommended_base_total <= 0:
        recommended_base_total = 1.0

    allocations = []
    for category, min_amount, max_amount in template["costs"]:
        recommended_amount = (min_amount + max_amount) / 2.0
        scaled_recommended = round(recommended_amount * hectares, 2)
        allocations.append(
            {
                "category": category,
                "min_amount": round(min_amount * hectares, 2),
                "max_amount": round(max_amount * hectares, 2),
                "recommended_amount": scaled_recommended,
                "percent_of_total": round((recommended_amount / recommended_base_total) * 100.0, 2),
            }
        )

    return schemas.VegetableBudgetTemplateResponse(
        crop_type="vegetables",
        vegetable_type=template["label"],
        hectares=round(hectares, 2),
        currency="PHP",
        budget_min=round(template["budget_min"] * hectares, 2),
        budget_max=round(template["budget_max"] * hectares, 2),
        budget_recommended=round(((template["budget_min"] + template["budget_max"]) / 2.0) * hectares, 2),
        allocations=allocations,
    )

def _normalize_category(cat: str) -> str:
    if not cat:
        return "Miscellaneous"
    c = cat.strip().lower()
    if c in {"fertilizer", "fertilizers"}:
        return "Fertilizers"
    if c in {"chemical", "chemicals"}:
        return "Chemicals"
    if c in {"seed", "seeds"}:
        return "Seeds"
    if c in {"labor", "labour"}:
        return "Labor"
    if c in {"land prep", "land preparation", "land_preparation"}:
        return "Land Preparation"
    if c in {"misc", "miscellaneous", "others"}:
        return "Miscellaneous"
    # Title-case fallback
    return cat.strip().title()

def _calculate_historical_allocations(db: Session, user_id: int, budget_total: float = 0.0):
    history_count = db.query(FinancialRecord).filter(
        FinancialRecord.owner_id == user_id,
        FinancialRecord.transaction_type == "expense",
        FinancialRecord.is_history == True,
        FinancialRecord.description != SEEDED_HISTORY_DESCRIPTION
    ).count()

    base_query = db.query(FinancialRecord).filter(
        FinancialRecord.owner_id == user_id,
        FinancialRecord.transaction_type == "expense",
        FinancialRecord.is_history == True,
        FinancialRecord.description != SEEDED_HISTORY_DESCRIPTION
    )

    records = base_query.all()

    totals = {}
    total_spend = 0.0
    for r in records:
        cat = _normalize_category(r.category)
        totals[cat] = totals.get(cat, 0.0) + (r.amount or 0.0)
        total_spend += (r.amount or 0.0)

    # Ensure known categories exist (even if zero)
    for cat in DEFAULT_HISTORICAL_CATEGORIES:
        totals.setdefault(cat, 0.0)

    allocations = []
    for cat, amt in totals.items():
        pct = (amt / total_spend * 100.0) if total_spend > 0 else 0.0
        allocated_amount = (budget_total * (pct / 100.0)) if budget_total > 0 else 0.0
        allocations.append({
            "category": cat,
            "historical_cost": round(amt, 2),
            "percent_of_total": round(pct, 2),
            "allocated_amount": round(allocated_amount, 2)
        })

    allocations.sort(key=lambda x: x["percent_of_total"], reverse=True)
    return {
        "total_historical_spend": round(total_spend, 2),
        "used_history_records": history_count > 0,
        "budget_total": round(budget_total, 2),
        "allocations": allocations
    }

@router.post("/financial/records", response_model=FinancialRecordSchema)
def create_financial_record(
    record: FinancialRecordCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    record_data = record.model_dump()
    linked_field = None
    if record.field_id:
        linked_field = db.query(FieldModel).filter(
            FieldModel.id == record.field_id,
            FieldModel.owner_id == current_user.id
        ).first()
        if not linked_field:
            raise HTTPException(status_code=404, detail="Field not found")

    db_record = FinancialRecord(**record_data, owner_id=current_user.id, user_id=current_user.id)
    
    # Update project budget and totals if linked
    if record.project_id:
        project = db.query(CropProject).filter(
            CropProject.id == record.project_id,
            CropProject.owner_id == current_user.id
        ).first()
        if not project:
            raise HTTPException(status_code=404, detail="Project not found")
        if linked_field and project.field_id and project.field_id != linked_field.id:
            raise HTTPException(status_code=400, detail="field_id does not match the selected project")
        
        db_record.budget_snapshot = project.budget_remaining if project.budget_remaining is not None else project.budget_total
        
        transaction_type = record.transaction_type.value if hasattr(record.transaction_type, "value") else record.transaction_type
        if transaction_type == "expense":
            # Historical allocation check (Node C)
            allocations = _calculate_historical_allocations(db, current_user.id, project.budget_total or 0)
            if allocations["total_historical_spend"] > 0:
                # Find historical percent for this category (or misc)
                category = _normalize_category(record.category or "Miscellaneous")
                hist = next((a for a in allocations["allocations"] if a["category"] == category), None)
                hist_pct = hist["percent_of_total"] if hist else 0.0
                allocated_budget = (project.budget_total or 0) * (hist_pct / 100.0)
                if record.amount > allocated_budget and not record.over_budget_approved:
                    raise HTTPException(
                        status_code=409,
                        detail={
                            "message": "Expense exceeds historical allocation for this category.",
                            "category": category,
                            "historical_percent": hist_pct,
                            "allocated_budget": round(allocated_budget, 2),
                            "expense_amount": record.amount
                        }
                    )
            if project.budget_remaining is None:
                project.budget_remaining = project.budget_total or 0
            if record.amount > (project.budget_remaining or 0):
                if not record.over_budget_approved:
                    raise HTTPException(
                        status_code=409,
                        detail={
                            "message": "Expense exceeds remaining budget. Confirm to proceed.",
                            "budget_remaining": project.budget_remaining or 0,
                            "expense_amount": record.amount
                        }
                    )
                db_record.is_over_budget = True
                db_record.over_budget_approved = True
            project.expense_total = (project.expense_total or 0) + record.amount
            project.budget_remaining = (project.budget_remaining or 0) - record.amount
        elif transaction_type == "income":
            project.income_total = (project.income_total or 0) + record.amount

    db.add(db_record)
    db.commit()
    db.refresh(db_record)
    if db_record.is_over_budget:
        send_push_to_user(
            db=db,
            user_id=current_user.id,
            title="Budget Exceeded Warning",
            body=f"Expense of {db_record.amount:.2f} exceeded your remaining project budget.",
            data={
                "event": "budget_exceeded",
                "record_id": str(db_record.id),
                "project_id": str(db_record.project_id or ""),
                "amount": str(db_record.amount),
            },
            notification_type="budget_alert",
        )
    return db_record

@router.post("/financial/records/confirm-over-budget", response_model=FinancialRecordSchema)
def confirm_over_budget_record(
    record: FinancialRecordCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    if not record.project_id:
        raise HTTPException(status_code=400, detail="project_id is required for over-budget confirmation")
    record.over_budget_approved = True
    return create_financial_record(record, db, current_user)

@router.get("/financial/budget/allocation")
def get_historical_budget_allocation(
    project_id: Optional[int] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    budget_total = 0.0
    if project_id:
        project = db.query(CropProject).filter(
            CropProject.id == project_id,
            CropProject.owner_id == current_user.id
        ).first()
        if not project:
            raise HTTPException(status_code=404, detail="Project not found")
        budget_total = project.budget_total or 0.0
    return _calculate_historical_allocations(db, current_user.id, budget_total)

@router.get("/financial/budget/corn-template", response_model=schemas.CornBudgetTemplateResponse)
def get_corn_budget_template(
    corn_type: str = "yellow",
    hectares: float = 1.0,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    return _build_corn_budget_template(corn_type, hectares)

@router.get("/financial/budget/rice-template", response_model=schemas.RiceBudgetTemplateResponse)
def get_rice_budget_template(
    crop_variety: Optional[str] = None,
    variety: Optional[str] = Query(default=None),
    hectares: float = 1.0,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    selected_variety = crop_variety or variety
    return _build_rice_budget_template(selected_variety, hectares)

@router.get("/financial/budget/coconut-template", response_model=schemas.CoconutBudgetTemplateResponse)
def get_coconut_budget_template(
    hectares: float = 1.0,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    return _build_coconut_budget_template(hectares)

@router.get("/financial/budget/vegetable-template", response_model=schemas.VegetableBudgetTemplateResponse)
def get_vegetable_budget_template(
    vegetable_type: str = "eggplant",
    hectares: float = 1.0,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    return _build_vegetable_budget_template(vegetable_type, hectares)

@router.post("/financial/budget/check")
def check_budget_logic(
    category: str,
    requested_amount: float,
    project_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    project = db.query(CropProject).filter(
        CropProject.id == project_id,
        CropProject.owner_id == current_user.id
    ).first()
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    allocations = _calculate_historical_allocations(db, current_user.id, project.budget_total or 0)
    if allocations["total_historical_spend"] <= 0:
        return {"decision": "APPROVE", "reason": "No historical data available."}

    category = _normalize_category(category)
    hist = next((a for a in allocations["allocations"] if a["category"] == category), None)
    hist_pct = hist["percent_of_total"] if hist else 0.0
    allowed_limit = (project.budget_total or 0) * (hist_pct / 100.0)

    if requested_amount > allowed_limit:
        return {
            "decision": "REJECT",
            "reason": f"Insufficient Funds. Historical limit for {category} is ₱{allowed_limit:.2f}",
            "suggested": round(allowed_limit, 2),
            "historical_percent": hist_pct
        }
    return {"decision": "APPROVE", "reason": "Within historical budget limits."}

@router.post("/financial/budget/seed")
def seed_historical_budget(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    raise HTTPException(
        status_code=410,
        detail="Historical budget seeding has been disabled. Transactions must be created by the user."
    )

@router.get("/financial/insights/summary", response_model=schemas.InsightSummary)
def get_financial_insight_summary(
    project_id: Optional[int] = None,
    field_id: Optional[int] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    if project_id:
        project = db.query(CropProject).filter(
            CropProject.id == project_id,
            CropProject.owner_id == current_user.id
        ).first()
        if not project:
            raise HTTPException(status_code=404, detail="Project not found")
        budget_total = project.budget_total or 0
        expenses_total = project.expense_total or 0
        income_total = project.income_total or 0
    elif field_id:
        field = db.query(FieldModel).filter(
            FieldModel.id == field_id,
            FieldModel.owner_id == current_user.id
        ).first()
        if not field:
            raise HTTPException(status_code=404, detail="Field not found")
        budget_total = db.query(CropProject).filter(
            CropProject.owner_id == current_user.id,
            CropProject.field_id == field_id
        ).with_entities(CropProject.budget_total).all()
        budget_total = sum(b[0] or 0 for b in budget_total)
        expenses_total = db.query(FinancialRecord).filter(
            FinancialRecord.owner_id == current_user.id,
            FinancialRecord.transaction_type == "expense",
            FinancialRecord.is_history == False,
            FinancialRecord.field_id == field_id
        ).with_entities(FinancialRecord.amount).all()
        expenses_total = sum(e[0] or 0 for e in expenses_total)
        income_total = db.query(FinancialRecord).filter(
            FinancialRecord.owner_id == current_user.id,
            FinancialRecord.transaction_type == "income",
            FinancialRecord.is_history == False,
            FinancialRecord.field_id == field_id
        ).with_entities(FinancialRecord.amount).all()
        income_total = sum(i[0] or 0 for i in income_total)
    else:
        budget_total = db.query(CropProject).filter(
            CropProject.owner_id == current_user.id
        ).with_entities(CropProject.budget_total).all()
        budget_total = sum(b[0] or 0 for b in budget_total)
        expenses_total = db.query(FinancialRecord).filter(
            FinancialRecord.owner_id == current_user.id,
            FinancialRecord.transaction_type == "expense",
            FinancialRecord.is_history == False
        ).with_entities(FinancialRecord.amount).all()
        expenses_total = sum(e[0] or 0 for e in expenses_total)
        income_total = db.query(FinancialRecord).filter(
            FinancialRecord.owner_id == current_user.id,
            FinancialRecord.transaction_type == "income",
            FinancialRecord.is_history == False
        ).with_entities(FinancialRecord.amount).all()
        income_total = sum(i[0] or 0 for i in income_total)

    net_profit = income_total - expenses_total
    max_value = max(budget_total, expenses_total, income_total, 1)

    def bar(label: str, value: float):
        return {"label": label, "percent": (value / max_value) * 100, "value": value}

    return {
        "budget_total": budget_total,
        "expenses_total": expenses_total,
        "income_total": income_total,
        "net_profit": net_profit,
        "is_over_budget": expenses_total > budget_total,
        "budget_bar": bar("Budget", budget_total),
        "expenses_bar": bar("Expenses", expenses_total),
        "income_bar": bar("Income", income_total),
    }

@router.get("/financial/insights/compare", response_model=schemas.InsightComparison)
def compare_financial_insights(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    projects = db.query(CropProject).filter(
        CropProject.owner_id == current_user.id
    ).order_by(CropProject.start_date.desc().nullslast(), CropProject.created_at.desc()).limit(2).all()

    if len(projects) < 2:
        raise HTTPException(status_code=400, detail="Not enough projects to compare")

    current = projects[0]
    previous = projects[1]

    def to_percent(expenses: float, net_profit: float):
        base = max(expenses + max(net_profit, 0), 1)
        expenses_percent = (expenses / base) * 100
        netprofit_percent = (max(net_profit, 0) / base) * 100
        return expenses_percent, netprofit_percent

    prev_exp_p, prev_np_p = to_percent(previous.expense_total or 0, (previous.income_total or 0) - (previous.expense_total or 0))
    cur_exp_p, cur_np_p = to_percent(current.expense_total or 0, (current.income_total or 0) - (current.expense_total or 0))

    return {
        "previous_label": previous.name,
        "current_label": current.name,
        "previous_expenses_percent": prev_exp_p,
        "current_expenses_percent": cur_exp_p,
        "previous_netprofit_percent": prev_np_p,
        "current_netprofit_percent": cur_np_p
    }

@router.get("/financial/records", response_model=List[FinancialRecordSchema])
def get_financial_records(
    start_date: datetime = None,
    end_date: datetime = None,
    category: str = None,
    field_id: Optional[int] = None,
    project_id: Optional[int] = None,
    include_history: bool = False,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    query = db.query(FinancialRecord).filter(FinancialRecord.owner_id == current_user.id)
    if not include_history:
        query = query.filter(FinancialRecord.is_history == False)
    
    if start_date:
        query = query.filter(FinancialRecord.date >= start_date)
    if end_date:
        query = query.filter(FinancialRecord.date <= end_date)
    if category:
        query = query.filter(FinancialRecord.category == category)
    if field_id:
        query = query.filter(FinancialRecord.field_id == field_id)
    if project_id:
        query = query.filter(FinancialRecord.project_id == project_id)
    
    records = query.order_by(FinancialRecord.date.desc()).all()
    return records

@router.get("/financial/summary")
def get_financial_summary(
    start_date: datetime = None,
    end_date: datetime = None,
    field_id: Optional[int] = None,
    project_id: Optional[int] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    if not start_date:
        start_date = datetime.now() - timedelta(days=30)
    if not end_date:
        end_date = datetime.now()
    
    records_query = db.query(FinancialRecord).filter(
        FinancialRecord.owner_id == current_user.id,
        FinancialRecord.is_history == False,
        FinancialRecord.date >= start_date,
        FinancialRecord.date <= end_date
    )
    if field_id:
        records_query = records_query.filter(FinancialRecord.field_id == field_id)
    if project_id:
        records_query = records_query.filter(FinancialRecord.project_id == project_id)
    records = records_query.all()

    # DEBUG PRINT: Check your terminal! 
    # This will tell us if the query found anything at all.
    print(f"DEBUG: Found {len(records)} records for user {current_user.id} in this date range.")

    total_income = 0
    total_expenses = 0
    categories = {}

    for r in records:
        # Normalize to UPPERCASE to avoid "income" vs "INCOME" errors
        t_type = r.transaction_type.upper() if r.transaction_type else ""
        
        if t_type == "INCOME":
            total_income += r.amount
        elif t_type == "EXPENSE":
            total_expenses += r.amount

        # Category logic
        if r.category not in categories:
            categories[r.category] = {"INCOME": 0, "EXPENSE": 0}
        categories[r.category][t_type if t_type in ["INCOME", "EXPENSE"] else "EXPENSE"] += r.amount
    
    net_profit = total_income - total_expenses
    
    return {
        "period": {"start_date": start_date, "end_date": end_date},
        "total_income": total_income,
        "total_expenses": total_expenses,
        "net_profit": net_profit,
        "profit_margin": (net_profit / total_income * 100) if total_income > 0 else 0,
        "category_breakdown": categories
    }

@router.post("/financial/partial-budgeting", response_model=PartialBudgetingResponse)
def calculate_partial_budgeting(
    input_data: PartialBudgetingInput,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    return partial_budgeting.calculate_net_benefit(input_data)

@router.get("/financial/net-financial-return/{field_id}")
def calculate_net_financial_return(
    field_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    from ..decision_tree.engine import DecisionTreeEngine
    
    decision_tree = DecisionTreeEngine()
    
    # Get field
    field = db.query(FieldModel).filter(
        FieldModel.id == field_id,
        FieldModel.owner_id == current_user.id
    ).first()
    
    if not field:
        raise HTTPException(status_code=404, detail="Field not found")
    
    # Get financial records for this field
    financial_records = db.query(FinancialRecord).filter(
        FinancialRecord.field_id == field_id,
        FinancialRecord.owner_id == current_user.id,
        FinancialRecord.is_history == False
    ).all()
    
    total_costs = sum(r.amount for r in financial_records if r.transaction_type == "expense")
    
    # Estimate yield value (simplified)
    base_yields = {
        "coconut": 50000,
        "corn": 30000,
        "rice": 40000  
    }
    
    base_yield = base_yields.get(field.crop_type.value, 20000)
    predicted_yield_value = base_yield * field.area_hectares
    
    net_financial_return = decision_tree.calculate_net_financial_return(
        predicted_yield_value, total_costs
    )
    
    return {
        "field_id": field_id,
        "crop_type": field.crop_type.value,
        "area_hectares": field.area_hectares,
        "predicted_yield_value": predicted_yield_value,
        "total_costs": total_costs,
        "net_financial_return": net_financial_return,
        "roi": (net_financial_return / total_costs * 100) if total_costs > 0 else 0
    }

# Use .put for full updates or .patch for partial updates
@router.put("/financial/records/{record_id}", response_model=schemas.FinancialRecord)
def update_record(
    record_id: int, 
    updated_data: schemas.FinancialRecordCreate, # The new data from Postman
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user)
):
    # 1. Find the record
    query = db.query(models.FinancialRecord).filter(models.FinancialRecord.id == record_id)
    db_record = query.first()

    # 2. Check if it exists
    if not db_record:
        raise HTTPException(status_code=404, detail="Record not found")

    # 3. Security Check: Does this record belong to the logged-in user?
    if db_record.owner_id != current_user.id:
        raise HTTPException(status_code=403, detail="Not authorized to update this record")

    # 4. Update the fields
    query.update(updated_data.model_dump(), synchronize_session=False)
    db.commit()
    
    return query.first()

@router.delete("/financial/records/{record_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_record(
    record_id: int, 
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user)
):
    query = db.query(models.FinancialRecord).filter(models.FinancialRecord.id == record_id)
    db_record = query.first()

    if not db_record:
        raise HTTPException(status_code=404, detail="Record not found")
        
    if db_record.owner_id != current_user.id:
        raise HTTPException(status_code=403, detail="Not authorized")

    query.delete(synchronize_session=False)
    db.commit()
    return None # 204 No Content doesn't return a body

# --- Crop Projects ---
@router.post("/financial/projects", response_model=CropProjectSchema)
def create_project(
    project: CropProjectCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    data = project.model_dump()
    if data.get("client_id"):
        existing_by_client_id = db.query(CropProject).filter(
            CropProject.owner_id == current_user.id,
            CropProject.client_id == data["client_id"],
            CropProject.is_deleted == False
        ).first()
        if existing_by_client_id:
            return existing_by_client_id

    duplicate_window_start = datetime.utcnow() - timedelta(seconds=15)
    existing_recent = db.query(CropProject).filter(
        CropProject.owner_id == current_user.id,
        CropProject.name == data["name"],
        CropProject.crop_type == data["crop_type"],
        CropProject.crop_variety == data.get("crop_variety"),
        CropProject.farm_id == data.get("farm_id"),
        CropProject.field_id == data.get("field_id"),
        CropProject.is_deleted == False,
        CropProject.created_at >= duplicate_window_start
    ).first()
    if existing_recent:
        return existing_recent

    budget_total = data.get("budget_total") or 0
    db_project = CropProject(
        **data,
        owner_id=current_user.id,
        budget_remaining=budget_total
    )
    db.add(db_project)
    db.commit()
    db.refresh(db_project)
    return db_project

@router.get("/financial/projects", response_model=List[CropProjectSchema])
def list_projects(
    field_id: Optional[int] = None,
    farm_id: Optional[int] = None,
    crop_type: Optional[str] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    query = db.query(CropProject).filter(CropProject.owner_id == current_user.id)
    if field_id:
        query = query.filter(CropProject.field_id == field_id)
    if farm_id:
        query = query.filter(CropProject.farm_id == farm_id)
    if crop_type:
        query = query.filter(CropProject.crop_type == crop_type)
    return query.all()

@router.get("/financial/projects/{project_id}", response_model=CropProjectSchema)
def get_project(
    project_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    project = db.query(CropProject).filter(
        CropProject.id == project_id,
        CropProject.owner_id == current_user.id
    ).first()
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    return project

@router.patch("/financial/projects/{project_id}", response_model=CropProjectSchema)
def update_project(
    project_id: int,
    payload: CropProjectUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    query = db.query(CropProject).filter(
        CropProject.id == project_id,
        CropProject.owner_id == current_user.id
    )
    project = query.first()
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    query.update(payload.model_dump(exclude_unset=True), synchronize_session=False)
    db.commit()
    return query.first()

@router.delete("/financial/projects/{project_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_project(
    project_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    query = db.query(CropProject).filter(
        CropProject.id == project_id,
        CropProject.owner_id == current_user.id
    )
    if not query.first():
        raise HTTPException(status_code=404, detail="Project not found")
    query.delete(synchronize_session=False)
    db.commit()
    return None
