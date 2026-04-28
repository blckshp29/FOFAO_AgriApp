from fastapi import APIRouter, Depends, HTTPException, status
from fastapi import Query
from sqlalchemy import or_
from sqlalchemy.orm import Session
from typing import Any, Dict, List, Literal, Optional
from datetime import datetime, timedelta

from .. import models, schemas
from ..database import get_db
from ..models import CompletedOperationHistory, CoconutAllocation, FinancialRecord, User, Field as FieldModel, CropProject, ProjectStatus
from ..schemas import (
    FinancialRecordCreate,
    FinancialRecord as FinancialRecordSchema,
    CropProjectCreate,
    CropProjectUpdate,
    CropProject as CropProjectSchema,
    ProjectCompletionResponse,
    CompletedProjectListItem,
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

DEFAULT_ALLOCATION_PERCENTAGES = {
    "Land Preparation": 18.0,
    "Seeds": 8.0,
    "Fertilizers": 28.0,
    "Chemicals": 18.0,
    "Labor": 22.0,
    "Miscellaneous": 6.0,
}

VISIBLE_ALLOCATION_CATEGORIES = [
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

def _scale_allocations_to_budget(allocations: list, budget_total: float) -> list:
    """
    Scale recommended_amounts proportionally so they sum exactly to budget_total.
    Percentages are recomputed after scaling.
    """
    if budget_total <= 0 or not allocations:
        return allocations

    base_total = sum(a.get("recommended_amount", 0.0) or 0.0 for a in allocations)
    if base_total <= 0:
        # Fallback to equal split if no base data
        equal = round(budget_total / len(allocations), 2)
        scaled = [{**a, "recommended_amount": equal} for a in allocations]
    else:
        factor = budget_total / base_total
        scaled = []
        running = 0.0
        for idx, item in enumerate(allocations):
            base_amt = item.get("recommended_amount", 0.0) or 0.0
            amt = round(base_amt * factor, 2)
            scaled.append({**item, "recommended_amount": amt})
            running += amt
        # Fix rounding drift on last item
        drift = round(budget_total - running, 2)
        scaled[-1]["recommended_amount"] = round(scaled[-1]["recommended_amount"] + drift, 2)

    # Recompute percents based on scaled amounts
    total_scaled = sum(a["recommended_amount"] for a in scaled) or 1.0
    for a in scaled:
        a["percent_of_total"] = round((a["recommended_amount"] / total_scaled) * 100.0, 2)
    return scaled


def _build_rice_budget_template(crop_variety: Optional[str], hectares: float, budget_total: float = 0.0) -> schemas.RiceBudgetTemplateResponse:
    hectares = hectares if hectares and hectares > 0 else 1.0  # keep as metadata only
    normalized_variety = _normalize_rice_variety(crop_variety)

    allocations = []
    budget_min = 0.0
    budget_max = 0.0
    budget_recommended = 0.0

    for category, min_amount, max_amount in RICE_BASE_COST_COMPONENTS:
        min_scaled = round(min_amount, 2)
        max_scaled = round(max_amount, 2)
        recommended_scaled = round(((min_amount + max_amount) / 2.0), 2)
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

    allocations = _scale_allocations_to_budget(allocations, budget_total)

    return schemas.RiceBudgetTemplateResponse(
        crop_type="rice",
        crop_variety=normalized_variety,
        hectares=round(hectares, 2),
        currency="PHP",
        budget_min=round(budget_min, 2),
        budget_max=round(budget_max, 2),
        budget_recommended=round(budget_total or budget_recommended, 2),
        allocations=allocations,
    )

def _build_corn_budget_template(corn_type: Optional[str], hectares: float, budget_total: float = 0.0) -> schemas.CornBudgetTemplateResponse:
    hectares = hectares if hectares and hectares > 0 else 1.0  # metadata only
    profile_key = _normalize_corn_type(corn_type)
    profile = CORN_COST_TEMPLATES[profile_key]

    raw_allocations = [("Seeds", *profile["seeds"]), *CORN_BASE_COST_COMPONENTS]
    scaled_allocations = []
    budget_min = 0.0
    budget_max = 0.0
    budget_recommended = 0.0

    for category, min_amount, max_amount in raw_allocations:
        min_scaled = round(min_amount, 2)
        max_scaled = round(max_amount, 2)
        recommended_scaled = round(((min_amount + max_amount) / 2.0), 2)
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

    allocations = _scale_allocations_to_budget(allocations, budget_total)

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
        budget_recommended=round(budget_total or budget_recommended, 2),
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

def _build_coconut_budget_template(
    gross_revenue: float,
    arrastre_cost: float,
    food_cost: float,
    number_of_labors: int,
    contract_type: Literal["50_50", "60_40", "tercia"],
) -> schemas.CoconutBudgetTemplateResponse:
    if number_of_labors <= 0:
        raise HTTPException(status_code=400, detail="number_of_labors must be greater than 0")

    if contract_type == "50_50":
        deductions = arrastre_cost
    else:
        deductions = arrastre_cost + food_cost

    net_revenue = max(gross_revenue - deductions, 0.0)
    owner_share = 0.0
    labor_total = 0.0
    tenant_share = None

    if contract_type == "50_50":
        owner_share = net_revenue * 0.5
        labor_total = net_revenue * 0.5
    elif contract_type == "60_40":
        owner_share = net_revenue * 0.6
        labor_total = net_revenue * 0.4
    else:
        owner_share = net_revenue / 3
        labor_total = net_revenue / 3
        tenant_share = net_revenue / 3

    labor_individual = labor_total / number_of_labors

    return schemas.CoconutBudgetTemplateResponse(
        net_revenue=round(net_revenue, 2),
        owner_share=round(owner_share, 2),
        tenant_share=round(tenant_share, 2) if tenant_share is not None else None,
        labor_total=round(labor_total, 2),
        labor_individual=round(labor_individual, 2),
        number_of_labors=number_of_labors,
    )


def _get_owned_project_or_404(db: Session, project_id: int, user_id: int) -> CropProject:
    project = db.query(CropProject).filter(
        CropProject.id == project_id,
        CropProject.owner_id == user_id,
    ).first()
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    return project


def _get_project_with_access_check(db: Session, project_id: int, user_id: int) -> CropProject:
    project = db.query(CropProject).filter(CropProject.id == project_id).first()
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    if project.owner_id != user_id:
        raise HTTPException(status_code=403, detail="Not authorized to access this project")
    return project


def _apply_project_status_filter(query, status_filter: Optional[str]):
    if not status_filter:
        return query

    normalized = status_filter.strip().lower()
    if normalized == "ongoing":
        return query.filter(CropProject.status.in_([ProjectStatus.PLANNED, ProjectStatus.ACTIVE]))
    if normalized == "completed":
        return query.filter(CropProject.status == ProjectStatus.COMPLETED)
    if normalized in {member.value for member in ProjectStatus}:
        return query.filter(CropProject.status == normalized)

    raise HTTPException(
        status_code=400,
        detail="Invalid status filter. Use ongoing, completed, planned, active, or archived.",
    )


def _resolve_coconut_project_gross_revenue(project: CropProject, payload: schemas.CoconutAllocationSaveRequest) -> float:
    if payload.gross_revenue is not None:
        return payload.gross_revenue

    if project.field and project.field.gross_revenue is not None:
        return project.field.gross_revenue

    raise HTTPException(
        status_code=400,
        detail="gross_revenue is required unless the linked field already has gross_revenue saved",
    )

def _build_vegetable_budget_template(vegetable_type: Optional[str], hectares: float, budget_total: float = 0.0) -> schemas.VegetableBudgetTemplateResponse:
    hectares = hectares if hectares and hectares > 0 else 1.0  # metadata only
    vegetable_key = _normalize_vegetable_type(vegetable_type)
    template = VEGETABLE_BUDGET_TEMPLATES[vegetable_key]

    recommended_base_total = sum(((min_amount + max_amount) / 2.0) for _, min_amount, max_amount in template["costs"])
    if recommended_base_total <= 0:
        recommended_base_total = 1.0

    allocations = []
    for category, min_amount, max_amount in template["costs"]:
        recommended_amount = (min_amount + max_amount) / 2.0
        allocations.append(
            {
                "category": category,
                "min_amount": round(min_amount, 2),
                "max_amount": round(max_amount, 2),
                "recommended_amount": round(recommended_amount, 2),
                "percent_of_total": round((recommended_amount / recommended_base_total) * 100.0, 2),
            }
        )

    allocations = _scale_allocations_to_budget(allocations, budget_total)

    return schemas.VegetableBudgetTemplateResponse(
        crop_type="vegetables",
        vegetable_type=template["label"],
        hectares=round(hectares, 2),
        currency="PHP",
        budget_min=round(template["budget_min"], 2),
        budget_max=round(template["budget_max"], 2),
        budget_recommended=round(budget_total or ((template["budget_min"] + template["budget_max"]) / 2.0), 2),
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
    completed_operations = db.query(CompletedOperationHistory).filter(
        CompletedOperationHistory.owner_id == user_id
    ).all()

    totals = {}
    total_spend = 0.0
    used_completed_histories = 0
    for operation in completed_operations:
        category_costs = {}
        if operation.category_costs:
            if isinstance(operation.category_costs, str):
                try:
                    parsed = json.loads(operation.category_costs)
                    if isinstance(parsed, dict):
                        category_costs = parsed
                except Exception:
                    category_costs = {}
            elif isinstance(operation.category_costs, dict):
                category_costs = operation.category_costs

        if category_costs:
            used_completed_histories += 1
            for category, amount in category_costs.items():
                normalized = _normalize_category(category)
                numeric_amount = float(amount or 0.0)
                totals[normalized] = totals.get(normalized, 0.0) + numeric_amount
                total_spend += numeric_amount

    if used_completed_histories > 0 and total_spend > 0:
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
            "used_history_records": True,
            "history_source": "completed_operation_histories",
            "budget_total": round(budget_total, 2),
            "allocations": allocations
        }

    base_query = db.query(FinancialRecord).outerjoin(
        CropProject, FinancialRecord.project_id == CropProject.id
    ).filter(
        FinancialRecord.transaction_type == "expense",
        FinancialRecord.description != SEEDED_HISTORY_DESCRIPTION,
        or_(
            (
                or_(FinancialRecord.owner_id == user_id, FinancialRecord.owner_id == 0)
            ) & (FinancialRecord.is_history == True),
            (
                (CropProject.owner_id == user_id)
                & (CropProject.status == ProjectStatus.COMPLETED)
            ),
        ),
    )

    history_count = base_query.count()

    records = base_query.all()
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
        "history_source": "financial_records",
        "budget_total": round(budget_total, 2),
        "allocations": allocations
    }


def _recalculate_project_totals(db: Session, project: CropProject) -> CropProject:
    expenses = db.query(FinancialRecord).filter(
        FinancialRecord.owner_id == project.owner_id,
        FinancialRecord.project_id == project.id,
        FinancialRecord.is_history == False,
        FinancialRecord.transaction_type == "expense",
    ).all()
    incomes = db.query(FinancialRecord).filter(
        FinancialRecord.owner_id == project.owner_id,
        FinancialRecord.project_id == project.id,
        FinancialRecord.is_history == False,
        FinancialRecord.transaction_type == "income",
    ).all()

    expense_total = round(sum(record.amount or 0.0 for record in expenses), 2)
    income_total = round(sum(record.amount or 0.0 for record in incomes), 2)
    budget_total = round(project.budget_total or 0.0, 2)

    project.expense_total = expense_total
    project.income_total = income_total
    project.budget_total = budget_total
    project.budget_remaining = round(budget_total - expense_total, 2)
    return project


def _build_budget_allocation_payload(summary: Dict[str, Any], budget_total: float) -> Dict[str, Any]:
    effective_budget = round(budget_total or summary.get("total_historical_spend", 0.0) or 0.0, 2)
    allocations = summary.get("allocations", []) or []

    has_meaningful_history = any((item.get("percent_of_total", 0) or 0) > 0 for item in allocations)

    if (not allocations or not has_meaningful_history) and effective_budget > 0:
        allocations = [
            {
                "category": category,
                "historical_cost": 0.0,
                "percent_of_total": DEFAULT_ALLOCATION_PERCENTAGES[category],
                "allocated_amount": round(effective_budget * (DEFAULT_ALLOCATION_PERCENTAGES[category] / 100.0), 2),
            }
            for category in DEFAULT_HISTORICAL_CATEGORIES
        ]

        # Fix rounding drift on the final category so totals match exactly.
        running_total = round(sum(item["allocated_amount"] for item in allocations), 2)
        drift = round(effective_budget - running_total, 2)
        allocations[-1]["allocated_amount"] = round(allocations[-1]["allocated_amount"] + drift, 2)

    budget_min = round(effective_budget * 0.9, 2) if effective_budget > 0 else 0.0
    budget_max = round(effective_budget * 1.1, 2) if effective_budget > 0 else 0.0

    return {
        "budget_total": effective_budget,
        "budget_recommended": effective_budget,
        "budget_min": budget_min,
        "budget_max": budget_max,
        "used_history_records": summary.get("used_history_records", False) and has_meaningful_history,
        "history_source": summary.get("history_source") if has_meaningful_history else "default_recommended_split",
        "total_historical_spend": round(summary.get("total_historical_spend", 0.0), 2),
        "allocations": _redistribute_land_preparation_allocation(allocations, effective_budget),
    }


def _redistribute_land_preparation_allocation(allocations: List[Dict[str, Any]], budget_total: float) -> List[Dict[str, Any]]:
    if not allocations:
        return allocations

    visible_allocations = []
    hidden_land_preparation = None

    for item in allocations:
        if item.get("category") == "Land Preparation":
            hidden_land_preparation = item
            continue
        if item.get("category") in VISIBLE_ALLOCATION_CATEGORIES:
            visible_allocations.append(
                {
                    **item,
                    "historical_cost": float(item.get("historical_cost", 0.0) or 0.0),
                    "percent_of_total": float(item.get("percent_of_total", 0.0) or 0.0),
                    "allocated_amount": float(item.get("allocated_amount", 0.0) or 0.0),
                }
            )

    if not visible_allocations:
        visible_allocations = [
            {
                "category": category,
                "historical_cost": 0.0,
                "percent_of_total": 0.0,
                "allocated_amount": 0.0,
            }
            for category in VISIBLE_ALLOCATION_CATEGORIES
        ]

    land_percent = float((hidden_land_preparation or {}).get("percent_of_total", 0.0) or 0.0)
    land_amount = float((hidden_land_preparation or {}).get("allocated_amount", 0.0) or 0.0)
    land_cost = float((hidden_land_preparation or {}).get("historical_cost", 0.0) or 0.0)

    visible_percent_total = sum(item["percent_of_total"] for item in visible_allocations)
    if visible_percent_total <= 0:
        fallback_total = sum(DEFAULT_ALLOCATION_PERCENTAGES[category] for category in VISIBLE_ALLOCATION_CATEGORIES)
        for item in visible_allocations:
            base_percent = DEFAULT_ALLOCATION_PERCENTAGES[item["category"]]
            share = base_percent / fallback_total if fallback_total > 0 else 1 / len(visible_allocations)
            item["percent_of_total"] = round(share * 100.0, 2)
            item["allocated_amount"] = round(budget_total * share, 2)
        visible_percent_total = sum(item["percent_of_total"] for item in visible_allocations)

    if land_percent > 0 or land_amount > 0 or land_cost > 0:
        for item in visible_allocations:
            share = (item["percent_of_total"] / visible_percent_total) if visible_percent_total > 0 else (1 / len(visible_allocations))
            item["percent_of_total"] = round(item["percent_of_total"] + (land_percent * share), 2)
            item["allocated_amount"] = round(item["allocated_amount"] + (land_amount * share), 2)
            item["historical_cost"] = round(item["historical_cost"] + (land_cost * share), 2)

    running_amount = round(sum(item["allocated_amount"] for item in visible_allocations), 2)
    drift = round(budget_total - running_amount, 2)
    if visible_allocations and budget_total > 0:
        visible_allocations[-1]["allocated_amount"] = round(visible_allocations[-1]["allocated_amount"] + drift, 2)

    running_percent = round(sum(item["percent_of_total"] for item in visible_allocations), 2)
    percent_drift = round(100.0 - running_percent, 2)
    if visible_allocations:
        visible_allocations[-1]["percent_of_total"] = round(visible_allocations[-1]["percent_of_total"] + percent_drift, 2)

    return visible_allocations


def _ensure_project_budget_total(project: CropProject) -> None:
    if (project.budget_total or 0) <= 0:
        raise HTTPException(
            status_code=400,
            detail="Project budget_total must be greater than 0 before budget allocation can be computed."
        )

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
    budget_total: Optional[float] = Query(default=None, alias="budget"),
    project_id: Optional[int] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    if budget_total is None and project_id is not None:
        project = db.query(CropProject).filter(
            CropProject.id == project_id,
            CropProject.owner_id == current_user.id
        ).first()
        if not project:
            raise HTTPException(status_code=404, detail="Project not found")
        _ensure_project_budget_total(project)
    effective_budget = _resolve_budget(project_id, budget_total, db, current_user.id)
    if effective_budget <= 0:
        raise HTTPException(
            status_code=400,
            detail="budget must be greater than 0, or provide a project with a valid budget_total."
        )
    summary = _calculate_historical_allocations(db, current_user.id, effective_budget)
    return _build_budget_allocation_payload(summary, effective_budget)

def _resolve_budget(project_id: Optional[int], budget_total: Optional[float], db: Session, user_id: int) -> float:
    if budget_total is not None and budget_total > 0:
        return budget_total
    if project_id:
        project = db.query(CropProject).filter(
            CropProject.id == project_id,
            CropProject.owner_id == user_id
        ).first()
        if project and project.budget_total and project.budget_total > 0:
            return project.budget_total
    # Fallback: no user budget supplied; allow builders to use their base recommended totals
    return 0.0


@router.get("/financial/budget/corn-template")
def get_corn_budget_template(
    corn_type: str = "yellow",
    hectares: float = 1.0,
    budget_total: Optional[float] = Query(default=None, alias="budget"),
    project_id: Optional[int] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    effective_budget = _resolve_budget(project_id, budget_total, db, current_user.id)
    allocations = _calculate_historical_allocations(db, current_user.id, effective_budget)
    if allocations["total_historical_spend"] <= 0:
        raise HTTPException(
            status_code=400,
            detail="No historical data available. Please input expense records first."
        )
    return allocations

@router.get("/financial/budget/rice-template")
def get_rice_budget_template(
    crop_variety: Optional[str] = None,
    variety: Optional[str] = Query(default=None),
    hectares: float = 1.0,
    budget_total: Optional[float] = Query(default=None, alias="budget"),
    project_id: Optional[int] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    raise HTTPException(
        status_code=400,
        detail="Rice budget allocation not yet implemented; please provide rice historical expense data first."
    )

@router.get("/financial/budget/coconut-template", response_model=schemas.CoconutBudgetTemplateResponse)
@router.get("/financial/coconut-allocation", response_model=schemas.CoconutBudgetTemplateResponse)
def get_coconut_budget_template(
    gross_revenue: Optional[float] = Query(default=None, ge=0),
    arrastre_cost: float = Query(default=0.0, ge=0),
    food_cost: float = Query(default=0.0, ge=0),
    number_of_labors: int = Query(..., gt=0),
    contract_type: Literal["50_50", "60_40", "tercia"] = Query(...),
    project_id: Optional[int] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    if gross_revenue is None and project_id is not None:
        project = _get_owned_project_or_404(db, project_id, current_user.id)
        gross_revenue = _resolve_coconut_project_gross_revenue(
            project,
            schemas.CoconutAllocationSaveRequest(
                gross_revenue=None,
                arrastre_cost=arrastre_cost,
                food_cost=food_cost,
                number_of_labors=number_of_labors,
                contract_type=contract_type,
            ),
        )
    if gross_revenue is None:
        raise HTTPException(status_code=400, detail="gross_revenue is required")

    return _build_coconut_budget_template(
        gross_revenue=gross_revenue,
        arrastre_cost=arrastre_cost,
        food_cost=food_cost,
        number_of_labors=number_of_labors,
        contract_type=contract_type,
    )

@router.get("/financial/budget/vegetable-template")
def get_vegetable_budget_template(
    vegetable_type: str = "eggplant",
    hectares: float = 1.0,
    budget_total: Optional[float] = Query(default=None, alias="budget"),
    project_id: Optional[int] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    raise HTTPException(
        status_code=400,
        detail="Vegetable budget allocation not yet implemented; please provide vegetable historical expense data first."
    )


@router.post(
    "/financial/projects/{project_id}/coconut-allocation",
    response_model=schemas.CoconutAllocationResponse,
)
def save_project_coconut_allocation(
    project_id: int,
    payload: schemas.CoconutAllocationSaveRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    project = _get_owned_project_or_404(db, project_id, current_user.id)
    if project.crop_type != models.CropType.COCONUT:
        raise HTTPException(status_code=400, detail="Coconut allocation is only available for coconut projects")

    gross_revenue = _resolve_coconut_project_gross_revenue(project, payload)
    allocation = _build_coconut_budget_template(
        gross_revenue=gross_revenue,
        arrastre_cost=payload.arrastre_cost,
        food_cost=payload.food_cost,
        number_of_labors=payload.number_of_labors,
        contract_type=payload.contract_type,
    )

    existing = db.query(CoconutAllocation).filter(CoconutAllocation.project_id == project_id).first()
    values = {
        "gross_revenue": gross_revenue,
        "arrastre_cost": payload.arrastre_cost,
        "food_cost": payload.food_cost,
        "number_of_labors": payload.number_of_labors,
        "contract_type": payload.contract_type,
        "net_revenue": allocation.net_revenue,
        "owner_share": allocation.owner_share,
        "tenant_share": allocation.tenant_share,
        "labor_total": allocation.labor_total,
        "labor_individual": allocation.labor_individual,
    }

    if existing:
        for key, value in values.items():
            setattr(existing, key, value)
        record = existing
    else:
        record = CoconutAllocation(project_id=project_id, **values)
        db.add(record)

    db.commit()
    db.refresh(record)
    return record


@router.get(
    "/financial/projects/{project_id}/coconut-allocation",
    response_model=schemas.CoconutAllocationResponse,
)
def get_project_coconut_allocation(
    project_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    project = _get_owned_project_or_404(db, project_id, current_user.id)
    if project.crop_type != models.CropType.COCONUT:
        raise HTTPException(status_code=400, detail="Coconut allocation is only available for coconut projects")

    allocation = db.query(CoconutAllocation).filter(CoconutAllocation.project_id == project_id).first()
    if not allocation:
        raise HTTPException(status_code=404, detail="Coconut allocation not found for this project")
    return allocation

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
        project = _recalculate_project_totals(db, project)
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
        CropProject.owner_id == current_user.id,
        CropProject.status == ProjectStatus.COMPLETED
    ).order_by(CropProject.completed_at.desc().nullslast(), CropProject.start_date.desc().nullslast(), CropProject.created_at.desc()).limit(2).all()

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
    records_query = db.query(FinancialRecord).filter(
        FinancialRecord.owner_id == current_user.id,
        FinancialRecord.is_history == False
    )
    if start_date:
        records_query = records_query.filter(FinancialRecord.date >= start_date)
    if end_date:
        records_query = records_query.filter(FinancialRecord.date <= end_date)
    if field_id:
        records_query = records_query.filter(FinancialRecord.field_id == field_id)
    if project_id:
        records_query = records_query.filter(FinancialRecord.project_id == project_id)
    records = records_query.all()

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
    crop_type = data.get("crop_type")
    if crop_type != models.CropType.COCONUT and budget_total <= 0:
        raise HTTPException(status_code=400, detail="budget_total must be greater than 0 for non-coconut projects")
    if crop_type == models.CropType.COCONUT and budget_total < 0:
        raise HTTPException(status_code=400, detail="budget_total must be greater than or equal to 0")
    db_project = CropProject(
        **data,
        owner_id=current_user.id,
        budget_remaining=budget_total
    )
    db.add(db_project)
    db.commit()
    db.refresh(db_project)
    return _recalculate_project_totals(db, db_project)

@router.get("/financial/projects", response_model=List[CropProjectSchema])
def list_projects(
    field_id: Optional[int] = None,
    farm_id: Optional[int] = None,
    crop_type: Optional[str] = None,
    status: Optional[str] = Query(default=None),
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
    query = _apply_project_status_filter(query, status)
    projects = query.order_by(CropProject.completed_at.desc().nullslast(), CropProject.created_at.desc()).all()
    return [_recalculate_project_totals(db, project) for project in projects]


@router.get("/financial/projects/completed", response_model=List[CompletedProjectListItem])
def list_completed_projects(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    projects = db.query(CropProject).filter(
        CropProject.owner_id == current_user.id,
        CropProject.status == ProjectStatus.COMPLETED
    ).order_by(CropProject.completed_at.desc().nullslast(), CropProject.created_at.desc()).all()

    return [
        CompletedProjectListItem(
            id=project.id,
            name=project.name,
            field_id=project.field_id,
            field_name=project.field.name if project.field else None,
            crop_type=project.crop_type,
            crop_variety=project.crop_variety,
            area_hectares=project.field.area_hectares if project.field else None,
            started_at=project.start_date,
            created_at=project.created_at,
            completed_at=project.completed_at,
            status=project.status,
            gross_revenue=project.field.gross_revenue if project.field else None,
            total_budget=project.budget_total or 0,
            total_expenses=project.expense_total or 0,
            total_income=project.income_total or 0,
        )
        for project in [_recalculate_project_totals(db, project) for project in projects]
    ]

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
    return _recalculate_project_totals(db, project)

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
    updates = payload.model_dump(exclude_unset=True)
    effective_crop_type = updates.get("crop_type", project.crop_type)
    new_budget_total = updates.get("budget_total")
    if effective_crop_type != models.CropType.COCONUT and new_budget_total is not None and new_budget_total <= 0:
        raise HTTPException(status_code=400, detail="budget_total must be greater than 0 for non-coconut projects")
    if effective_crop_type == models.CropType.COCONUT and new_budget_total is not None and new_budget_total < 0:
        raise HTTPException(status_code=400, detail="budget_total must be greater than or equal to 0")
    new_budget_remaining = updates.get("budget_remaining")
    if new_budget_remaining is not None and new_budget_remaining < 0:
        raise HTTPException(status_code=400, detail="budget_remaining must be greater than or equal to 0")
    new_status = updates.get("status")
    if new_status == ProjectStatus.COMPLETED:
        updates["completed_at"] = project.completed_at or datetime.utcnow()
        if project.end_date is None and "end_date" not in updates:
            updates["end_date"] = updates["completed_at"]
    elif new_status and new_status != ProjectStatus.COMPLETED:
        updates["completed_at"] = None

    if new_budget_total is not None and "budget_remaining" not in updates:
        current_expenses = project.expense_total or 0
        updates["budget_remaining"] = round(new_budget_total - current_expenses, 2)

    query.update(updates, synchronize_session=False)
    db.commit()
    project = query.first()
    return _recalculate_project_totals(db, project)


@router.patch("/financial/projects/{project_id}/complete", response_model=ProjectCompletionResponse)
def complete_project(
    project_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    project = _get_project_with_access_check(db, project_id, current_user.id)

    if project.status == ProjectStatus.COMPLETED:
        raise HTTPException(status_code=400, detail="Project is already completed")

    project.status = ProjectStatus.COMPLETED
    project.completed_at = datetime.utcnow()
    if project.end_date is None:
        project.end_date = project.completed_at

    db.add(project)
    db.commit()
    db.refresh(project)

    return ProjectCompletionResponse(
        id=project.id,
        field_id=project.field_id,
        crop_type=project.crop_type,
        crop_variety=project.crop_variety,
        status=project.status,
        completed_at=project.completed_at,
        message="Project marked as completed and preserved for historical analysis.",
    )

@router.delete("/financial/projects/{project_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_project(
    project_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    db.query(CoconutAllocation).filter(CoconutAllocation.project_id == project_id).delete(synchronize_session=False)
    query = db.query(CropProject).filter(
        CropProject.id == project_id,
        CropProject.owner_id == current_user.id
    )
    if not query.first():
        raise HTTPException(status_code=404, detail="Project not found")
    query.delete(synchronize_session=False)
    db.commit()
    return None
