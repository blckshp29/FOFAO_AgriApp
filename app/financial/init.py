from sqlalchemy.orm import Session

from ..models import CropProject, CropType, FinancialRecord


DEFAULT_PROJECT_BUDGETS = {
    CropType.RICE: 67000.0,
    CropType.CORN: 35000.0,
    CropType.COCONUT: 25000.0,
    CropType.VEGETABLES: 35000.0,
}


def _recommended_budget_for_project(project: CropProject) -> float:
    crop_default = DEFAULT_PROJECT_BUDGETS.get(project.crop_type, 30000.0)

    expenses = project.expense_total or 0.0
    incomes = project.income_total or 0.0

    # Ensure repaired budgets are never lower than already-recorded expenses.
    base_budget = max(crop_default, expenses)

    # If the project already has income/expense activity, give a small safety margin.
    if expenses > 0 or incomes > 0:
        base_budget = max(base_budget, round(expenses * 1.2, 2))

    return round(base_budget, 2)


def repair_zero_budget_projects(db: Session) -> int:
    projects = db.query(CropProject).filter(
        (CropProject.budget_total.is_(None)) | (CropProject.budget_total <= 0)
    ).all()

    repaired = 0
    for project in projects:
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

        project.expense_total = expense_total
        project.income_total = income_total
        project.budget_total = _recommended_budget_for_project(project)
        project.budget_remaining = round(project.budget_total - expense_total, 2)
        repaired += 1

    if repaired:
        db.commit()

    return repaired
