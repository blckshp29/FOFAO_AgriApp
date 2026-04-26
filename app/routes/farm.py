from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List
from datetime import datetime, timedelta

from .. import models, schemas
from ..database import get_db
from ..models import CompletedOperationHistory, Farm, Field, User
from ..operations.history import mark_field_completed, reopen_field_operation
from ..schemas import (
    CompletedOperationHistory as CompletedOperationHistorySchema,
    FarmCreate,
    Farm as FarmSchema,
    FieldCreate,
    Field as FieldSchema,
    FieldUpdate,
)
from .auth import get_current_user

router = APIRouter()


def _validate_field_payload(payload: dict, crop_type) -> None:
    if crop_type == models.CropType.COCONUT:
        gross_revenue = payload.get("gross_revenue")
        if gross_revenue is not None and gross_revenue < 0:
            raise HTTPException(status_code=400, detail="gross_revenue must be greater than or equal to 0")
        return

    area_hectares = payload.get("area_hectares")
    if area_hectares is None or area_hectares <= 0:
        raise HTTPException(status_code=400, detail="area_hectares must be greater than 0 for non-coconut crops")

@router.post("/farms", response_model=FarmSchema)
def create_farm(
    farm: FarmCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    db_farm = Farm(**farm.model_dump(), user_id=current_user.id)
    db.add(db_farm)
    db.commit()
    db.refresh(db_farm)
    return db_farm

@router.get("/farms", response_model=List[FarmSchema])
def get_farms(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    farms = db.query(Farm).filter(Farm.user_id == current_user.id).all()
    return farms

@router.get("/farms/{farm_id}", response_model=FarmSchema)
def get_farm(
    farm_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    farm = db.query(Farm).filter(
        Farm.id == farm_id,
        Farm.user_id == current_user.id
    ).first()
    
    if not farm:
        raise HTTPException(status_code=404, detail="Farm not found")
    
    return farm

@router.post("/fields", response_model=FieldSchema)
def create_field(
    field: FieldCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    # Check if farm belongs to user
    farm = db.query(Farm).filter(
        Farm.id == field.farm_id,
        Farm.user_id == current_user.id
    ).first()
    
    if not farm:        
        raise HTTPException(status_code=404, detail="Farm not found")
    
    payload = field.model_dump()
    _validate_field_payload(payload, payload["crop_type"])

    # Idempotency guard: if the same field submission is sent twice, return existing.
    if payload.get("client_id"):
        existing_by_client_id = db.query(Field).filter(
            Field.owner_id == current_user.id,
            Field.client_id == payload["client_id"],
            Field.is_deleted == False
        ).first()
        if existing_by_client_id:
            return existing_by_client_id

    duplicate_window_start = datetime.utcnow() - timedelta(seconds=15)
    existing_recent = db.query(Field).filter(
        Field.owner_id == current_user.id,
        Field.farm_id == payload["farm_id"],
        Field.name == payload["name"],
        Field.crop_type == payload["crop_type"],
        Field.crop_variety == payload.get("crop_variety"),
        Field.area_hectares == payload["area_hectares"],
        Field.gross_revenue == payload.get("gross_revenue"),
        Field.is_deleted == False,
        Field.created_at >= duplicate_window_start
    ).first()
    if existing_recent:
        return existing_recent

    db_field = Field(**payload, owner_id=current_user.id)
    db.add(db_field)
    db.commit()
    db.refresh(db_field)
    return db_field


@router.get("/fields", response_model=List[FieldSchema])
def get_fields(
    farm_id: int | None = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    query = db.query(Field).filter(
        Field.owner_id == current_user.id,
        Field.is_deleted == False
    )

    if farm_id is not None:
        query = query.filter(Field.farm_id == farm_id)

    return query.order_by(Field.created_at.desc()).all()


@router.get("/fields/completed", response_model=List[CompletedOperationHistorySchema])
def get_completed_fields(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    histories = db.query(CompletedOperationHistory).filter(
        CompletedOperationHistory.owner_id == current_user.id
    ).order_by(CompletedOperationHistory.completed_at.desc(), CompletedOperationHistory.id.desc()).all()
    return histories


@router.get("/fields/{field_id}", response_model=FieldSchema)
def get_field(
    field_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    field = db.query(Field).filter(
        Field.id == field_id,
        Field.owner_id == current_user.id,
        Field.is_deleted == False
    ).first()

    if not field:
        raise HTTPException(status_code=404, detail="Field not found")

    return field


@router.get("/farms/{farm_id}/fields", response_model=List[FieldSchema])
def get_farm_fields(
    farm_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    # Check if farm belongs to user
    farm = db.query(Farm).filter(
        Farm.id == farm_id,
        Farm.user_id == current_user.id
    ).first()
    
    if not farm:
        raise HTTPException(status_code=404, detail="Farm not found")
    
    fields = db.query(Field).filter(Field.farm_id == farm_id).all()
    return fields

@router.put("/farms/{farm_id}", response_model=schemas.Farm)
def update_farm(farm_id: int, updated_farm: schemas.FarmCreate, db: Session = Depends(get_db), current_user: models.User = Depends(get_current_user)):
    query = db.query(models.Farm).filter(models.Farm.id == farm_id, models.Farm.user_id == current_user.id)
    if not query.first():
        raise HTTPException(status_code=404, detail="Farm not found")
    query.update(updated_farm.model_dump(), synchronize_session=False)
    db.commit()
    return query.first()

@router.delete("/farms/{farm_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_farm(farm_id: int, db: Session = Depends(get_db), current_user: models.User = Depends(get_current_user)):
    query = db.query(models.Farm).filter(models.Farm.id == farm_id, models.Farm.user_id == current_user.id)
    if not query.first():
        raise HTTPException(status_code=404, detail="Farm not found")
    query.delete(synchronize_session=False)
    db.commit()

@router.delete("/fields/{field_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_field(
    field_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user)
):
    query = db.query(models.Field).filter(
        models.Field.id == field_id,
        models.Field.owner_id == current_user.id
    )
    if not query.first():
        raise HTTPException(status_code=404, detail="Field not found")
    query.delete(synchronize_session=False)
    db.commit()


@router.patch("/fields/{field_id}", response_model=FieldSchema)
def update_field(
    field_id: int,
    payload: FieldUpdate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user)
):
    query = db.query(models.Field).filter(
        models.Field.id == field_id,
        models.Field.owner_id == current_user.id
    )
    field = query.first()
    if not field:
        raise HTTPException(status_code=404, detail="Field not found")

    update_data = payload.model_dump(exclude_unset=True)
    next_crop_type = update_data.get("crop_type", field.crop_type)
    merged_payload = {
        "area_hectares": update_data.get("area_hectares", field.area_hectares),
        "gross_revenue": update_data.get("gross_revenue", field.gross_revenue),
    }
    _validate_field_payload(merged_payload, next_crop_type)

    for key, value in update_data.items():
        setattr(field, key, value)

    operation_status = update_data.get("operation_status")
    status = update_data.get("status")
    completed_at = update_data.get("completed_at")

    if operation_status == "completed" or status == "completed":
        mark_field_completed(db, field, completed_at)
    elif operation_status == "ongoing":
        reopen_field_operation(field)
    elif completed_at is not None and field.operation_status == "completed":
        mark_field_completed(db, field, completed_at)

    db.commit()
    db.refresh(field)
    return field
