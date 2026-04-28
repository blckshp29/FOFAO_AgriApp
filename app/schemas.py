import json
from pydantic import AliasChoices, BaseModel, Field as PyField, ConfigDict, model_validator # Rename Field here
from typing import Optional, List, Dict, Any, Literal
from datetime import datetime, date
from pydantic import field_validator
from enum import Enum
# Import your models normally
from .models import User, ScheduledTask

class CropTypeEnum(str, Enum):
    coconut = "coconut"
    corn = "corn"
    rice = "rice"
    vegetables = "vegetables"

class SexEnum(str, Enum):
    M = "M"
    F = "F"

class SyncStatusEnum(str, Enum):
    pending = "pending"
    synced = "synced"
    conflict = "conflict"
    deleted = "deleted"

class ProjectStatusEnum(str, Enum):
    planned = "planned"
    active = "active"
    completed = "completed"
    archived = "archived"

class TransactionTypeEnum(str, Enum):
    income = "income"
    expense = "expense"

class OperationTypeEnum(str, Enum):
    land_preparation = "land_preparation"
    planting = "planting"
    fertilization = "fertilization"
    irrigation = "irrigation"
    pest_control = "pest_control"
    harvesting = "harvesting"

class TaskStatusEnum(str, Enum):
    pending = "pending"
    completed = "completed"
    cancelled = "cancelled"
    rescheduled = "rescheduled"

class OperationStatusEnum(str, Enum):
    ongoing = "ongoing"
    completed = "completed"

class OtpChannelEnum(str, Enum):
    email = "email"
    sms = "sms"

class SyncMeta(BaseModel):
    client_id: Optional[str] = None
    sync_status: Optional[SyncStatusEnum] = SyncStatusEnum.pending
    last_synced_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    deleted_at: Optional[datetime] = None
    is_deleted: Optional[bool] = False

    model_config = ConfigDict(from_attributes=True)

# --- User Schemas ---
class UserBase(BaseModel):
    username: str
    email: Optional[str] = None
    full_name: Optional[str] = None
    farm_name: Optional[str] = None
    client_id: Optional[str] = None
    sex: Optional[SexEnum] = None
    location: Optional[str] = None
    province: Optional[str] = None
    city_municipality: Optional[str] = None
    barangay: Optional[str] = None
    mobile_number: Optional[str] = None
    birthdate: Optional[date] = None

    @field_validator("sex", mode="before")
    @classmethod
    def normalize_sex(cls, v):
        if v is None or isinstance(v, SexEnum):
            return v
        if isinstance(v, str):
            val = v.strip().lower()
            if val in {"m", "male"}:
                return SexEnum.M
            if val in {"f", "female"}:
                return SexEnum.F
        return v

    @field_validator("birthdate", mode="before")
    @classmethod
    def parse_birthdate(cls, v):
        if v is None or isinstance(v, date):
            return v
        if isinstance(v, str):
            try:
                return datetime.strptime(v, "%d/%m/%Y").date()
            except ValueError:
                raise ValueError("birthdate must be in dd/mm/yyyy format")
        return v

class UserCreate(UserBase):
    password: str
    otp_code: Optional[str] = None

class RegisterRequest(BaseModel):
    username: str
    password: str

class UserUpdate(BaseModel):
    full_name: Optional[str] = None
    farm_name: Optional[str] = None
    sex: Optional[SexEnum] = None
    location: Optional[str] = None
    province: Optional[str] = None
    city_municipality: Optional[str] = None
    barangay: Optional[str] = None
    mobile_number: Optional[str] = None
    birthdate: Optional[date] = None

    @field_validator("sex", mode="before")
    @classmethod
    def normalize_sex(cls, v):
        if v is None or isinstance(v, SexEnum):
            return v
        if isinstance(v, str):
            val = v.strip().lower()
            if val in {"m", "male"}:
                return SexEnum.M
            if val in {"f", "female"}:
                return SexEnum.F
        return v

    @field_validator("birthdate", mode="before")
    @classmethod
    def parse_birthdate(cls, v):
        if v is None or isinstance(v, date):
            return v
        if isinstance(v, str):
            try:
                return datetime.strptime(v, "%d/%m/%Y").date()
            except ValueError:
                raise ValueError("birthdate must be in dd/mm/yyyy format")
        return v

class PasswordChangeRequest(BaseModel):
    current_password: str
    new_password: str

class UserLogin(BaseModel):
    identifier: str
    password: str

class User(UserBase):
    id: int
    created_at: datetime
    updated_at: Optional[datetime] = None
    email_verified: bool = False
    phone_verified: bool = False
    last_login_at: Optional[datetime] = None
    last_synced_at: Optional[datetime] = None
    sync_status: Optional[SyncStatusEnum] = SyncStatusEnum.pending
    
    model_config = ConfigDict(from_attributes=True)

class UserPreferenceBase(BaseModel):
    email_notifications: bool = True
    sms_notifications: bool = False
    push_notifications: bool = False
    marketing_notifications: bool = False
    language: str = "en"
    timezone: str = "Asia/Manila"

class UserPreferenceUpdate(BaseModel):
    email_notifications: Optional[bool] = None
    sms_notifications: Optional[bool] = None
    push_notifications: Optional[bool] = None
    marketing_notifications: Optional[bool] = None
    language: Optional[str] = None
    timezone: Optional[str] = None

class UserPreference(UserPreferenceBase):
    id: int
    user_id: int
    created_at: datetime
    updated_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)

class Notification(BaseModel):
    id: int
    user_id: int
    title: str
    message: str
    type: str
    data: Optional[Dict[str, Any]] = None
    is_read: bool
    created_at: datetime
    read_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)

    @field_validator("data", mode="before")
    @classmethod
    def parse_notification_data(cls, value):
        if value is None or isinstance(value, dict):
            return value
        if isinstance(value, str):
            try:
                return json.loads(value)
            except Exception:
                return None
        return None

class NotificationCreate(BaseModel):
    title: str
    message: str
    type: str = "system"
    data: Optional[Dict[str, Any]] = None

class FCMTokenUpsert(BaseModel):
    token: str
    device_type: str = "web"

class FCMToken(BaseModel):
    id: int
    user_id: int
    token: str
    device_type: str
    is_active: bool
    created_at: datetime
    updated_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)

class PushNotificationRequest(BaseModel):
    title: str
    body: str
    data: Optional[Dict[str, str]] = None
    topic: Optional[str] = None

# --- OTP Schemas ---
class OtpRequest(BaseModel):
    channel: OtpChannelEnum
    destination: str  # email or mobile number

class OtpVerify(BaseModel):
    channel: OtpChannelEnum
    destination: str
    code: str

class OtpResponse(BaseModel):
    success: bool
    message: str

# --- Farm Schemas ---
class FarmBase(BaseModel):
    name: str
    area_hectares: Optional[float] = None
    soil_type: Optional[str] = None
    client_id: Optional[str] = None
    location: Optional[str] = None
    province: Optional[str] = None
    city_municipality: Optional[str] = None
    barangay: Optional[str] = None
    location_lat: Optional[float] = None
    location_lon: Optional[float] = None

class FarmCreate(FarmBase):
    pass

class Farm(FarmBase):
    id: int
    user_id: int
    created_at: datetime
    updated_at: Optional[datetime] = None
    last_synced_at: Optional[datetime] = None
    sync_status: Optional[SyncStatusEnum] = SyncStatusEnum.pending
    
    model_config = ConfigDict(from_attributes=True)

# --- Field Schemas ---
class FieldBase(BaseModel):
    name: str
    area_hectares: Optional[float] = None
    crop_type: CropTypeEnum
    crop_variety: Optional[str] = None
    gross_revenue: Optional[float] = None
    client_id: Optional[str] = None
    planting_date: Optional[datetime] = None
    land_prep_start_date: Optional[datetime] = None
    corn_type: Optional[str] = PyField(default=None, validation_alias=AliasChoices("corn_type", "cornType"))
    intended_use: Optional[str] = PyField(default=None, validation_alias=AliasChoices("intended_use", "intendedUse"))
    planting_method: Optional[str] = PyField(default=None, validation_alias=AliasChoices("planting_method", "plantingMethod"))
    seed_rate_kg_per_ha: Optional[float] = PyField(
        default=None,
        validation_alias=AliasChoices("seed_rate_kg_per_ha", "seedRateKgPerHa")
    )
    seed_bags_count: Optional[float] = PyField(default=None, validation_alias=AliasChoices("seed_bags_count", "seedBagsCount"))
    row_spacing_cm: Optional[float] = PyField(default=None, validation_alias=AliasChoices("row_spacing_cm", "rowSpacingCm"))
    hill_spacing_cm: Optional[float] = PyField(default=None, validation_alias=AliasChoices("hill_spacing_cm", "hillSpacingCm"))
    target_plant_population: Optional[int] = PyField(
        default=None,
        validation_alias=AliasChoices("target_plant_population", "targetPlantPopulation")
    )
    irrigation_source: Optional[str] = PyField(default=None, validation_alias=AliasChoices("irrigation_source", "irrigationSource"))
    drainage_condition: Optional[str] = PyField(default=None, validation_alias=AliasChoices("drainage_condition", "drainageCondition"))
    previous_crop: Optional[str] = PyField(default=None, validation_alias=AliasChoices("previous_crop", "previousCrop"))
    basal_fertilizer_type: Optional[str] = PyField(
        default=None,
        validation_alias=AliasChoices("basal_fertilizer_type", "basalFertilizerType")
    )
    basal_fertilizer_rate: Optional[float] = PyField(
        default=None,
        validation_alias=AliasChoices("basal_fertilizer_rate", "basalFertilizerRate")
    )
    side_dress_fertilizer_type: Optional[str] = PyField(
        default=None,
        validation_alias=AliasChoices("side_dress_fertilizer_type", "sideDressFertilizerType")
    )
    side_dress_fertilizer_rate: Optional[float] = PyField(
        default=None,
        validation_alias=AliasChoices("side_dress_fertilizer_rate", "sideDressFertilizerRate")
    )
    pest_observations: Optional[str] = PyField(default=None, validation_alias=AliasChoices("pest_observations", "pestObservations"))
    disease_observations: Optional[str] = PyField(
        default=None,
        validation_alias=AliasChoices("disease_observations", "diseaseObservations")
    )
    infestation_severity: Optional[str] = PyField(
        default=None,
        validation_alias=AliasChoices("infestation_severity", "infestationSeverity")
    )
    last_spray_date: Optional[datetime] = PyField(default=None, validation_alias=AliasChoices("last_spray_date", "lastSprayDate"))
    pesticide_used: Optional[str] = PyField(default=None, validation_alias=AliasChoices("pesticide_used", "pesticideUsed"))
    moisture_content_at_harvest: Optional[float] = PyField(
        default=None,
        validation_alias=AliasChoices("moisture_content_at_harvest", "moistureContentAtHarvest")
    )
    expected_yield_per_ha: Optional[float] = PyField(
        default=None,
        validation_alias=AliasChoices("expected_yield_per_ha", "expectedYieldPerHa")
    )
    actual_yield: Optional[float] = PyField(default=None, validation_alias=AliasChoices("actual_yield", "actualYield"))
    market_price_per_unit: Optional[float] = PyField(
        default=None,
        validation_alias=AliasChoices("market_price_per_unit", "marketPricePerUnit")
    )
    operation_status: Optional[OperationStatusEnum] = PyField(
        default=OperationStatusEnum.ongoing,
        validation_alias=AliasChoices("operation_status", "operationStatus")
    )
    status: Optional[str] = "ongoing"
    completed_at: Optional[datetime] = PyField(
        default=None,
        validation_alias=AliasChoices("completed_at", "completedAt")
    )
    location_lat: Optional[float] = None
    location_lon: Optional[float] = None

    @field_validator("gross_revenue")
    @classmethod
    def validate_gross_revenue(cls, value):
        if value is not None and value < 0:
            raise ValueError("gross_revenue must be greater than or equal to 0")
        return value

    @field_validator("area_hectares")
    @classmethod
    def validate_area_hectares(cls, value):
        if value is not None and value < 0:
            raise ValueError("area_hectares must be greater than or equal to 0")
        return value

    @field_validator("location_lat", "location_lon", mode="before")
    @classmethod
    def empty_string_to_none(cls, value):
        if value == "":
            return None
        return value

    @field_validator("area_hectares", "gross_revenue", mode="before")
    @classmethod
    def numeric_empty_string_to_none(cls, value):
        if value == "":
            return None
        return value

    @field_validator("crop_type", mode="after")
    @classmethod
    def validate_crop_specific_requirements(cls, value, info):
        data = info.data
        area_hectares = data.get("area_hectares")
        gross_revenue = data.get("gross_revenue")

        if value == CropTypeEnum.coconut:
            if gross_revenue is not None and gross_revenue < 0:
                raise ValueError("gross_revenue must be greater than or equal to 0")
            return value

        if area_hectares is None or area_hectares <= 0:
            raise ValueError("area_hectares must be greater than 0 for non-coconut crops")

        return value

class FieldCreate(FieldBase):
    farm_id: int


class FieldUpdate(BaseModel):
    name: Optional[str] = None
    area_hectares: Optional[float] = None
    crop_type: Optional[CropTypeEnum] = None
    crop_variety: Optional[str] = None
    gross_revenue: Optional[float] = None
    client_id: Optional[str] = None
    planting_date: Optional[datetime] = None
    land_prep_start_date: Optional[datetime] = None
    corn_type: Optional[str] = PyField(default=None, validation_alias=AliasChoices("corn_type", "cornType"))
    intended_use: Optional[str] = PyField(default=None, validation_alias=AliasChoices("intended_use", "intendedUse"))
    planting_method: Optional[str] = PyField(default=None, validation_alias=AliasChoices("planting_method", "plantingMethod"))
    seed_rate_kg_per_ha: Optional[float] = PyField(default=None, validation_alias=AliasChoices("seed_rate_kg_per_ha", "seedRateKgPerHa"))
    seed_bags_count: Optional[float] = PyField(default=None, validation_alias=AliasChoices("seed_bags_count", "seedBagsCount"))
    row_spacing_cm: Optional[float] = PyField(default=None, validation_alias=AliasChoices("row_spacing_cm", "rowSpacingCm"))
    hill_spacing_cm: Optional[float] = PyField(default=None, validation_alias=AliasChoices("hill_spacing_cm", "hillSpacingCm"))
    target_plant_population: Optional[int] = PyField(default=None, validation_alias=AliasChoices("target_plant_population", "targetPlantPopulation"))
    irrigation_source: Optional[str] = PyField(default=None, validation_alias=AliasChoices("irrigation_source", "irrigationSource"))
    drainage_condition: Optional[str] = PyField(default=None, validation_alias=AliasChoices("drainage_condition", "drainageCondition"))
    previous_crop: Optional[str] = PyField(default=None, validation_alias=AliasChoices("previous_crop", "previousCrop"))
    basal_fertilizer_type: Optional[str] = PyField(default=None, validation_alias=AliasChoices("basal_fertilizer_type", "basalFertilizerType"))
    basal_fertilizer_rate: Optional[float] = PyField(default=None, validation_alias=AliasChoices("basal_fertilizer_rate", "basalFertilizerRate"))
    side_dress_fertilizer_type: Optional[str] = PyField(default=None, validation_alias=AliasChoices("side_dress_fertilizer_type", "sideDressFertilizerType"))
    side_dress_fertilizer_rate: Optional[float] = PyField(default=None, validation_alias=AliasChoices("side_dress_fertilizer_rate", "sideDressFertilizerRate"))
    pest_observations: Optional[str] = PyField(default=None, validation_alias=AliasChoices("pest_observations", "pestObservations"))
    disease_observations: Optional[str] = PyField(default=None, validation_alias=AliasChoices("disease_observations", "diseaseObservations"))
    infestation_severity: Optional[str] = PyField(default=None, validation_alias=AliasChoices("infestation_severity", "infestationSeverity"))
    last_spray_date: Optional[datetime] = PyField(default=None, validation_alias=AliasChoices("last_spray_date", "lastSprayDate"))
    pesticide_used: Optional[str] = PyField(default=None, validation_alias=AliasChoices("pesticide_used", "pesticideUsed"))
    moisture_content_at_harvest: Optional[float] = PyField(default=None, validation_alias=AliasChoices("moisture_content_at_harvest", "moistureContentAtHarvest"))
    expected_yield_per_ha: Optional[float] = PyField(default=None, validation_alias=AliasChoices("expected_yield_per_ha", "expectedYieldPerHa"))
    actual_yield: Optional[float] = PyField(default=None, validation_alias=AliasChoices("actual_yield", "actualYield"))
    market_price_per_unit: Optional[float] = PyField(default=None, validation_alias=AliasChoices("market_price_per_unit", "marketPricePerUnit"))
    operation_status: Optional[OperationStatusEnum] = PyField(
        default=None,
        validation_alias=AliasChoices("operation_status", "operationStatus")
    )
    status: Optional[str] = None
    completed_at: Optional[datetime] = PyField(
        default=None,
        validation_alias=AliasChoices("completed_at", "completedAt")
    )
    location_lat: Optional[float] = None
    location_lon: Optional[float] = None

    @field_validator("location_lat", "location_lon", mode="before")
    @classmethod
    def empty_location_string_to_none(cls, value):
        if value == "":
            return None
        return value

    @field_validator("area_hectares", "gross_revenue", mode="before")
    @classmethod
    def empty_numeric_string_to_none(cls, value):
        if value == "":
            return None
        return value

    @field_validator("gross_revenue")
    @classmethod
    def validate_updated_gross_revenue(cls, value):
        if value is not None and value < 0:
            raise ValueError("gross_revenue must be greater than or equal to 0")
        return value

    @field_validator("area_hectares")
    @classmethod
    def validate_updated_area_hectares(cls, value):
        if value is not None and value < 0:
            raise ValueError("area_hectares must be greater than or equal to 0")
        return value

class Field(FieldBase):
    id: int
    farm_id: int
    current_stage: str
    expected_harvest_date: Optional[datetime] = None
    created_at: datetime
    updated_at: Optional[datetime] = None
    last_synced_at: Optional[datetime] = None
    sync_status: Optional[SyncStatusEnum] = SyncStatusEnum.pending
    
    model_config = ConfigDict(from_attributes=True)


class CompletedOperationHistory(BaseModel):
    id: int
    owner_id: int
    field_id: int
    farm_id: Optional[int] = None
    project_id: Optional[int] = None
    crop_type: CropTypeEnum
    crop_variety: Optional[str] = None
    season_label: Optional[str] = None
    season_year: Optional[int] = None
    start_date: Optional[datetime] = None
    completed_at: datetime
    planned_budget: float = 0
    actual_cost: float = 0
    actual_yield: float = 0
    actual_revenue: float = 0
    location: Optional[str] = None
    task_history: List[Dict[str, Any]] = PyField(default_factory=list)
    category_costs: Dict[str, float] = PyField(default_factory=dict)
    financial_snapshot: Dict[str, Any] = PyField(default_factory=dict)
    created_at: datetime
    updated_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)

    @field_validator("task_history", mode="before")
    @classmethod
    def parse_task_history(cls, value):
        if value is None:
            return []
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
                return parsed if isinstance(parsed, list) else []
            except Exception:
                return []
        return []

    @field_validator("category_costs", mode="before")
    @classmethod
    def parse_category_costs(cls, value):
        if value is None:
            return {}
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
                return parsed if isinstance(parsed, dict) else {}
            except Exception:
                return {}
        return {}

    @field_validator("financial_snapshot", mode="before")
    @classmethod
    def parse_financial_snapshot(cls, value):
        if value is None:
            return {}
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
                return parsed if isinstance(parsed, dict) else {}
            except Exception:
                return {}
        return {}

# --- Inventory Schemas ---
class InventoryBase(BaseModel):
    item_name: str
    category: str
    quantity: float
    unit: str
    unit_cost: float
    client_id: Optional[str] = None

class InventoryCreate(InventoryBase):
    farm_id: int

class Inventory(InventoryBase):
    id: int
    farm_id: int
    created_at: datetime
    updated_at: Optional[datetime] = None
    last_synced_at: Optional[datetime] = None
    sync_status: Optional[SyncStatusEnum] = SyncStatusEnum.pending
    
    model_config = ConfigDict(from_attributes=True)

# --- Crop Project Schemas ---
class CropProjectBase(BaseModel):
    name: str
    crop_type: CropTypeEnum
    crop_variety: Optional[str] = None
    budget_total: float = 0
    currency: str = "PHP"
    client_id: Optional[str] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    notes: Optional[str] = None
    farm_id: Optional[int] = None
    field_id: Optional[int] = None

    @model_validator(mode="after")
    def validate_budget_total_for_crop(self):
        if self.crop_type != CropTypeEnum.coconut and (self.budget_total is None or self.budget_total <= 0):
            raise ValueError("budget_total must be greater than 0 for non-coconut projects")
        if self.crop_type == CropTypeEnum.coconut and self.budget_total is not None and self.budget_total < 0:
            raise ValueError("budget_total must be greater than or equal to 0")
        return self

class CropProjectCreate(CropProjectBase):
    pass

class CropProjectUpdate(BaseModel):
    name: Optional[str] = None
    crop_type: Optional[CropTypeEnum] = None
    crop_variety: Optional[str] = None
    budget_total: Optional[float] = None
    budget_remaining: Optional[float] = None
    income_total: Optional[float] = None
    expense_total: Optional[float] = None
    currency: Optional[str] = None
    status: Optional[ProjectStatusEnum] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    notes: Optional[str] = None
    farm_id: Optional[int] = None
    field_id: Optional[int] = None

    @field_validator("budget_total")
    @classmethod
    def validate_updated_budget_total(cls, value):
        if value is not None and value < 0:
            raise ValueError("budget_total must be greater than or equal to 0")
        return value

    @field_validator("budget_remaining")
    @classmethod
    def validate_budget_remaining(cls, value):
        if value is not None and value < 0:
            raise ValueError("budget_remaining must be greater than or equal to 0")
        return value

class CropProject(CropProjectBase):
    id: int
    owner_id: int
    budget_remaining: float = 0
    income_total: float = 0
    expense_total: float = 0
    status: ProjectStatusEnum = ProjectStatusEnum.planned
    completed_at: Optional[datetime] = None
    created_at: datetime
    updated_at: Optional[datetime] = None
    last_synced_at: Optional[datetime] = None
    sync_status: Optional[SyncStatusEnum] = SyncStatusEnum.pending

    model_config = ConfigDict(from_attributes=True)


class ProjectCompletionResponse(BaseModel):
    id: int
    field_id: Optional[int] = None
    crop_type: CropTypeEnum
    crop_variety: Optional[str] = None
    status: ProjectStatusEnum
    completed_at: Optional[datetime] = None
    message: str

    model_config = ConfigDict(from_attributes=True)


class CompletedProjectListItem(BaseModel):
    id: int
    name: str
    field_id: Optional[int] = None
    field_name: Optional[str] = None
    crop_type: CropTypeEnum
    crop_variety: Optional[str] = None
    area_hectares: Optional[float] = None
    started_at: Optional[datetime] = None
    created_at: datetime
    completed_at: Optional[datetime] = None
    status: ProjectStatusEnum
    gross_revenue: Optional[float] = None
    total_budget: float = 0
    total_expenses: float = 0
    total_income: float = 0

    model_config = ConfigDict(from_attributes=True)

# --- Financial Record Schemas ---
class FinancialRecordBase(BaseModel):
    transaction_type: TransactionTypeEnum = PyField(
        validation_alias=AliasChoices("transaction_type", "transactionType", "type")
    )
    category: str
    amount: float
    currency: str = "PHP"
    description: Optional[str] = None
    client_id: Optional[str] = PyField(default=None, validation_alias=AliasChoices("client_id", "clientId"))
    is_history: Optional[bool] = PyField(default=False, validation_alias=AliasChoices("is_history", "isHistory"))
    field_id: Optional[int] = PyField(default=None, validation_alias=AliasChoices("field_id", "fieldId"))
    project_id: Optional[int] = PyField(default=None, validation_alias=AliasChoices("project_id", "projectId"))
    is_over_budget: Optional[bool] = PyField(default=False, validation_alias=AliasChoices("is_over_budget", "isOverBudget"))
    over_budget_approved: Optional[bool] = PyField(
        default=False,
        validation_alias=AliasChoices("over_budget_approved", "overBudgetApproved")
    )
    budget_snapshot: Optional[float] = PyField(
        default=None,
        validation_alias=AliasChoices("budget_snapshot", "budgetSnapshot")
    )

    @field_validator("transaction_type", mode="before")
    @classmethod
    def normalize_transaction_type(cls, value):
        if value is None or isinstance(value, TransactionTypeEnum):
            return value
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"income", "expense"}:
                return normalized
        return value

    @field_validator("category", mode="before")
    @classmethod
    def normalize_category(cls, value):
        if value is None:
            return "Miscellaneous"
        if isinstance(value, str):
            cleaned = value.strip()
            return cleaned or "Miscellaneous"
        return value

class FinancialRecordCreate(FinancialRecordBase):
    pass

class FinancialRecord(FinancialRecordBase):
    id: int
    owner_id: int
    date: datetime
    created_at: datetime
    updated_at: Optional[datetime] = None
    last_synced_at: Optional[datetime] = None
    sync_status: Optional[SyncStatusEnum] = SyncStatusEnum.pending
    
    model_config = ConfigDict(from_attributes=True)

# --- Scheduled Task Schemas ---
class ScheduledTaskBase(BaseModel):    
    task_type: OperationTypeEnum
    task_name: str
    description: Optional[str] = None
    scheduled_date: datetime
    client_id: Optional[str] = None
    original_scheduled_date: Optional[datetime] = None
    rescheduled_reason: Optional[str] = None
    estimated_cost: float
    requires_dry_weather: bool = True
    requires_network: bool = False
    priority: int = PyField(default=1, ge=1, le=5) 
    status: Optional[TaskStatusEnum] = None  # Crucial for changing "pending" to "completed"
    actual_cost: Optional[float] = None
    weather_check_date: Optional[datetime] = None
    weather_status: Optional[str] = None
    cycle_number: Optional[int] = PyField(default=None, ge=1, le=2)
    cycle_day: Optional[int] = PyField(default=None, ge=0)
    completed_at: Optional[datetime] = None
    confirmed_by_user: Optional[bool] = False
    early_completed: Optional[bool] = False
    early_completion_reason: Optional[str] = None
    early_completion_warning_acknowledged: Optional[bool] = False
    early_completion_days: Optional[int] = 0
    field_id: int
    project_id: Optional[int] = None

    model_config = ConfigDict(from_attributes=True)

class ScheduledTaskCreate(ScheduledTaskBase):
    pass # Removed field_id here because it's already in the Base

class ScheduledTask(ScheduledTaskBase):
    id: int
    user_id: int
    status: TaskStatusEnum
    actual_cost: Optional[float] = None
    decision_tree_recommendation: bool
    tomorrow_check_at: Optional[datetime] = None
    tomorrow_notification_sent_at: Optional[datetime] = None
    tomorrow_notification_type: Optional[str] = None
    created_at: datetime
    updated_at: Optional[datetime] = None
    last_synced_at: Optional[datetime] = None
    sync_status: Optional[SyncStatusEnum] = SyncStatusEnum.pending
    
    model_config = ConfigDict(from_attributes=True)

    # --- Add this to app/schemas.py ---

class ScheduledTaskUpdate(BaseModel):
    task_type: Optional[str] = None
    task_name: Optional[str] = None
    description: Optional[str] = None
    scheduled_date: Optional[datetime] = None
    original_scheduled_date: Optional[datetime] = None
    rescheduled_reason: Optional[str] = None
    estimated_cost: Optional[float] = None
    requires_dry_weather: Optional[bool] = None
    requires_network: Optional[bool] = None
    priority: Optional[int] = None
    status: Optional[TaskStatusEnum] = None
    actual_cost: Optional[float] = None
    weather_check_date: Optional[datetime] = None
    weather_status: Optional[str] = None
    cycle_number: Optional[int] = PyField(default=None, ge=1, le=2)
    cycle_day: Optional[int] = PyField(default=None, ge=0)
    completed_at: Optional[datetime] = None
    confirmed_by_user: Optional[bool] = None
    early_completed: Optional[bool] = None
    early_completion_reason: Optional[str] = None
    early_completion_warning_acknowledged: Optional[bool] = None
    early_completion_days: Optional[int] = None
    confirm_early_completion: Optional[bool] = PyField(
        default=None,
        validation_alias=AliasChoices("confirm_early_completion", "confirmEarlyCompletion")
    )
    field_id: Optional[int] = None
    project_id: Optional[int] = None

    model_config = ConfigDict(from_attributes=True)

# --- Weather Data Schemas ---
class WeatherDataBase(BaseModel):
    location_lat: float
    location_lon: float
    date: datetime

class WeatherForecastRequest(BaseModel):
    # Ensure there is a COLON (:) and an EQUALS (=)
    latitude: float = PyField(..., ge=-90, le=90)
    longitude: float = PyField(..., ge=-180, le=180)
    days: int = PyField(default=5, ge=1, le=5) # OpenWeatherMap 5-day forecast limit

class WeatherCurrentResponse(BaseModel):
    temperature: Optional[float] = None
    humidity: Optional[float] = None
    wind_speed: Optional[float] = None
    precipitation: Optional[float] = None
    condition: Optional[str] = None
    description: Optional[str] = None
    icon: Optional[str] = None

class WeatherForecastResponse(BaseModel):
    latitude: float
    longitude: float
    hourly: List[Dict[str, Any]]
    daily: List[Dict[str, Any]]
    retrieved_at: datetime

# --- Decision Tree Schemas ---
class DecisionTreeRequest(BaseModel):
    field_id: int
    operation_type: OperationTypeEnum
    budget_constraint: Optional[float] = None

class DecisionTreeResponse(BaseModel):
    recommended_date: datetime
    confidence_score: float
    estimated_cost: float
    weather_risk: str
    net_financial_return: Optional[float] = None
    recommendation_reason: str

# --- Optimization Request/Response ---
class OptimizationRequest(BaseModel):
    field_id: int
    operation_type: OperationTypeEnum
    current_budget: float

class OptimizationResponse(BaseModel):
    optimal_date: datetime
    predicted_yield_value: float
    total_projected_cost: float
    net_financial_return: float
    weather_conditions: Dict[str, Any]
    budget_constraint_satisfied: bool
    recommendation: str

# --- Rice Schedule Schemas ---
class RiceScheduleRequest(BaseModel):
    land_prep_start_date: Optional[datetime] = None
    crop_variety: Optional[str] = None


class CornScheduleRequest(BaseModel):
    planting_date: Optional[datetime] = None
    corn_type: Optional[str] = None
    crop_variety: Optional[str] = None
    force_regenerate: bool = False

# --- Partial Budgeting Schemas ---
class PartialBudgetingInput(BaseModel):
    added_returns: float = 0
    reduced_costs: float = 0
    added_costs: float = 0
    reduced_returns: float = 0

class PartialBudgetingResponse(BaseModel):
    net_benefit: float
    is_profitable: bool
    recommendation: str

# --- Insights Schemas ---
class FinanceBarItem(BaseModel):
    label: str
    percent: float
    value: float

class InsightSummary(BaseModel):
    budget_total: float
    expenses_total: float
    income_total: float
    net_profit: float
    is_over_budget: bool
    budget_bar: FinanceBarItem
    expenses_bar: FinanceBarItem
    income_bar: FinanceBarItem

class InsightComparison(BaseModel):
    previous_label: str
    current_label: str
    previous_expenses_percent: float
    current_expenses_percent: float
    previous_netprofit_percent: float
    current_netprofit_percent: float

class BudgetAllocationItem(BaseModel):
    category: str
    min_amount: float
    max_amount: float
    recommended_amount: float
    percent_of_total: float

class CornYieldIncomeEstimate(BaseModel):
    unit: str
    yield_min: float
    yield_max: float
    yield_recommended: float
    price_min: float
    price_max: float
    price_recommended: float
    income_min: float
    income_max: float
    income_recommended: float

class CornBudgetTemplateResponse(BaseModel):
    crop_type: str
    corn_type: str
    hectares: float
    currency: str
    budget_min: float
    budget_max: float
    budget_recommended: float
    allocations: List[BudgetAllocationItem]
    expected_returns: CornYieldIncomeEstimate

class CoconutBudgetTemplateResponse(BaseModel):
    net_revenue: float
    owner_share: float
    tenant_share: Optional[float] = None
    labor_total: float
    labor_individual: float
    number_of_labors: int


class CoconutAllocationSaveRequest(BaseModel):
    gross_revenue: Optional[float] = None
    arrastre_cost: float = 0
    food_cost: float = 0
    number_of_labors: int
    contract_type: Literal["50_50", "60_40", "tercia"]

    @field_validator("gross_revenue", "arrastre_cost", "food_cost")
    @classmethod
    def validate_non_negative_amounts(cls, value):
        if value is not None and value < 0:
            raise ValueError("amounts must be greater than or equal to 0")
        return value

    @field_validator("number_of_labors")
    @classmethod
    def validate_number_of_labors(cls, value):
        if value <= 0:
            raise ValueError("number_of_labors must be greater than 0")
        return value


class CoconutAllocationResponse(BaseModel):
    id: int
    project_id: int
    gross_revenue: float
    arrastre_cost: float
    food_cost: float
    number_of_labors: int
    contract_type: Literal["50_50", "60_40", "tercia"]
    net_revenue: float
    owner_share: float
    tenant_share: Optional[float] = None
    labor_total: float
    labor_individual: float
    created_at: datetime
    updated_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)

class VegetableBudgetTemplateResponse(BaseModel):
    crop_type: str
    vegetable_type: str
    hectares: float
    currency: str
    budget_min: float
    budget_max: float
    budget_recommended: float
    allocations: List[BudgetAllocationItem]

class RiceBudgetTemplateResponse(BaseModel):
    crop_type: str
    crop_variety: str
    hectares: float
    currency: str
    budget_min: float
    budget_max: float
    budget_recommended: float
    allocations: List[BudgetAllocationItem]

# --- Sync Schemas ---
class SyncEntityEnum(str, Enum):
    farm = "farm"
    field = "field"
    inventory = "inventory"
    project = "project"
    financial_record = "financial_record"
    scheduled_task = "scheduled_task"

class SyncPushItem(BaseModel):
    entity: SyncEntityEnum
    data: Dict[str, Any]
    updated_at: Optional[datetime] = None
    is_deleted: Optional[bool] = False

class SyncPushRequest(BaseModel):
    client_id: str
    items: List[SyncPushItem]

class SyncConflictItem(BaseModel):
    entity: SyncEntityEnum
    server_id: int
    client_id: Optional[str]
    reason: str

class SyncPushResponse(BaseModel):
    accepted: int
    conflicts: List[SyncConflictItem]

class SyncPullResponse(BaseModel):
    items: List[SyncPushItem]

# --- Token and Authentication ---
class Token(BaseModel):
    access_token: str
    token_type: str

class TokenData(BaseModel):
    username: Optional[str] = None
