"""Contrat d'événement (section 4 du guide) vérifié par Pydantic v2."""
import uuid
from decimal import Decimal
from pydantic import BaseModel, ConfigDict, Field


class Item(BaseModel):
    model_config = ConfigDict(extra="forbid")
    sku: str = Field(min_length=1)
    qty: int = Field(gt=0)


class OrderEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")
    event_id: uuid.UUID
    event_type: str = Field(min_length=1)
    customer_id: int = Field(gt=0)
    order_id: int
    total: Decimal = Field(gt=0, max_digits=10, decimal_places=2)
    items: list[Item] = Field(min_length=1)
    created_at: str = Field(min_length=1)
