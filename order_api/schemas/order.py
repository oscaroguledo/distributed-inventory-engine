import uuid

from pydantic import BaseModel, Field


class ReserveRequest(BaseModel):
    sku: str
    quantity: int = Field(gt=0)
    reservation_id: uuid.UUID
