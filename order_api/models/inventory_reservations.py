import uuid
from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Index, Integer, Text, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from order_api.models.base import Base, TimestampMixin


class InventoryReservation(Base, TimestampMixin):
    """Durable hold:{reservation_id} ticket, plus how it was resolved."""

    __tablename__ = "inventory_reservations"
    __table_args__ = (
        CheckConstraint("quantity > 0", name="ck_inventory_reservations_quantity_positive"),
        CheckConstraint(
            "status IN ('held', 'committed', 'released')",
            name="ck_inventory_reservations_status_valid",
        ),
        Index("idx_reservations_sku_status", "sku", "status"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        comment="same value as the client-supplied reservation_id",
    )
    sku: Mapped[str] = mapped_column(Text, ForeignKey("inventory_balances.sku"), nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    held_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    resolved_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, comment="NULL while status = held"
    )

    def __repr__(self) -> str:
        return (
            f"<InventoryReservation(id={self.id}, sku={self.sku}, "
            f"quantity={self.quantity}, status={self.status})>"
        )

    def to_dict(self) -> dict:
        return {
            "id": str(self.id),
            "sku": self.sku,
            "quantity": self.quantity,
            "status": self.status,
            "held_at": self.held_at.isoformat() if self.held_at else None,
            "resolved_at": self.resolved_at.isoformat() if self.resolved_at else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }
