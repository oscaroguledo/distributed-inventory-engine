import uuid
from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Index, Integer, Text, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from order_api.models.base import Base


class StockAuditLedger(Base):
    """Append-only — never UPDATE or DELETE. No TimestampMixin;
    occurred_at is the only timestamp that applies here."""

    __tablename__ = "stock_audit_ledger"
    __table_args__ = (
        CheckConstraint(
            "event_type IN ('reserved', 'committed', 'released')",
            name="ck_stock_audit_ledger_event_type_valid",
        ),
        Index("idx_ledger_reservation", "reservation_id"),
        Index("idx_ledger_sku_time", "sku", "occurred_at"),
    )

    # Generated client-side (not a Postgres server_default): keeps the model
    # portable across dialects and avoids a DB-specific function dependency.
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    reservation_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("inventory_reservations.id"), nullable=False
    )
    sku: Mapped[str] = mapped_column(Text, nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    def __repr__(self) -> str:
        return (
            f"<StockAuditLedger(id={self.id}, reservation_id={self.reservation_id}, "
            f"sku={self.sku}, event_type={self.event_type})>"
        )

    def to_dict(self) -> dict:
        return {
            "id": str(self.id),
            "reservation_id": str(self.reservation_id),
            "sku": self.sku,
            "quantity": self.quantity,
            "event_type": self.event_type,
            "occurred_at": self.occurred_at.isoformat() if self.occurred_at else None,
        }
