import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Index, Integer, Text, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from order_api.models.base import Base


class ReconciliationLog(Base):
    """Append-only — durable record of every time the watchdog rebuilt
    Redis from Postgres, so a self-healed drift stays auditable."""

    __tablename__ = "reconciliation_log"
    __table_args__ = (Index("idx_reconciliation_log_sku_time", "sku", "occurred_at"),)

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    sku: Mapped[str] = mapped_column(Text, ForeignKey("inventory_balances.sku"), nullable=False)
    redis_available: Mapped[int] = mapped_column(
        Integer, nullable=False, comment="the stale/lost value Redis had before rebuild"
    )
    postgres_available: Mapped[int] = mapped_column(
        Integer, nullable=False, comment="the authoritative value Redis was rebuilt to"
    )
    drift: Mapped[int] = mapped_column(Integer, nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    def __repr__(self) -> str:
        return (
            f"<ReconciliationLog(id={self.id}, sku={self.sku}, "
            f"drift={self.drift}, occurred_at={self.occurred_at})>"
        )

    def to_dict(self) -> dict:
        return {
            "id": str(self.id),
            "sku": self.sku,
            "redis_available": self.redis_available,
            "postgres_available": self.postgres_available,
            "drift": self.drift,
            "occurred_at": self.occurred_at.isoformat() if self.occurred_at else None,
        }
