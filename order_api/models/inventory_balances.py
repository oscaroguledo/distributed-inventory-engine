from sqlalchemy import CheckConstraint, Index, Integer, Text
from sqlalchemy.orm import Mapped, mapped_column

from order_api.models.base import Base, TimestampMixin


class InventoryBalance(Base, TimestampMixin):
    """Durable mirror of Redis's stock:{sku}:available counter."""

    __tablename__ = "inventory_balances"
    __table_args__ = (
        CheckConstraint("total_stock >= 0", name="ck_inventory_balances_total_stock_non_negative"),
        CheckConstraint("available >= 0", name="ck_inventory_balances_available_non_negative"),
        Index("idx_inventory_balances_updated_at", "updated_at"),
    )

    sku: Mapped[str] = mapped_column(Text, primary_key=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    total_stock: Mapped[int] = mapped_column(Integer, nullable=False)
    available: Mapped[int] = mapped_column(Integer, nullable=False)

    def __repr__(self) -> str:
        return (
            f"<InventoryBalance(sku={self.sku}, name={self.name}, "
            f"total_stock={self.total_stock}, available={self.available}, "
            f"updated_at={self.updated_at})>"
        )

    def to_dict(self) -> dict:
        return {
            "sku": self.sku,
            "name": self.name,
            "total_stock": self.total_stock,
            "available": self.available,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }
