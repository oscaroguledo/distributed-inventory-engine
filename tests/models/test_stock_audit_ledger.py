import uuid

from sqlalchemy import CheckConstraint, Index

from order_api.models.stock_audit_ledger import StockAuditLedger


def test_tablename():
    assert StockAuditLedger.__tablename__ == "stock_audit_ledger"


def test_columns():
    columns = {c.name: c for c in StockAuditLedger.__table__.columns}

    assert set(columns) == {
        "id",
        "reservation_id",
        "sku",
        "quantity",
        "event_type",
        "occurred_at",
    }
    assert columns["id"].primary_key is True
    assert columns["reservation_id"].nullable is False
    assert columns["sku"].nullable is False
    assert columns["quantity"].nullable is False
    assert columns["event_type"].nullable is False
    assert columns["occurred_at"].nullable is False


def test_no_updated_at_column():
    """The whole point of skipping TimestampMixin: this table is append-only."""
    columns = {c.name for c in StockAuditLedger.__table__.columns}
    assert "updated_at" not in columns
    assert "created_at" not in columns


def test_reservation_id_foreign_key_to_inventory_reservations():
    fks = list(StockAuditLedger.__table__.columns["reservation_id"].foreign_keys)
    assert len(fks) == 1
    assert next(iter(fks)).target_fullname == "inventory_reservations.id"


def test_check_constraint_present():
    check_names = {
        c.name for c in StockAuditLedger.__table__.constraints if isinstance(c, CheckConstraint)
    }
    assert "ck_stock_audit_ledger_event_type_valid" in check_names


def test_indexes_present():
    index_names = {idx.name for idx in StockAuditLedger.__table__.indexes if isinstance(idx, Index)}
    assert "idx_ledger_reservation" in index_names
    assert "idx_ledger_sku_time" in index_names


def test_repr_and_to_dict():
    entry_id = uuid.uuid4()
    reservation_id = uuid.uuid4()
    entry = StockAuditLedger(
        id=entry_id,
        reservation_id=reservation_id,
        sku="WIDGET-1",
        quantity=2,
        event_type="reserved",
    )

    assert str(entry_id) in repr(entry)
    as_dict = entry.to_dict()
    assert as_dict["reservation_id"] == str(reservation_id)
    assert as_dict["event_type"] == "reserved"
