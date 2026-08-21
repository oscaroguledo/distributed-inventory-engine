import uuid

from sqlalchemy import Index

from order_api.models.reconciliation_log import ReconciliationLog


def test_tablename():
    assert ReconciliationLog.__tablename__ == "reconciliation_log"


def test_columns():
    columns = {c.name: c for c in ReconciliationLog.__table__.columns}

    assert set(columns) == {
        "id",
        "sku",
        "redis_available",
        "postgres_available",
        "drift",
        "occurred_at",
    }
    assert columns["id"].primary_key is True
    assert columns["sku"].nullable is False
    assert columns["redis_available"].nullable is False
    assert columns["postgres_available"].nullable is False
    assert columns["drift"].nullable is False
    assert columns["occurred_at"].nullable is False


def test_no_updated_at_column():
    """Append-only, same as stock_audit_ledger: no TimestampMixin."""
    columns = {c.name for c in ReconciliationLog.__table__.columns}
    assert "updated_at" not in columns
    assert "created_at" not in columns


def test_sku_foreign_key_to_inventory_balances():
    fks = list(ReconciliationLog.__table__.columns["sku"].foreign_keys)
    assert len(fks) == 1
    assert next(iter(fks)).target_fullname == "inventory_balances.sku"


def test_index_present():
    index_names = {
        idx.name for idx in ReconciliationLog.__table__.indexes if isinstance(idx, Index)
    }
    assert "idx_reconciliation_log_sku_time" in index_names


def test_repr_and_to_dict():
    entry_id = uuid.uuid4()
    entry = ReconciliationLog(
        id=entry_id,
        sku="WIDGET-1",
        redis_available=999,
        postgres_available=40,
        drift=959,
    )

    assert str(entry_id) in repr(entry)
    assert "WIDGET-1" in repr(entry)
    as_dict = entry.to_dict()
    assert as_dict["sku"] == "WIDGET-1"
    assert as_dict["redis_available"] == 999
    assert as_dict["postgres_available"] == 40
    assert as_dict["drift"] == 959
