from sqlalchemy import CheckConstraint, Index

from order_api.models.inventory_balances import InventoryBalance


def test_tablename():
    assert InventoryBalance.__tablename__ == "inventory_balances"


def test_columns():
    columns = {c.name: c for c in InventoryBalance.__table__.columns}

    assert set(columns) == {
        "sku",
        "name",
        "total_stock",
        "available",
        "created_at",
        "updated_at",
    }
    assert columns["sku"].primary_key is True
    assert columns["name"].nullable is False
    assert columns["total_stock"].nullable is False
    assert columns["available"].nullable is False
    assert columns["created_at"].nullable is False
    assert columns["updated_at"].nullable is False


def test_check_constraints_present():
    check_names = {
        c.name
        for c in InventoryBalance.__table__.constraints
        if isinstance(c, CheckConstraint)
    }

    assert "ck_inventory_balances_total_stock_non_negative" in check_names
    assert "ck_inventory_balances_available_non_negative" in check_names


def test_updated_at_index_present():
    index_names = {
        idx.name for idx in InventoryBalance.__table__.indexes if isinstance(idx, Index)
    }

    assert "idx_inventory_balances_updated_at" in index_names


def test_repr_and_to_dict():
    balance = InventoryBalance(sku="WIDGET-1", name="Widget", total_stock=100, available=80)

    assert "WIDGET-1" in repr(balance)
    as_dict = balance.to_dict()
    assert as_dict["sku"] == "WIDGET-1"
    assert as_dict["available"] == 80
    assert as_dict["created_at"] is None
    assert as_dict["updated_at"] is None
