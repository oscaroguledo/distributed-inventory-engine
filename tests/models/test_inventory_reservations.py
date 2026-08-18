import uuid

from sqlalchemy import CheckConstraint, Index

from order_api.models.inventory_reservations import InventoryReservation


def test_tablename():
    assert InventoryReservation.__tablename__ == "inventory_reservations"


def test_columns():
    columns = {c.name: c for c in InventoryReservation.__table__.columns}

    assert set(columns) == {
        "id",
        "sku",
        "quantity",
        "status",
        "held_at",
        "resolved_at",
        "created_at",
        "updated_at",
    }
    assert columns["id"].primary_key is True
    assert columns["sku"].nullable is False
    assert columns["quantity"].nullable is False
    assert columns["status"].nullable is False
    assert columns["held_at"].nullable is False
    assert columns["resolved_at"].nullable is True
    assert columns["created_at"].nullable is False
    assert columns["updated_at"].nullable is False


def test_sku_foreign_key_to_inventory_balances():
    fks = list(InventoryReservation.__table__.columns["sku"].foreign_keys)
    assert len(fks) == 1
    assert next(iter(fks)).target_fullname == "inventory_balances.sku"


def test_check_constraints_present():
    check_names = {
        c.name
        for c in InventoryReservation.__table__.constraints
        if isinstance(c, CheckConstraint)
    }

    assert "ck_inventory_reservations_quantity_positive" in check_names
    assert "ck_inventory_reservations_status_valid" in check_names


def test_sku_status_index_present():
    index_names = {
        idx.name for idx in InventoryReservation.__table__.indexes if isinstance(idx, Index)
    }

    assert "idx_reservations_sku_status" in index_names


def test_repr_and_to_dict():
    reservation_id = uuid.uuid4()
    reservation = InventoryReservation(
        id=reservation_id, sku="WIDGET-1", quantity=2, status="held"
    )

    assert str(reservation_id) in repr(reservation)
    as_dict = reservation.to_dict()
    assert as_dict["id"] == str(reservation_id)
    assert as_dict["status"] == "held"
    assert as_dict["resolved_at"] is None
