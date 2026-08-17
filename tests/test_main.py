import runpy
import sys
from unittest.mock import patch

from fastapi import FastAPI

from order_api.main import app


def test_app_has_expected_title():
    assert isinstance(app, FastAPI)
    assert app.title == "Order API"


def test_health_router_is_registered():
    paths = {route.path for route in app.routes if hasattr(route, "path")}
    assert "/health" in paths


def test_main_block_starts_uvicorn_with_expected_args():
    sys.modules.pop("order_api.main", None)
    with patch("uvicorn.run") as mock_run:
        runpy.run_module("order_api.main", run_name="__main__")

    mock_run.assert_called_once_with(
        "order_api.main:app", host="0.0.0.0", port=8000, reload=True
    )
