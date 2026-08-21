import runpy
import sys
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from order_api.core.config import get_settings
from order_api.main import app


def test_app_has_expected_title():
    assert isinstance(app, FastAPI)
    assert app.title == "Order API"


def test_health_router_is_registered():
    paths = {route.path for route in app.routes if hasattr(route, "path")}
    assert "/health" in paths


@pytest.mark.asyncio
async def test_metrics_endpoint_exposes_prometheus_format():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/metrics")

    assert response.status_code == 200
    assert "text/plain" in response.headers["content-type"]
    assert "order_api_requests_total" in response.text


def test_main_block_starts_uvicorn_with_port_from_settings():
    expected_port = get_settings().order_api_port

    sys.modules.pop("order_api.main", None)
    with patch("uvicorn.run") as mock_run:
        runpy.run_module("order_api.main", run_name="__main__")

    mock_run.assert_called_once_with(
        "order_api.main:app", host="0.0.0.0", port=expected_port, reload=True
    )
