import pytest
from pydantic import ValidationError

from order_api.core.response import APIResponse, EResponse, SResponse


def test_sresponse_defaults():
    response = SResponse()
    assert response.success is True
    assert response.message == "OK"
    assert response.status == 200
    assert response.data is None


def test_sresponse_with_custom_values():
    response = SResponse(data={"id": 1}, message="Created", status=201)
    assert response.success is True
    assert response.message == "Created"
    assert response.status == 201
    assert response.data == {"id": 1}


def test_eresponse_defaults():
    response = EResponse()
    assert response.success is False
    assert response.message == "Error"
    assert response.status == 400
    assert response.data is None


def test_eresponse_with_custom_values():
    response = EResponse(message="Not found", status=404, data={"field": "sku"})
    assert response.success is False
    assert response.message == "Not found"
    assert response.status == 404
    assert response.data == {"field": "sku"}


def test_api_response_requires_core_fields():
    with pytest.raises(ValidationError):
        APIResponse()
