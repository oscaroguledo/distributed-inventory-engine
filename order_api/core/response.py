from typing import Any, Generic, TypeVar

from pydantic import BaseModel

T = TypeVar("T")


class APIResponse(BaseModel, Generic[T]):
    success: bool
    message: str
    status: int
    data: T | None = None


def SResponse(data: Any = None, message: str = "OK", status: int = 200) -> APIResponse:
    return APIResponse(success=True, message=message, status=status, data=data)


def EResponse(message: str = "Error", status: int = 400, data: Any = None) -> APIResponse:
    return APIResponse(success=False, message=message, status=status, data=data)
