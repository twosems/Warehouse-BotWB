# utils/audit.py
from contextvars import ContextVar
from typing import Optional

_current_user_id: ContextVar[Optional[int]] = ContextVar("_current_user_id", default=None)

def set_current_user(user_id: Optional[int]) -> None:
    _current_user_id.set(user_id)

def get_current_user() -> Optional[int]:
    return _current_user_id.get()
