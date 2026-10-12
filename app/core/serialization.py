"""Serialization utilities for AsyncFlow."""
import json
from typing import Any


def payload_to_json(payload: Any) -> str:
    """Serialize a payload to JSON string."""
    if payload is None:
        return ""
    if isinstance(payload, str):
        return payload
    return json.dumps(payload, default=str)


def payload_from_json(payload_str: str | None) -> Any:
    """Deserialize a JSON payload string."""
    if payload_str is None or payload_str == "":
        return None
    try:
        return json.loads(payload_str)
    except (json.JSONDecodeError, ValueError):
        return None