"""Unit tests for the API listen address (no I/O).

The API has no authentication, so `asyncflow-api` must listen on loopback unless
`API_HOST` says otherwise.
"""
import uvicorn

from app.api import main as api_main
from app.config.settings import Settings


def test_api_host_defaults_to_loopback(monkeypatch):
    monkeypatch.delenv("API_HOST", raising=False)
    assert Settings(_env_file=None).api_host == "127.0.0.1"


def test_api_host_is_configurable_by_environment(monkeypatch):
    monkeypatch.setenv("API_HOST", "0.0.0.0")
    assert Settings(_env_file=None).api_host == "0.0.0.0"


def test_main_listens_on_the_configured_host(monkeypatch):
    calls = {}
    monkeypatch.setattr(uvicorn, "run", lambda _app, **kwargs: calls.update(kwargs))
    monkeypatch.setattr(api_main.settings, "api_host", "127.0.0.1")

    api_main.main()

    assert calls == {"host": "127.0.0.1", "port": 8000}
