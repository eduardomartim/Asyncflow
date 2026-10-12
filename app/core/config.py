"""Configuration module for AsyncFlow.

Re-exports the single Settings class/instance from app.config.settings.
Previously this re-instantiated `Settings()` here too, so code importing
from app.core.config vs app.config.settings silently got two DIFFERENT
singleton objects (each parsing .env/env vars independently) -- harmless
while both happen to read the same environment, but wasteful and a trap
for the next change that mutates one at runtime. There is exactly one
Settings instance now, constructed in app.config.settings.
"""

from app.config.settings import Settings, settings

__all__ = ["Settings", "settings"]
