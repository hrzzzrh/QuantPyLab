"""Pytest-wide isolation for project file logs."""

from os import environ

environ.setdefault("QUANTPYLAB_DISABLE_FILE_LOGGING", "1")
