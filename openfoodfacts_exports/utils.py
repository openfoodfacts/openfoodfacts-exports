import logging
import os

import sentry_sdk
import toml
from sentry_sdk.integrations import Integration
from sentry_sdk.integrations.logging import LoggingIntegration

from openfoodfacts_exports import settings

# Sentry for error reporting
_sentry_dsn = os.environ.get("SENTRY_DSN")
_environment = os.environ.get("ENVIRONMENT", "dev")


def init_sentry(integrations: list[Integration] | None = None):
    if _sentry_dsn:
        integrations = integrations or []
        integrations.append(
            LoggingIntegration(
                level=logging.INFO,  # Capture info and above as breadcrumbs
                event_level=logging.WARNING,  # Send warning and errors as events
            )
        )
        sentry_sdk.init(
            _sentry_dsn,
            environment=_environment,
            integrations=integrations,
            release=get_package_version(),
        )
    elif _environment == "prod":
        raise ValueError("No SENTRY_DSN specified for production openfoodfacts-exports")


def get_package_version() -> str:
    """Return Robotoff version from pyproject.toml file."""
    return toml.load(str(settings.PROJECT_DIR / "pyproject.toml"))["tool"]["poetry"][
        "version"
    ]
