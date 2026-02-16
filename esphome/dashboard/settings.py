from __future__ import annotations

import hmac
import os
from pathlib import Path
from typing import Any

from esphome.core import CORE
from esphome.helpers import get_bool_env

from .util.password import password_hash

# Sentinel file name used for CORE.config_path when dashboard initializes.
# This ensures .parent returns the config directory instead of root.
_DASHBOARD_SENTINEL_FILE = "___DASHBOARD_SENTINEL___.yaml"


class DashboardSettings:
    """Settings for the dashboard."""

    __slots__ = (
        "config_dir",
        "password_hash",
        "username",
        "using_password",
        "on_ha_addon",
        "cookie_secret",
        "absolute_config_dir",
        "verbose",
        "remote_build_url",
        "remote_build_token",
        "remote_build_server_enabled",
        "remote_build_server_token",
        "dashboard_enabled",
    )

    def __init__(self) -> None:
        """Initialize the dashboard settings."""
        self.config_dir: Path = None
        self.password_hash: bytes = b""
        self.username: str = ""
        self.using_password: bool = False
        self.on_ha_addon: bool = False
        self.cookie_secret: str | None = None
        self.absolute_config_dir: Path | None = None
        self.verbose: bool = False
        self.remote_build_url: str = ""
        self.remote_build_token: str = ""
        self.remote_build_server_enabled: bool = False
        self.remote_build_server_token: str = ""
        self.dashboard_enabled: bool = True

    def parse_args(self, args: Any) -> None:
        """Parse the arguments."""
        self.on_ha_addon: bool = args.ha_addon
        dashboard_env = os.getenv("ESPHOME_DASHBOARD_ENABLED")
        if dashboard_env is None:
            self.dashboard_enabled = bool(getattr(args, "dashboard_enabled", True))
        else:
            self.dashboard_enabled = dashboard_env.lower() in (
                "1",
                "true",
                "yes",
                "on",
            )
        password = args.password or os.getenv("PASSWORD") or ""
        if not self.on_ha_addon:
            self.username = args.username or os.getenv("USERNAME") or ""
            self.using_password = bool(password)
        if self.using_password:
            self.password_hash = password_hash(password)
        self.config_dir = Path(args.configuration)
        self.absolute_config_dir = self.config_dir.resolve()
        self.verbose = args.verbose
        self.remote_build_url = (
            getattr(args, "remote_build_url", None)
            or os.getenv("ESPHOME_REMOTE_BUILD_URL", "")
        )
        self.remote_build_token = (
            getattr(args, "remote_build_token", None)
            or os.getenv("ESPHOME_REMOTE_BUILD_TOKEN", "")
        )
        self.remote_build_server_enabled = bool(
            getattr(args, "remote_build_server_enabled", False)
            or get_bool_env("ESPHOME_REMOTE_BUILD_SERVER_ENABLED")
        )
        self.remote_build_server_token = (
            getattr(args, "remote_build_server_token", None)
            or os.getenv("ESPHOME_REMOTE_BUILD_SERVER_TOKEN", "")
            or self.remote_build_token
        )
        # Set to a sentinel file so .parent gives us the config directory.
        # Previously this was `os.path.join(self.config_dir, ".")` which worked because
        # os.path.dirname("/config/.") returns "/config", but Path("/config/.").parent
        # normalizes to Path("/config") first, then .parent returns Path("/"), breaking
        # secret resolution. Using a sentinel file ensures .parent gives the correct directory.
        CORE.config_path = self.config_dir / _DASHBOARD_SENTINEL_FILE

    @property
    def relative_url(self) -> str:
        return os.getenv("ESPHOME_DASHBOARD_RELATIVE_URL") or "/"

    @property
    def status_use_mqtt(self) -> bool:
        return get_bool_env("ESPHOME_DASHBOARD_USE_MQTT")

    @property
    def using_ha_addon_auth(self) -> bool:
        if not self.on_ha_addon:
            return False
        return not get_bool_env("DISABLE_HA_AUTHENTICATION")

    @property
    def using_auth(self) -> bool:
        return self.using_password or self.using_ha_addon_auth

    @property
    def streamer_mode(self) -> bool:
        return get_bool_env("ESPHOME_STREAMER_MODE")

    def check_password(self, username: str, password: str) -> bool:
        if not self.using_auth:
            return True
        # Compare in constant running time (to prevent timing attacks)
        username_matches = hmac.compare_digest(
            username.encode("utf-8"), self.username.encode("utf-8")
        )
        password_matches = hmac.compare_digest(
            self.password_hash, password_hash(password)
        )
        return username_matches and password_matches

    def rel_path(self, *args: Any) -> Path:
        """Return a path relative to the ESPHome config folder."""
        joined_path = self.config_dir / Path(*args)
        # Raises ValueError if not relative to ESPHome config folder
        joined_path.resolve().relative_to(self.absolute_config_dir)
        return joined_path
