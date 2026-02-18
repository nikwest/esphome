"""Remote build server for ESPHome.

A standalone Tornado WebSocket server that accepts compilation requests,
runs esphome compile, streams build logs back, and serves firmware binaries.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import subprocess
import time
from pathlib import Path
from typing import Any

import yaml

import tornado.ioloop
import tornado.process
import tornado.web
import tornado.websocket

from esphome import const

from .const import DASHBOARD_COMMAND

_LOGGER = logging.getLogger(__name__)

# Cleanup build metadata older than this (seconds)
BUILD_TIMEOUT = 3600  # 1 hour

DEFAULT_WORKSPACE = Path("/config/.esphome/remote-build")

# Store active builds: build_id -> BuildInfo
_builds: dict[str, _BuildInfo] = {}


def _sanitize_name(name: str) -> str:
    """Convert config names into safe folder names."""
    return re.sub(r"[^A-Za-z0-9_.-]", "_", name).strip("._") or "device"


def _extract_config_name(yaml_content: str, fallback: str) -> str:
    """Extract esphome.name from YAML content; fall back when missing."""
    try:
        data = yaml.safe_load(yaml_content) or {}
    except yaml.YAMLError:
        return _sanitize_name(fallback)

    name = data.get("esphome", {}).get("name") if isinstance(data, dict) else None
    if isinstance(name, str) and name:
        return _sanitize_name(name)

    return _sanitize_name(fallback)


def _workspace_root() -> Path:
    """Return root workspace directory for persistent remote builds."""
    raw = os.getenv("ESPHOME_REMOTE_BUILD_WORKSPACE", "").strip()
    if raw:
        return Path(raw)
    return DEFAULT_WORKSPACE


class _BuildInfo:
    """Track a build workspace and metadata."""

    __slots__ = ("workspace_dir", "firmware_path", "created_at", "downloaded")

    def __init__(self, workspace_dir: str) -> None:
        self.workspace_dir = workspace_dir
        self.firmware_path: Path | None = None
        self.created_at = time.monotonic()
        self.downloaded = False


def _cleanup_old_builds() -> None:
    """Remove stale build metadata entries."""
    now = time.monotonic()
    expired = [
        bid
        for bid, info in _builds.items()
        if (now - info.created_at) > BUILD_TIMEOUT or info.downloaded
    ]
    for bid in expired:
        _builds.pop(bid, None)
        _LOGGER.debug("Cleaned build metadata %s", bid)


def _check_auth(handler: tornado.web.RequestHandler, token: str) -> bool:
    """Verify bearer token authentication."""
    if not token:
        return True
    auth = handler.request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        return auth[7:] == token
    return False


def _check_ws_auth(
    handler: tornado.websocket.WebSocketHandler, token: str
) -> bool:
    """Verify bearer token for WebSocket connections (via query param or header)."""
    if not token:
        return True
    # Check query parameter first (WebSocket clients can't always set headers)
    q_token = handler.get_argument("token", default=None)
    if q_token == token:
        return True
    # Fall back to Authorization header
    auth = handler.request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        return auth[7:] == token
    return False


class VersionHandler(tornado.web.RequestHandler):
    """GET /version - return server version."""

    def initialize(self, token: str = "") -> None:
        self._token = token

    def get(self) -> None:
        if not _check_auth(self, self._token):
            self.set_status(401)
            self.write({"error": "unauthorized"})
            return
        self.set_header("Content-Type", "application/json")
        self.write({"version": const.__version__})


class DownloadHandler(tornado.web.RequestHandler):
    """GET /download/<build_id>/firmware.bin - serve compiled firmware."""

    def initialize(self, token: str = "") -> None:
        self._token = token

    def get(self, build_id: str) -> None:
        if not _check_auth(self, self._token):
            self.set_status(401)
            self.write({"error": "unauthorized"})
            return

        info = _builds.get(build_id)
        if not info or not info.firmware_path or not info.firmware_path.exists():
            self.set_status(404)
            self.write({"error": "build not found"})
            return

        self.set_header("Content-Type", "application/octet-stream")
        self.set_header(
            "Content-Disposition", f'attachment; filename="firmware.bin"'
        )
        with open(info.firmware_path, "rb") as f:
            self.write(f.read())
        info.downloaded = True
        _LOGGER.info("Firmware downloaded for build %s", build_id)


class CompileWebSocket(tornado.websocket.WebSocketHandler):
    """WebSocket handler for remote compilation requests.

    Protocol:
        Client sends: {"type": "spawn", "yaml": "<content>", "secrets": "<content>"}
        Server sends: {"event": "line", "data": "..."} for each log line
        Server sends: {"event": "done", "firmware_url": "/download/<id>/firmware.bin"}
        Server sends: {"event": "exit", "code": <int>} on failure
    """

    def initialize(self, token: str = "") -> None:
        self._token = token
        self._proc: tornado.process.Subprocess | None = None
        self._is_closed = False
        self._build_id: str | None = None

    def check_origin(self, origin: str) -> bool:
        return True

    def open(self, *args: str, **kwargs: str) -> None:
        if not _check_ws_auth(self, self._token):
            self.write_message({"event": "exit", "code": 1, "error": "unauthorized"})
            self.close()
            return
        self.set_nodelay(True)
        _LOGGER.info("Remote compile WebSocket opened")

    async def on_message(self, message: str) -> None:
        try:
            msg = json.loads(message)
        except json.JSONDecodeError:
            self.write_message({"event": "exit", "code": 1, "error": "invalid JSON"})
            self.close()
            return

        if msg.get("type") != "spawn":
            return

        if self._proc is not None:
            return  # Already spawned

        yaml_content = msg.get("yaml", "")
        secrets_content = msg.get("secrets", "")

        if not yaml_content:
            self.write_message(
                {"event": "exit", "code": 1, "error": "missing yaml content"}
            )
            self.close()
            return

        await self._run_compile(yaml_content, secrets_content)

    async def _run_compile(
        self, yaml_content: str, secrets_content: str
    ) -> None:
        """Set up temp dir, write files, and run esphome compile."""
        import secrets as secrets_mod

        _cleanup_old_builds()

        build_id = secrets_mod.token_hex(16)
        config_name = _extract_config_name(yaml_content, fallback=build_id)
        workspace_dir = _workspace_root() / config_name
        workspace_dir.mkdir(parents=True, exist_ok=True)

        build_info = _BuildInfo(str(workspace_dir))
        _builds[build_id] = build_info
        self._build_id = build_id

        config_path = str(workspace_dir / "config.yaml")
        secrets_path = str(workspace_dir / "secrets.yaml")

        try:
            with open(config_path, "w", encoding="utf-8") as f:
                f.write(yaml_content)
            if secrets_content:
                with open(secrets_path, "w", encoding="utf-8") as f:
                    f.write(secrets_content)
        except OSError as err:
            _LOGGER.error("Failed to write config files: %s", err)
            self.write_message(
                {"event": "exit", "code": 1, "error": f"failed to write files: {err}"}
            )
            self.close()
            return

        command = [*DASHBOARD_COMMAND, "compile", config_path]
        _LOGGER.info(
            "Running remote compile for '%s' in %s: %s",
            config_name,
            workspace_dir,
            " ".join(command),
        )

        # Ensure PlatformIO toolchain binaries are in PATH
        env = os.environ.copy()
        pio_dir = Path.home() / ".platformio" / "packages"
        if pio_dir.exists():
            extra = os.pathsep.join(
                str(p / "bin") for p in pio_dir.iterdir() if (p / "bin").is_dir()
            )
            if extra:
                env["PATH"] = extra + os.pathsep + env.get("PATH", "")
                _LOGGER.debug("Extended PATH with PlatformIO tools: %s", extra)

        try:
            self._proc = tornado.process.Subprocess(
                command,
                stdout=tornado.process.Subprocess.STREAM,
                stderr=subprocess.STDOUT,
                stdin=subprocess.PIPE,
                close_fds=False,
                env=env,
            )
            self._proc.set_exit_callback(
                lambda rc: tornado.ioloop.IOLoop.current().add_callback(
                    self._on_exit, rc
                )
            )
        except Exception as err:
            _LOGGER.error("Failed to start compile process: %s", err)
            self.write_message(
                {"event": "exit", "code": 1, "error": f"failed to start: {err}"}
            )
            self.close()
            return

        tornado.ioloop.IOLoop.current().spawn_callback(self._stream_output)
        tornado.ioloop.IOLoop.current().spawn_callback(self._keepalive)

    async def _keepalive(self) -> None:
        """Send periodic keepalive messages to prevent proxy timeouts."""
        while not self._is_closed:
            await asyncio.sleep(15)
            if self._is_closed:
                break
            try:
                self.write_message({"event": "keepalive"})
            except tornado.websocket.WebSocketClosedError:
                break

    async def _stream_output(self) -> None:
        """Read process stdout and stream to WebSocket."""
        reg = b"[\n\r]"
        while True:
            try:
                data: bytes = await self._proc.stdout.read_until_regex(reg)
            except tornado.iostream.StreamClosedError:
                break
            if self._is_closed:
                break
            text = data.decode("utf-8", "replace")
            try:
                self.write_message({"event": "line", "data": text})
            except tornado.websocket.WebSocketClosedError:
                break

    def _on_exit(self, returncode: int) -> None:
        """Handle process exit."""
        if self._is_closed:
            return

        _LOGGER.info("Remote compile exited with code %s", returncode)

        if returncode == 0 and self._build_id:
            # Find firmware binary
            info = _builds.get(self._build_id)
            if info:
                firmware = self._find_firmware(info.workspace_dir)
                if firmware:
                    info.firmware_path = firmware
                    self.write_message(
                        {
                            "event": "done",
                            "firmware_url": f"/download/{self._build_id}/firmware.bin",
                        }
                    )
                    self.close()
                    return

        self.write_message({"event": "exit", "code": returncode})
        self.close()

    @staticmethod
    def _find_firmware(workspace_dir: str) -> Path | None:
        """Search for compiled firmware binary in the workspace build directory."""
        # ESPHome puts firmware in .esphome/build/<name>/.pioenvs/<name>/firmware.bin
        # or similar paths depending on platform
        build_dir = Path(workspace_dir) / ".esphome"
        if not build_dir.exists():
            return None

        # Search for common firmware filenames
        for pattern in [
            "**/*firmware*.bin",
            "**/*firmware*.elf",
            "**/firmware-factory.bin",
        ]:
            results = list(build_dir.glob(pattern))
            if results:
                # Prefer .bin over .elf, prefer firmware.bin over others
                bin_files = [r for r in results if r.suffix == ".bin"]
                if bin_files:
                    return bin_files[0]
                return results[0]
        return None

    def on_close(self) -> None:
        self._is_closed = True
        if self._proc is not None and self._proc.returncode is None:
            _LOGGER.debug("Terminating remote compile process")
            self._proc.proc.terminate()


def make_remote_build_app(token: str = "") -> tornado.web.Application:
    """Create the remote build server Tornado application."""
    handler_kwargs = {"token": token}
    return tornado.web.Application(
        [
            (r"/version", VersionHandler, handler_kwargs),
            (r"/compile", CompileWebSocket, handler_kwargs),
            (r"/download/([a-f0-9]+)/firmware\.bin", DownloadHandler, handler_kwargs),
        ]
    )


def make_app(token: str = "") -> tornado.web.Application:
    """Backward-compatible alias for make_remote_build_app."""
    return make_remote_build_app(token)


def start_server(port: int = 6053, token: str = "") -> None:
    """Start the remote build server.

    Args:
        port: TCP port to listen on.
        token: Bearer token for authentication. Empty string disables auth.
    """
    app = make_remote_build_app(token)
    app.listen(port)
    _LOGGER.info(
        "ESPHome remote build server v%s listening on port %d (auth=%s, workspace=%s)",
        const.__version__,
        port,
        "enabled" if token else "disabled",
        _workspace_root(),
    )
    tornado.ioloop.IOLoop.current().start()
