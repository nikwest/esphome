"""Tests for remote build server and dashboard remote build integration."""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
import pytest_asyncio
from tornado.httpclient import AsyncHTTPClient
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop
from tornado.testing import bind_unused_port
from tornado.websocket import websocket_connect

from esphome import const
from esphome.dashboard import web_server
from esphome.dashboard.core import DASHBOARD
from esphome.dashboard.web_server import (
    EsphomeCommandWebSocket,
    EsphomeCompileHandler,
    EsphomeRunHandler,
)

from .common import get_fixture_path


# ---------------------------------------------------------------------------
# 1. websocket_class decorator: handler isolation tests
# ---------------------------------------------------------------------------


class TestWebsocketClassDecorator:
    """Verify that @websocket_class creates isolated _message_handlers per class."""

    def test_base_class_has_spawn_handler(self):
        """EsphomeCommandWebSocket should have 'spawn' in its handlers."""
        assert "spawn" in EsphomeCommandWebSocket._message_handlers

    def test_base_class_has_stdin_handler(self):
        """EsphomeCommandWebSocket should have 'stdin' in its handlers."""
        assert "stdin" in EsphomeCommandWebSocket._message_handlers

    def test_compile_handler_has_own_spawn(self):
        """EsphomeCompileHandler should override 'spawn' with its own handler."""
        base_spawn = EsphomeCommandWebSocket._message_handlers["spawn"]
        compile_spawn = EsphomeCompileHandler._message_handlers["spawn"]
        assert compile_spawn is not base_spawn

    def test_run_handler_has_own_spawn(self):
        """EsphomeRunHandler should override 'spawn' with its own handler."""
        base_spawn = EsphomeCommandWebSocket._message_handlers["spawn"]
        run_spawn = EsphomeRunHandler._message_handlers["spawn"]
        assert run_spawn is not base_spawn

    def test_compile_and_run_have_different_spawn(self):
        """CompileHandler and RunHandler should have different spawn handlers."""
        compile_spawn = EsphomeCompileHandler._message_handlers["spawn"]
        run_spawn = EsphomeRunHandler._message_handlers["spawn"]
        assert compile_spawn is not run_spawn

    def test_handlers_are_separate_dicts(self):
        """Each decorated class should have its own _message_handlers dict."""
        assert (
            EsphomeCommandWebSocket._message_handlers
            is not EsphomeCompileHandler._message_handlers
        )
        assert (
            EsphomeCommandWebSocket._message_handlers
            is not EsphomeRunHandler._message_handlers
        )
        assert (
            EsphomeCompileHandler._message_handlers
            is not EsphomeRunHandler._message_handlers
        )

    def test_subclass_inherits_stdin(self):
        """CompileHandler and RunHandler should inherit 'stdin' from base."""
        base_stdin = EsphomeCommandWebSocket._message_handlers["stdin"]
        assert EsphomeCompileHandler._message_handlers["stdin"] is base_stdin
        assert EsphomeRunHandler._message_handlers["stdin"] is base_stdin


# ---------------------------------------------------------------------------
# 2. Remote build server tests
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture()
async def remote_build_server():
    """Start a remote build server on a random port for testing."""
    from esphome.dashboard.remote_build_server import make_remote_build_app

    sock, port = bind_unused_port()
    app = make_remote_build_app(token="test-token-123")
    http_server = HTTPServer(app)
    http_server.add_sockets([sock])

    client = AsyncHTTPClient()
    yield {"port": port, "client": client, "url": f"http://127.0.0.1:{port}"}

    http_server.stop()
    sock.close()
    client.close()


class TestRemoteBuildServerVersion:
    """Test the /version endpoint of the remote build server."""

    @pytest.mark.asyncio
    async def test_version_returns_esphome_version(self, remote_build_server):
        """GET /version should return the current ESPHome version."""
        url = f"{remote_build_server['url']}/version"
        resp = await remote_build_server["client"].fetch(
            url,
            headers={"Authorization": "Bearer test-token-123"},
        )
        assert resp.code == 200
        data = json.loads(resp.body)
        assert data["version"] == const.__version__

    @pytest.mark.asyncio
    async def test_version_requires_auth(self, remote_build_server):
        """GET /version without token should return 401."""
        from tornado.httpclient import HTTPClientError

        url = f"{remote_build_server['url']}/version"
        with pytest.raises(HTTPClientError) as exc_info:
            await remote_build_server["client"].fetch(url)
        assert exc_info.value.code == 401

    @pytest.mark.asyncio
    async def test_version_rejects_bad_token(self, remote_build_server):
        """GET /version with wrong token should return 401."""
        from tornado.httpclient import HTTPClientError

        url = f"{remote_build_server['url']}/version"
        with pytest.raises(HTTPClientError) as exc_info:
            await remote_build_server["client"].fetch(
                url,
                headers={"Authorization": "Bearer wrong-token"},
            )
        assert exc_info.value.code == 401


class TestRemoteBuildServerCompile:
    """Test the /compile WebSocket endpoint."""

    @pytest.mark.asyncio
    async def test_compile_websocket_connects(self, remote_build_server):
        """Should be able to connect to /compile with valid token."""
        port = remote_build_server["port"]
        ws_url = f"ws://127.0.0.1:{port}/compile?token=test-token-123"
        ws = await websocket_connect(ws_url)
        assert ws is not None
        ws.close()

    @pytest.mark.asyncio
    async def test_compile_sends_spawn_with_yaml(self, remote_build_server):
        """Sending a spawn message with YAML should start compilation."""
        port = remote_build_server["port"]
        ws_url = f"ws://127.0.0.1:{port}/compile?token=test-token-123"
        ws = await websocket_connect(ws_url)

        # Send a minimal (but invalid) YAML to trigger a quick failure
        ws.write_message(
            json.dumps(
                {
                    "type": "spawn",
                    "yaml": "this is not valid esphome yaml",
                    "secrets": "",
                }
            )
        )

        # Should receive at least one line event and an exit event
        events = []
        while True:
            msg = await asyncio.wait_for(ws.read_message(), timeout=30)
            if msg is None:
                break
            data = json.loads(msg)
            events.append(data)
            if data.get("event") in ("exit", "done"):
                break

        assert len(events) > 0
        # Last event should be exit (compilation should fail on invalid YAML)
        last_event = events[-1]
        assert last_event["event"] == "exit"
        ws.close()


# ---------------------------------------------------------------------------
# 3. Dashboard integration tests (mocked remote server)
# ---------------------------------------------------------------------------


class TestDashboardRemoteBuildSettings:
    """Test that remote build settings are properly parsed."""

    def test_settings_from_env_vars(self):
        """Settings should read from environment variables."""
        from esphome.dashboard.settings import DashboardSettings

        settings = DashboardSettings()
        args = Mock(
            ha_addon=False,
            configuration="/config",
            password="",
            username="",
            verbose=False,
            remote_build_url=None,
            remote_build_token=None,
        )

        with patch.dict(
            "os.environ",
            {
                "ESPHOME_REMOTE_BUILD_URL": "http://10.0.1.2:6053",
                "ESPHOME_REMOTE_BUILD_TOKEN": "my-secret-token",
            },
        ):
            settings.parse_args(args)

        assert settings.remote_build_url == "http://10.0.1.2:6053"
        assert settings.remote_build_token == "my-secret-token"

    def test_settings_from_cli_args(self):
        """Settings should prefer CLI args over env vars."""
        from esphome.dashboard.settings import DashboardSettings

        settings = DashboardSettings()
        args = Mock(
            ha_addon=False,
            configuration="/config",
            password="",
            username="",
            verbose=False,
            remote_build_url="http://cli-server:6053",
            remote_build_token="cli-token",
        )

        settings.parse_args(args)

        assert settings.remote_build_url == "http://cli-server:6053"
        assert settings.remote_build_token == "cli-token"

    def test_settings_default_empty(self):
        """Without config, remote build settings should be empty strings."""
        from esphome.dashboard.settings import DashboardSettings

        settings = DashboardSettings()
        args = Mock(
            ha_addon=False,
            configuration="/config",
            password="",
            username="",
            verbose=False,
            remote_build_url=None,
            remote_build_token=None,
        )

        with patch.dict("os.environ", {}, clear=True):
            settings.parse_args(args)

        assert settings.remote_build_url == ""
        assert settings.remote_build_token == ""
