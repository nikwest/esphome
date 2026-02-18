"""Tests for remote build server and dashboard remote build integration."""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
import pytest_asyncio
from tornado.httpclient import AsyncHTTPClient
from tornado.httpserver import HTTPServer
from tornado.testing import bind_unused_port
from tornado.websocket import websocket_connect

from esphome import const
from esphome.dashboard import remote_build_server, web_server
from esphome.dashboard.web_server import (
    EsphomeCommandWebSocket,
    EsphomeCompileHandler,
    EsphomeRunHandler,
)


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
async def remote_build_server_fixture():
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
    async def test_version_returns_esphome_version(self, remote_build_server_fixture):
        """GET /version should return the current ESPHome version."""
        url = f"{remote_build_server_fixture['url']}/version"
        resp = await remote_build_server_fixture["client"].fetch(
            url,
            headers={"Authorization": "Bearer test-token-123"},
        )
        assert resp.code == 200
        data = json.loads(resp.body)
        assert data["version"] == const.__version__

    @pytest.mark.asyncio
    async def test_version_requires_auth(self, remote_build_server_fixture):
        """GET /version without token should return 401."""
        from tornado.httpclient import HTTPClientError

        url = f"{remote_build_server_fixture['url']}/version"
        with pytest.raises(HTTPClientError) as exc_info:
            await remote_build_server_fixture["client"].fetch(url)
        assert exc_info.value.code == 401

    @pytest.mark.asyncio
    async def test_version_rejects_bad_token(self, remote_build_server_fixture):
        """GET /version with wrong token should return 401."""
        from tornado.httpclient import HTTPClientError

        url = f"{remote_build_server_fixture['url']}/version"
        with pytest.raises(HTTPClientError) as exc_info:
            await remote_build_server_fixture["client"].fetch(
                url,
                headers={"Authorization": "Bearer wrong-token"},
            )
        assert exc_info.value.code == 401


class TestRemoteBuildServerCompile:
    """Test the /compile WebSocket endpoint."""

    @pytest.mark.asyncio
    async def test_compile_websocket_connects(self, remote_build_server_fixture):
        """Should be able to connect to /compile with valid token."""
        port = remote_build_server_fixture["port"]
        ws_url = f"ws://127.0.0.1:{port}/compile?token=test-token-123"
        ws = await websocket_connect(ws_url)
        assert ws is not None
        ws.close()

    @pytest.mark.asyncio
    async def test_compile_rejects_missing_yaml(self, remote_build_server_fixture):
        """Spawn without YAML should fail fast with an exit event."""
        port = remote_build_server_fixture["port"]
        ws_url = f"ws://127.0.0.1:{port}/compile?token=test-token-123"
        ws = await websocket_connect(ws_url)

        ws.write_message(json.dumps({"type": "spawn", "secrets": ""}))

        msg = await asyncio.wait_for(ws.read_message(), timeout=10)
        assert msg is not None
        data = json.loads(msg)
        assert data.get("event") == "exit"
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
            remote_build_workspace=None,
        )

        with patch.dict(
            "os.environ",
            {
                "ESPHOME_REMOTE_BUILD_URL": "http://10.0.1.2:6053",
                "ESPHOME_REMOTE_BUILD_TOKEN": "my-secret-token",
                "ESPHOME_REMOTE_BUILD_WORKSPACE": "/config/.esphome/remote-build",
            },
        ):
            settings.parse_args(args)

        assert settings.remote_build_url == "http://10.0.1.2:6053"
        assert settings.remote_build_token == "my-secret-token"
        assert settings.remote_build_workspace == "/config/.esphome/remote-build"

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
            remote_build_workspace="/config/custom-remote-build",
        )

        settings.parse_args(args)

        assert settings.remote_build_url == "http://cli-server:6053"
        assert settings.remote_build_token == "cli-token"
        assert settings.remote_build_workspace == "/config/custom-remote-build"

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
            remote_build_workspace=None,
        )

        with patch.dict("os.environ", {}, clear=True):
            settings.parse_args(args)

        assert settings.remote_build_url == ""
        assert settings.remote_build_token == ""
        assert settings.remote_build_workspace == ""



# ---------------------------------------------------------------------------
# 4. Regression tests for remote compile/download protocol and firmware cache
# ---------------------------------------------------------------------------


class TestRemoteBuildRegression:
    """Regression tests for remote build integration edge cases."""

    @pytest.mark.asyncio
    async def test_download_firmware_with_storage_firmware_path(self, tmp_path):
        """Should use storage firmware_bin_path without requiring pioenvs_dir."""
        handler = MagicMock()
        body = b"\x01\x02firmware"

        with (
            patch.object(
                web_server.tornado.httpclient,
                "AsyncHTTPClient",
                return_value=SimpleNamespace(
                    fetch=AsyncMock(return_value=SimpleNamespace(body=body))
                ),
            ),
            patch.object(
                web_server.StorageJSON,
                "load",
                return_value=SimpleNamespace(
                    firmware_bin_path=tmp_path / "cache" / "firmware.bin"
                ),
            ),
        ):
            result = await web_server._download_firmware(
                handler,
                "http://remote",
                "/download/abc/firmware.bin",
                "",
                "technik.yaml",
            )

        assert result == tmp_path / "cache" / "firmware.bin"
        assert result.read_bytes() == body

    @pytest.mark.asyncio
    async def test_download_firmware_without_storage_uses_build_dir(self, tmp_path):
        """Should cache firmware in .esphome/build/<name>/.pioenvs/<name>/firmware.bin."""
        handler = MagicMock()
        body = b"\x03\x04firmware"

        with (
            patch.object(
                web_server.tornado.httpclient,
                "AsyncHTTPClient",
                return_value=SimpleNamespace(
                    fetch=AsyncMock(return_value=SimpleNamespace(body=body))
                ),
            ),
            patch.object(web_server.StorageJSON, "load", return_value=None),
            patch.object(web_server, "_resolve_config_name", return_value="tech"),
            patch.object(web_server.settings, "config_dir", tmp_path),
        ):
            result = await web_server._download_firmware(
                handler,
                "http://remote",
                "/download/abc/firmware.bin",
                "",
                "technik.yaml",
            )

        expected = tmp_path / ".esphome" / "build" / "tech" / ".pioenvs" / "tech" / "firmware.bin"
        assert result == expected
        assert result.read_bytes() == body

    @pytest.mark.asyncio
    async def test_remote_compile_sends_yaml_and_secrets_payload(self, tmp_path):
        """Remote compile protocol should send YAML/secrets content, not filename."""

        class FakeConn:
            def __init__(self):
                self.sent_messages = []
                self._messages = iter(
                    [
                        json.dumps({"event": "line", "data": "ok\n"}),
                        json.dumps(
                            {
                                "event": "done",
                                "firmware_url": "/download/abc123/firmware.bin",
                            }
                        ),
                    ]
                )

            def write_message(self, message):
                self.sent_messages.append(message)

            async def read_message(self):
                return next(self._messages, None)

        cfg = tmp_path / "technik.yaml"
        cfg.write_text(
            "esphome:\n  name: tech\napi:\n  password: !secret api_key\n",
            encoding="utf-8",
        )
        (tmp_path / "secrets.yaml").write_text(
            "api_key: test\nunused_secret: do-not-send\n", encoding="utf-8"
        )

        fake_conn = FakeConn()
        handler = MagicMock()

        with (
            patch.object(web_server.settings, "remote_build_url", "http://remote"),
            patch.object(web_server.settings, "remote_build_token", "token"),
            patch.object(type(web_server.settings), "rel_path", return_value=cfg),
            patch.object(
                web_server.tornado.httpclient,
                "AsyncHTTPClient",
                return_value=SimpleNamespace(
                    fetch=AsyncMock(
                        return_value=SimpleNamespace(
                            body=json.dumps({"version": const.__version__}).encode()
                        )
                    )
                ),
            ),
            patch.object(
                web_server.tornado.websocket,
                "websocket_connect",
                AsyncMock(return_value=fake_conn),
            ),
        ):
            firmware_path = await web_server._remote_compile(handler, "technik.yaml")

        assert firmware_path == "/download/abc123/firmware.bin"
        assert fake_conn.sent_messages, "No websocket message sent"
        payload = json.loads(fake_conn.sent_messages[0])
        assert payload["type"] == "spawn"
        assert "yaml" in payload and "esphome:" in payload["yaml"]
        assert "secrets" in payload and "api_key: test" in payload["secrets"]
        assert "unused_secret" not in payload["secrets"]
        assert "configuration" not in payload

    @pytest.mark.asyncio
    async def test_download_firmware_updates_storage_version(self, tmp_path):
        """Remote firmware download should update local storage version and firmware path."""
        handler = MagicMock()
        body = b"\xAA\xBBfirmware"
        firmware_file = tmp_path / "cache" / "firmware.bin"
        storage = SimpleNamespace(
            firmware_bin_path=firmware_file,
            esphome_version="old-version",
            save=MagicMock(),
        )

        with (
            patch.object(
                web_server.tornado.httpclient,
                "AsyncHTTPClient",
                return_value=SimpleNamespace(
                    fetch=AsyncMock(return_value=SimpleNamespace(body=body))
                ),
            ),
            patch.object(web_server.StorageJSON, "load", return_value=storage),
        ):
            result = await web_server._download_firmware(
                handler,
                "http://remote",
                "/download/abc/firmware.bin",
                "",
                "technik.yaml",
            )

        assert result == firmware_file
        assert storage.esphome_version == const.__version__
        assert storage.firmware_bin_path == firmware_file
        storage.save.assert_called_once()


class TestRemoteSecretFiltering:
    """Tests for reducing secrets payload to only referenced keys."""

    def test_filter_remote_secrets_only_used_keys(self):
        yaml_content = "api:\n  password: !secret api_key\n"
        secrets_content = "api_key: test\nunused: nope\n"

        filtered = web_server._filter_remote_secrets(yaml_content, secrets_content)

        assert "api_key: test" in filtered
        assert "unused" not in filtered

    def test_filter_remote_secrets_no_secret_refs(self):
        yaml_content = "esphome:\n  name: tech\n"
        secrets_content = "api_key: test\n"

        filtered = web_server._filter_remote_secrets(yaml_content, secrets_content)

        assert filtered == ""


class TestRemoteBuildWorkspaceWrites:
    """Ensure remote build workspace files are written only when changed."""

    def test_write_text_if_changed_skips_same_content(self, tmp_path):
        target = tmp_path / "config.yaml"
        target.write_text("same\n", encoding="utf-8")

        changed = remote_build_server._write_text_if_changed(target, "same\n")

        assert changed is False
        assert target.read_text(encoding="utf-8") == "same\n"

    def test_write_text_if_changed_updates_different_content(self, tmp_path):
        target = tmp_path / "config.yaml"
        target.write_text("old\n", encoding="utf-8")

        changed = remote_build_server._write_text_if_changed(target, "new\n")

        assert changed is True
        assert target.read_text(encoding="utf-8") == "new\n"
