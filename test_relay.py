#!/usr/bin/env python3
"""
Test suite for WE Relay (test_relay.py). Run with: python -m unittest -v
Tests:
1. Multi-project registration and scoping (`my-project`, `other-project`, `all`).
2. Message send + automatic Markdown copy under handoffs/relay/.
3. Inbox filtering (`unread`, `project`).
4. ACK state transition (`ack_by`).
5. Concurrent long-polling `/v1/wait` instant wake-up via `threading.Condition`.
6. Webhook delivery with `X-Relay-Signature: sha256=<hmac>` verification and status recording.
7. Agent status updates per project.
8. Data home (`RELAY_HOME`) for the server and the CLI url lookup.
9. Cross-origin browser writes are refused.
"""

import hashlib
import hmac
import http.server
import json
import os
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from unittest import mock

import cli
import server


class WebhookReceiverHandler(http.server.BaseHTTPRequestHandler):
    received = []

    def log_message(self, format, *args):
        pass

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length)
        sig = self.headers.get("X-Relay-Signature", "")
        WebhookReceiverHandler.received.append(
            {
                "raw": raw,
                "json": json.loads(raw.decode("utf-8")),
                "signature": sig,
            }
        )
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'{"ok":true}')


class RelayTestSuite(unittest.TestCase):
    def setUp(self):
        os.environ["RELAY_QUIET"] = "1"
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self.tmp_dir.name)
        self.db_path = self.tmp_path / "test_relay.db"
        self.handoffs_dir = self.tmp_path / "handoffs"

        self.relay_srv = server.create_server(
            host="127.0.0.1",
            port=0,
            db_path=self.db_path,
            handoffs_dir=self.handoffs_dir,
        )
        self.port = self.relay_srv.server_address[1]
        self.base_url = f"http://127.0.0.1:{self.port}"
        self.srv_thread = threading.Thread(
            target=self.relay_srv.serve_forever, daemon=True
        )
        self.srv_thread.start()

        WebhookReceiverHandler.received.clear()
        self.wh_srv = http.server.ThreadingHTTPServer(
            ("127.0.0.1", 0), WebhookReceiverHandler
        )
        self.wh_port = self.wh_srv.server_address[1]
        self.wh_url = f"http://127.0.0.1:{self.wh_port}/hook"
        self.wh_thread = threading.Thread(
            target=self.wh_srv.serve_forever, daemon=True
        )
        self.wh_thread.start()

    def tearDown(self):
        self.relay_srv.shutdown()
        self.relay_srv.server_close()
        self.wh_srv.shutdown()
        self.wh_srv.server_close()
        self.tmp_dir.cleanup()

    def _req(self, method: str, path: str, payload=None, timeout: float = 10.0):
        data = json.dumps(payload).encode("utf-8") if payload is not None else None
        headers = {"Accept": "application/json"}
        if data is not None:
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(
            f"{self.base_url}{path}", data=data, method=method, headers=headers
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))

    def test_send_inbox_ack_and_markdown_mirror(self):
        status, res = self._req(
            "POST",
            "/v1/messages",
            {
                "project": "my-project",
                "sender": "claude",
                "recipient": "codex",
                "subject": "Review ready",
                "body": "Please review the changes on branch feature-x.",
            },
        )
        self.assertEqual(status, 201)
        msg = res["message"]
        self.assertEqual(msg["project"], "my-project")
        self.assertEqual(msg["sender"], "claude")
        self.assertEqual(msg["recipient"], "codex")
        self.assertTrue(Path(msg["file_path"]).exists())

        # Check unread inbox for codex on my-project
        _, inbox_res = self._req(
            "GET", "/v1/messages?project=my-project&agent=codex&unread=true"
        )
        self.assertEqual(len(inbox_res["messages"]), 1)
        self.assertEqual(inbox_res["messages"][0]["id"], msg["id"])

        # Check unread inbox on a different project is empty
        _, other_proj_res = self._req(
            "GET", "/v1/messages?project=other-project&agent=codex&unread=true"
        )
        self.assertEqual(len(other_proj_res["messages"]), 0)

        # ACK message as codex
        _, ack_res = self._req(
            "POST", "/v1/ack", {"id": msg["id"], "agent": "codex"}
        )
        self.assertIn("codex", ack_res["message"]["ack_by"])

        # Unread inbox should now be empty
        _, inbox_after = self._req(
            "GET", "/v1/messages?project=my-project&agent=codex&unread=true"
        )
        self.assertEqual(len(inbox_after["messages"]), 0)

    def test_inline_message_mirror_written_under_handoffs_relay(self):
        _, res = self._req(
            "POST",
            "/v1/messages",
            {
                "sender": "gemini",
                "recipient": "human",
                "subject": "Mirror check",
                "body": "This text must appear in the Markdown copy.",
            },
        )
        mirror = Path(res["message"]["file_path"])
        self.assertEqual(mirror.parent.resolve(), (self.handoffs_dir / "relay").resolve())
        self.assertTrue(mirror.name.startswith("relay-"))
        self.assertTrue(mirror.name.endswith("-gemini-to-human.md"))
        text = mirror.read_text(encoding="utf-8")
        self.assertIn("# Mirror check", text)
        self.assertIn("**Project:** `default`", text)
        self.assertIn("This text must appear in the Markdown copy.", text)

    def test_concurrent_long_poll_wait_wakeup(self):
        wait_result = {}

        def waiter():
            st, body = self._req(
                "GET",
                "/v1/wait?project=my-project&agent=codex&since_id=0&timeout=10&auto_ack=true",
                timeout=15.0,
            )
            wait_result["status"] = st
            wait_result["body"] = body

        t = threading.Thread(target=waiter, daemon=True)
        start_ts = time.monotonic()
        t.start()
        time.sleep(0.25)

        # Send broadcast message to 'all' from 'claude'
        self._req(
            "POST",
            "/v1/messages",
            {
                "project": "my-project",
                "sender": "claude",
                "recipient": "all",
                "subject": "Build is green",
                "body": "Merge branch feature-x now.",
            },
        )

        t.join(timeout=5.0)
        elapsed = time.monotonic() - start_ts
        self.assertLess(elapsed, 3.0, "Wait endpoint should wake up immediately")
        self.assertEqual(wait_result.get("status"), 200)
        self.assertFalse(wait_result["body"]["timed_out"])
        self.assertEqual(len(wait_result["body"]["messages"]), 1)
        self.assertIn("codex", wait_result["body"]["messages"][0]["ack_by"])

    def test_webhook_hmac_verification_and_status(self):
        secret = "test-webhook-secret"
        st, wh_res = self._req(
            "POST",
            "/v1/webhooks",
            {
                "project": "my-project",
                "agent": "gemini",
                "url": self.wh_url,
                "secret": secret,
            },
        )
        self.assertEqual(st, 201)
        wh_id = wh_res["webhook"]["id"]

        # Trigger message to gemini
        self._req(
            "POST",
            "/v1/messages",
            {
                "project": "my-project",
                "sender": "human",
                "recipient": "gemini",
                "subject": "Wake up",
                "body": "Test webhook payload",
            },
        )

        # Wait briefly for async webhook thread
        deadline = time.monotonic() + 5.0
        while not WebhookReceiverHandler.received and time.monotonic() < deadline:
            time.sleep(0.05)

        self.assertEqual(len(WebhookReceiverHandler.received), 1)
        item = WebhookReceiverHandler.received[0]
        expected_hmac = hmac.new(
            secret.encode("utf-8"), item["raw"], hashlib.sha256
        ).hexdigest()
        self.assertEqual(item["signature"], f"sha256={expected_hmac}")
        self.assertEqual(item["json"]["recipient"], "gemini")
        self.assertEqual(item["json"]["project"], "my-project")

        # Verify webhook delivery status recorded in DB
        time.sleep(0.1)
        _, wh_list = self._req("GET", "/v1/webhooks")
        matching = [w for w in wh_list["webhooks"] if w["id"] == wh_id][0]
        self.assertEqual(matching["last_status"], "200 OK")
        self.assertTrue(bool(matching["last_Attempt_at"]))

    def test_multi_project_status_board(self):
        self._req(
            "POST",
            "/v1/status",
            {
                "project": "my-project",
                "agent": "codex",
                "state": "frozen",
                "branch": "feature/login",
                "commit_sha": "abc1234",
                "detail": "Frozen, waiting for review",
            },
        )
        self._req(
            "POST",
            "/v1/status",
            {
                "project": "other-project",
                "agent": "codex",
                "state": "working",
                "branch": "main",
                "commit_sha": "d4e5f6a",
                "detail": "Fixing the build",
            },
        )

        _, mp_st = self._req("GET", "/v1/status?project=my-project")
        self.assertEqual(len(mp_st["statuses"]), 1)
        self.assertEqual(mp_st["statuses"][0]["state"], "frozen")

        _, op_st = self._req("GET", "/v1/status?project=other-project")
        self.assertEqual(len(op_st["statuses"]), 1)
        self.assertEqual(op_st["statuses"][0]["state"], "working")

        _, all_st = self._req("GET", "/v1/status?project=all")
        self.assertEqual(len(all_st["statuses"]), 2)

    def test_assets_route_serves_images_under_assets_only(self):
        assets = self.tmp_path / "assets"
        (assets / "backgrounds").mkdir(parents=True)
        (assets / "backgrounds" / "track-day.webp").write_bytes(b"RIFF0000WEBP")
        (assets / "notes.txt").write_text("not an image", encoding="utf-8")

        with mock.patch.object(server, "ASSETS_DIR", assets):
            url = f"{self.base_url}/assets/backgrounds/track-day.webp"
            with urllib.request.urlopen(url, timeout=10.0) as resp:
                self.assertEqual(resp.status, 200)
                self.assertEqual(resp.headers["Content-Type"], "image/webp")
                self.assertEqual(resp.read(), b"RIFF0000WEBP")

            # Outside assets/ (plain or encoded), not an image, a folder, or missing: all 404.
            for path in (
                "/assets/../test_relay.db",
                "/assets/%2e%2e/test_relay.db",
                "/assets/notes.txt",
                "/assets/backgrounds/",
                "/assets/missing.png",
            ):
                with self.assertRaises(urllib.error.HTTPError, msg=path) as ctx:
                    urllib.request.urlopen(f"{self.base_url}{path}", timeout=10.0)
                self.assertEqual(ctx.exception.code, 404, path)
                ctx.exception.close()

    def test_cross_origin_write_is_refused(self):
        body = json.dumps(
            {"sender": "human", "recipient": "claude", "subject": "Hi", "body": "Hi"}
        ).encode("utf-8")

        def post(origin):
            req = urllib.request.Request(
                f"{self.base_url}/v1/messages",
                data=body,
                method="POST",
                headers={"Content-Type": "text/plain", "Origin": origin},
            )
            return urllib.request.urlopen(req, timeout=10.0)

        with self.assertRaises(urllib.error.HTTPError) as ctx:
            post("http://evil.example")
        self.assertEqual(ctx.exception.code, 403)
        ctx.exception.close()
        _, listing = self._req("GET", "/v1/messages")
        self.assertEqual(len(listing["messages"]), 0)

        # The dashboard is same-origin, so its writes still work.
        with post(f"http://127.0.0.1:{self.port}") as resp:
            self.assertEqual(resp.status, 201)


class DataHomeTests(unittest.TestCase):
    def test_server_paths_follow_relay_home(self):
        with tempfile.TemporaryDirectory() as tmp:
            env = {"RELAY_HOME": tmp}
            with mock.patch.dict(os.environ, env):
                os.environ.pop("RELAY_DB_PATH", None)
                os.environ.pop("RELAY_HANDOFFS_DIR", None)
                self.assertEqual(server.default_db_path(), Path(tmp) / "relay.db")
                self.assertEqual(server.default_handoffs_dir(), Path(tmp) / "handoffs")

                os.environ["RELAY_DB_PATH"] = str(Path(tmp) / "other.db")
                os.environ["RELAY_HANDOFFS_DIR"] = str(Path(tmp) / "md")
                self.assertEqual(server.default_db_path(), Path(tmp) / "other.db")
                self.assertEqual(server.default_handoffs_dir(), Path(tmp) / "md")

    def test_default_home_is_dot_we_relay_in_user_home(self):
        with mock.patch.dict(os.environ):
            os.environ.pop("RELAY_HOME", None)
            self.assertEqual(server.relay_home(), Path.home() / ".we-relay")

    def test_cli_finds_server_by_env_then_url_file_then_default(self):
        with tempfile.TemporaryDirectory() as tmp:
            with mock.patch.dict(os.environ, {"RELAY_HOME": tmp}):
                os.environ.pop("RELAY_URL", None)
                self.assertEqual(cli._resolve_default_url(), "http://127.0.0.1:8787")

                (Path(tmp) / ".relay-url").write_text("http://127.0.0.1:9999", encoding="utf-8")
                self.assertEqual(cli._resolve_default_url(), "http://127.0.0.1:9999")

                os.environ["RELAY_URL"] = "http://127.0.0.1:7777"
                self.assertEqual(cli._resolve_default_url(), "http://127.0.0.1:7777")


if __name__ == "__main__":
    unittest.main(verbosity=2)
