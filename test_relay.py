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
10. API agents against a fake OpenAI-compatible server: round trip, retries, key redaction,
    the loop guard (two agents that would ping-pong forever), the config and test endpoints.
11. `listen` prints each message once, keeps going, and acknowledges with --ack.
"""

import contextlib
import hashlib
import hmac
import http.server
import io
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path
from unittest import mock

import agent_runner
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


class FakeModelHandler(http.server.BaseHTTPRequestHandler):
    """A tiny OpenAI-compatible chat server.
    mode "ok" answers; "deny" answers 401 and echoes the Authorization header back (the worst case
    for a key leak); "flaky" answers 503 first, then ok."""

    requests = []
    mode = "ok"

    def log_message(self, format, *args):
        pass

    def do_POST(self):
        raw = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        body = json.loads(raw.decode("utf-8"))
        auth = self.headers.get("Authorization", "")
        FakeModelHandler.requests.append({"path": self.path, "auth": auth, "body": body})
        if FakeModelHandler.mode == "deny":
            code, payload = 401, {"error": {"message": f"Invalid API key. You sent: {auth}"}}
        elif FakeModelHandler.mode == "flaky" and len(FakeModelHandler.requests) == 1:
            code, payload = 503, {"error": {"message": "busy"}}
        else:
            first_line = body["messages"][-1]["content"].splitlines()[0]
            content = f"<think>hidden reasoning</think>Fake answer to: {first_line}"
            code, payload = 200, {"choices": [{"message": {"role": "assistant", "content": content}}]}
        out = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(out)))
        self.end_headers()
        self.wfile.write(out)


class ApiAgentTests(unittest.TestCase):
    KEY_ENV = "WE_RELAY_TEST_KEY"

    def setUp(self):
        os.environ["RELAY_QUIET"] = "1"
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self.tmp_dir.name)
        self.handoffs_dir = self.tmp_path / "handoffs"
        self.relay_srv = server.create_server(
            host="127.0.0.1", port=0, db_path=self.tmp_path / "relay.db", handoffs_dir=self.handoffs_dir
        )
        self.base_url = f"http://127.0.0.1:{self.relay_srv.server_address[1]}"
        threading.Thread(target=self.relay_srv.serve_forever, daemon=True).start()

        FakeModelHandler.requests = []
        FakeModelHandler.mode = "ok"
        self.model_srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), FakeModelHandler)
        self.model_url = f"http://127.0.0.1:{self.model_srv.server_address[1]}/v1"
        threading.Thread(target=self.model_srv.serve_forever, daemon=True).start()

        # Made at run time, so no key-like literal sits in the source.
        self.secret = "sk-" + uuid.uuid4().hex + uuid.uuid4().hex
        self.agents_path = self.tmp_path / "agents.json"
        self.env = mock.patch.dict(os.environ, {self.KEY_ENV: self.secret, "RELAY_AGENTS_FILE": str(self.agents_path)})
        self.env.start()
        self.stop = threading.Event()
        self.threads = []

    def tearDown(self):
        self.stop_agents()
        self.env.stop()
        self.relay_srv.shutdown()
        self.relay_srv.server_close()
        self.model_srv.shutdown()
        self.model_srv.server_close()
        self.tmp_dir.cleanup()

    def agent(self, name, **extra):
        raw = {"name": name, "provider": "Fake", "base_url": self.model_url, "model": "fake-model",
               "api_key_env": self.KEY_ENV, "projects": ["p"], "cooldown_seconds": 0}
        return agent_runner.normalize_agent({**raw, **extra})

    def start_agent(self, agent, humans=("human",)):
        worker = agent_runner.AgentWorker(
            agent, "p", self.base_url, list(humans), self.stop, key=self.secret, wait_timeout=0.5
        )
        t = threading.Thread(target=worker.run, daemon=True)
        t.start()
        self.threads.append(t)
        return worker

    def stop_agents(self):
        self.stop.set()
        for t in self.threads:
            t.join(timeout=5)
        self.threads = []

    def req(self, method, path, payload=None):
        data = json.dumps(payload).encode("utf-8") if payload is not None else None
        r = urllib.request.Request(f"{self.base_url}{path}", data=data, method=method,
                                   headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(r, timeout=40) as resp:
            raw = resp.read().decode("utf-8")
        return raw, json.loads(raw)

    def send(self, sender, recipient, subject, body="Hello there."):
        return self.req("POST", "/v1/messages", {"project": "p", "sender": sender, "recipient": recipient,
                                                 "subject": subject, "body": body})[1]["message"]

    def messages(self):
        return self.req("GET", "/v1/messages?project=p&limit=1000")[1]["messages"]

    def wait_for(self, check, timeout=15.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            result = check()
            if result:
                return result
            time.sleep(0.05)
        self.fail("condition not met in time")

    def settle(self, quiet=1.5, timeout=20.0):
        """Wait until no new message has arrived for `quiet` seconds; return the messages."""
        deadline = time.monotonic() + timeout
        last, since = -1, time.monotonic()
        while time.monotonic() < deadline:
            count = len(self.messages())
            if count != last:
                last, since = count, time.monotonic()
            elif time.monotonic() - since >= quiet:
                return self.messages()
            time.sleep(0.1)
        self.fail("messages never stopped arriving")

    # ---- Pure rules ----

    def test_redact_removes_keys_and_key_shapes(self):
        text = f"HTTP 401: bad key {self.secret}. Header: Authorization: Bearer {self.secret}"
        out = agent_runner.redact(text, [self.secret])
        self.assertNotIn(self.secret, out)
        self.assertIn("[redacted]", out)
        # Keys it was not told about are caught by their shape.
        for other in ("sk-" + uuid.uuid4().hex, "AIza" + uuid.uuid4().hex, "Bearer " + uuid.uuid4().hex):
            self.assertNotIn(other.split()[-1], agent_runner.redact(f"echo: {other}"))
        self.assertEqual(agent_runner.redact("risk-assessment is fine"), "risk-assessment is fine")

    def test_should_reply_rules(self):
        humans = ["human"]
        rule = lambda sender, to, turns=0: agent_runner.should_reply(
            {"sender": sender, "recipient": to}, "deepseek", humans, turns, 3)[0]
        self.assertTrue(rule("codex", "deepseek"))      # addressed by name
        self.assertTrue(rule("human", "all"))           # a person's broadcast
        self.assertFalse(rule("qwen", "all"))           # another agent's broadcast: never
        self.assertFalse(rule("deepseek", "all"))       # its own message
        self.assertFalse(rule("human", "codex"))        # for someone else
        self.assertFalse(rule("codex", "deepseek", 3))  # turn cap reached
        self.assertEqual(agent_runner.thread_key("Re: RE:  Plan  B"), agent_runner.thread_key("plan b"))

    def test_inline_key_in_agents_file_is_refused(self):
        with self.assertRaises(ValueError) as ctx:
            agent_runner.normalize_agent({"name": "x", "base_url": "http://h/v1", "model": "m", "api_key": self.secret})
        self.assertIn("api_key_env", str(ctx.exception))
        self.assertNotIn(self.secret, str(ctx.exception))

    # ---- The model call ----

    def test_chat_retries_with_back_off_and_does_not_retry_bad_keys(self):
        pauses = []
        FakeModelHandler.mode = "flaky"
        text = agent_runner.chat(self.agent("a"), [{"role": "user", "content": "hi"}], self.secret, sleep=pauses.append)
        self.assertEqual(agent_runner.visible(text), "Fake answer to: hi")
        self.assertEqual(pauses, [1.0])
        self.assertEqual(len(FakeModelHandler.requests), 2)

        FakeModelHandler.requests, FakeModelHandler.mode, pauses = [], "deny", []
        with self.assertRaises(agent_runner.ModelError) as ctx:
            agent_runner.chat(self.agent("a"), [{"role": "user", "content": "hi"}], self.secret, sleep=pauses.append)
        self.assertIn("HTTP 401", str(ctx.exception))
        self.assertNotIn(self.secret, str(ctx.exception))
        self.assertEqual((len(FakeModelHandler.requests), pauses), (1, []))

        closed = self.agent("a", base_url="http://127.0.0.1:9/v1")  # nothing listens on port 9
        with self.assertRaises(agent_runner.ModelError) as ctx:
            agent_runner.chat(closed, [{"role": "user", "content": "hi"}], "", timeout=2, retries=1, sleep=pauses.append)
        self.assertIn("Could not reach", str(ctx.exception))
        self.assertEqual(pauses, [1.0])

    # ---- Round trip and safety ----

    def test_round_trip_with_fake_model(self):
        with contextlib.redirect_stdout(io.StringIO()):
            self.start_agent(self.agent("fake-bot"))
            asked = self.send("human", "fake-bot", "Hello")
            reply = self.wait_for(lambda: [m for m in self.messages() if m["sender"] == "fake-bot"])[0]
            self.wait_for(lambda: "fake-bot" in [m for m in self.messages() if m["id"] == asked["id"]][0]["ack_by"])
            self.send("human", "fake-bot", "Re: Hello", "And then?")
            self.wait_for(lambda: len([m for m in self.messages() if m["sender"] == "fake-bot"]) == 2)

        self.assertEqual((reply["recipient"], reply["subject"]), ("human", "Re: Hello"))
        self.assertTrue(reply["body"].startswith("Fake answer to: From human to fake-bot"))
        self.assertNotIn("think", reply["body"])  # reasoning tags are not posted...
        second_turn = FakeModelHandler.requests[1]["body"]["messages"]
        self.assertEqual([m["role"] for m in second_turn], ["system", "user", "assistant", "user"])
        self.assertIn("<think>", second_turn[2]["content"])  # ...but go back to the model as history

        call = FakeModelHandler.requests[0]
        self.assertEqual(call["path"], "/v1/chat/completions")
        self.assertEqual(call["auth"], f"Bearer {self.secret}")
        self.assertEqual(call["body"]["model"], "fake-model")
        self.assertNotIn("tools", call["body"])  # talk, don't act
        self.assertEqual(call["body"]["messages"][0]["role"], "system")
        self.assertIn("cannot run commands", call["body"]["messages"][0]["content"])

        status = [s for s in self.req("GET", "/v1/status?project=p")[1]["statuses"] if s["agent"] == "fake-bot"][0]
        self.assertIn("fake-model", status["role"])  # the dashboard shows which model it runs
        self.assertEqual((status["branch"], status["commit_sha"]), ("", ""))  # no git pills on its card

    def test_key_never_appears_in_any_output(self):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            self.start_agent(self.agent("fake-bot"))  # default logger, printing to stdout
            self.send("human", "fake-bot", "First")
            self.wait_for(lambda: [m for m in self.messages() if m["sender"] == "fake-bot"])
            FakeModelHandler.mode = "deny"  # the model server now echoes the key back in its error
            self.send("human", "fake-bot", "Second")
            status = self.wait_for(lambda: [s for s in self.req("GET", "/v1/status?project=p")[1]["statuses"]
                                            if s["agent"] == "fake-bot" and s["state"] == "blocked"])[0]
            self.stop_agents()

        self.assertEqual(FakeModelHandler.requests[-1]["auth"], f"Bearer {self.secret}")  # the key was used
        self.assertIn("[redacted]", status["detail"])  # and the echo was caught
        self.assertIn("[redacted]", out.getvalue())
        outputs = {
            "stdout": out.getvalue(), "stderr": err.getvalue(), "status detail": status["detail"],
            "messages API": self.req("GET", "/v1/messages?limit=1000")[0],
            "status API": self.req("GET", "/v1/status")[0],
        }
        for where, text in outputs.items():
            self.assertNotIn(self.secret, text, where)
        for f in self.tmp_path.rglob("*"):  # the database, its WAL and every Markdown copy
            if f.is_file() and f.name != "agents.json":
                self.assertNotIn(self.secret.encode(), f.read_bytes(), str(f))

    def test_two_api_agents_stop_their_ping_pong(self):
        log = io.StringIO()
        with contextlib.redirect_stdout(log):
            self.start_agent(self.agent("alpha", max_turns_per_thread=3))
            self.start_agent(self.agent("beta", max_turns_per_thread=3))
            # beta writes to alpha by name: each answers the other, which without a guard never ends.
            self.send("beta", "alpha", "Ping", "Your turn.")
            msgs = self.settle()
            time.sleep(1.0)
            self.assertEqual(len(self.messages()), len(msgs), "still talking after the cap")

        by = lambda name: [m for m in msgs if m["sender"] == name]
        self.assertEqual(len(by("alpha")), 3)
        self.assertEqual(len(by("beta")), 1 + 3)  # the opening message plus three replies
        self.assertEqual(len(FakeModelHandler.requests), 6)
        self.assertIn("turn cap reached", log.getvalue())

    def test_broadcasts_get_answers_only_from_people(self):
        with contextlib.redirect_stdout(io.StringIO()):
            self.start_agent(self.agent("alpha"))
            self.start_agent(self.agent("beta"))
            self.send("claude", "all", "Build is green")  # an agent's broadcast: no answers
            self.send("human", "all", "Standup")          # a person's broadcast: one answer each
            msgs = self.settle()
        replies = [m for m in msgs if m["sender"] in ("alpha", "beta")]
        self.assertEqual(sorted((m["sender"], m["recipient"], m["subject"]) for m in replies),
                         [("alpha", "human", "Re: Standup"), ("beta", "human", "Re: Standup")])

    # ---- Dashboard endpoints ----

    def write_agents(self, **first):
        agents = [
            {"name": "cloud-bot", "provider": "Fake", "base_url": self.model_url, "model": "fake-model",
             "api_key_env": self.KEY_ENV, "system_prompt": "Be brief.", "projects": ["p"], **first},
            {"name": "local-bot", "base_url": self.model_url, "model": "llama", "api_key_env": ""},
        ]
        self.agents_path.write_text(json.dumps({"agents": agents}), encoding="utf-8")

    def test_agents_config_endpoint_never_returns_keys(self):
        self.write_agents()
        raw, data = self.req("GET", "/v1/agents/config")
        self.assertNotIn(self.secret, raw)
        cloud, local = data["agents"]
        self.assertEqual((cloud["api_key_env"], cloud["key_needed"], cloud["key_set"]), (self.KEY_ENV, True, True))
        self.assertEqual((local["key_needed"], local["key_set"]), (False, False))
        self.assertNotIn("system_prompt", cloud)

        with mock.patch.dict(os.environ):
            del os.environ[self.KEY_ENV]
            self.assertFalse(self.req("GET", "/v1/agents/config")[1]["agents"][0]["key_set"])

        # A key pasted into agents.json is refused and never echoed.
        self.write_agents(api_key=self.secret)
        raw, data = self.req("GET", "/v1/agents/config")
        self.assertNotIn(self.secret, raw)
        self.assertIn("api_key", data["error"])
        self.assertEqual(data["agents"], [])

        self.agents_path.unlink()
        self.assertFalse(self.req("GET", "/v1/agents/config")[1]["found"])

    def test_connection_endpoint_reports_ok_or_a_redacted_error(self):
        self.write_agents()
        raw, data = self.req("POST", "/v1/agents/test", {"name": "cloud-bot"})
        self.assertTrue(data["ok"], raw)
        self.assertEqual(FakeModelHandler.requests[-1]["auth"], f"Bearer {self.secret}")

        FakeModelHandler.mode = "deny"
        raw, data = self.req("POST", "/v1/agents/test", {"name": "cloud-bot"})
        self.assertFalse(data["ok"])
        self.assertIn("HTTP 401", data["error"])
        self.assertIn("[redacted]", data["error"])
        self.assertNotIn(self.secret, raw)

        with mock.patch.dict(os.environ):
            del os.environ[self.KEY_ENV]
            data = self.req("POST", "/v1/agents/test", {"name": "cloud-bot"})[1]
        self.assertFalse(data["ok"])
        self.assertIn(self.KEY_ENV, data["error"])
        self.assertFalse(self.req("POST", "/v1/agents/test", {"name": "nobody"})[1]["ok"])

    # ---- listen ----

    def test_listen_prints_each_message_once(self):
        def listen(agent, *extra):
            proc = subprocess.Popen(
                [sys.executable, str(Path(cli.__file__)), "--url", self.base_url,
                 "listen", "--project", "p", "--agent", agent, "--every", "1", *extra],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding="utf-8",
            )
            lines = []
            threading.Thread(target=lambda: lines.extend(iter(proc.stdout.readline, "")), daemon=True).start()
            return proc, lines

        codex, codex_out = listen("codex")
        gemini, gemini_out = listen("gemini", "--ack")
        try:
            self.send("human", "all", "Morning plan")
            self.send("human", "codex", "Only for codex")
            self.wait_for(lambda: "".join(codex_out).count("Only for codex") and "".join(gemini_out).count("Morning plan"))
            time.sleep(2.5)  # several more one-second waits: nothing may print twice
        finally:
            for p in (codex, gemini):
                p.terminate()
                p.wait(timeout=10)
                p.stdout.close()
                p.stderr.close()

        codex_text, gemini_text = "".join(codex_out), "".join(gemini_out)
        self.assertEqual(codex_text.count("Subject:   Morning plan"), 1)
        self.assertEqual(codex_text.count("Subject:   Only for codex"), 1)
        self.assertEqual(gemini_text.count("Subject:   Morning plan"), 1)
        self.assertNotIn("Only for codex", gemini_text)
        self.assertNotIn("Timed out", codex_text)  # timeouts print nothing
        acks = {m["subject"]: m["ack_by"] for m in self.messages()}
        self.assertEqual(acks["Morning plan"], ["gemini"])  # --ack acknowledged; codex did not
        self.assertEqual(acks["Only for codex"], [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
