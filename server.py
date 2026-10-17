#!/usr/bin/env python3
"""
WE Relay (server.py)
Multi-project, zero-dependency local message bus for AI coding agents: SQLite WAL
persistence, Markdown copies of messages, long-polling wake-ups, HMAC-SHA256 webhooks,
SSE stream and a live dashboard.
Binds to 127.0.0.1:8787 by default (override with RELAY_HOST / RELAY_PORT).
Data lives in RELAY_HOME (default ~/.we-relay).
"""

import argparse
import datetime
import hashlib
import hmac
import http.server
import json
import os
import re
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

BASE_DIR = Path(__file__).resolve().parent
INDEX_HTML_PATH = BASE_DIR / "index.html"
URL_FILE_NAME = ".relay-url"


def relay_home() -> Path:
    return Path(os.environ.get("RELAY_HOME") or Path.home() / ".we-relay").expanduser()


def default_db_path() -> Path:
    return Path(os.environ.get("RELAY_DB_PATH") or relay_home() / "relay.db")


def default_handoffs_dir() -> Path:
    return Path(os.environ.get("RELAY_HANDOFFS_DIR") or relay_home() / "handoffs")


DEFAULT_PROJECT = "default"
VALID_STATES = {"working", "waiting_for_review", "frozen", "blocked", "done", "idle"}

DEFAULT_AGENT_ROLES = {
    "claude": "Claude Code",
    "codex": "Codex CLI",
    "gemini": "Gemini CLI / Antigravity",
    "human": "Human lead / supervisor",
}

DEFAULT_PROJECTS = [
    {
        "slug": DEFAULT_PROJECT,
        "name": "Default",
        "description": "Default project. Add more from the dashboard or with the projects command.",
    },
]


def utc_now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def utc_now_filename_ts() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d-%H%M%S")


def sanitize_slug(val: str, default: str = DEFAULT_PROJECT) -> str:
    raw = (val or "").strip().lower()
    if not raw:
        return default
    cleaned = re.sub(r"[^a-z0-9_-]+", "-", raw).strip("-")
    return cleaned or default


class RelayState:
    def __init__(self, db_path: Path, handoffs_dir: Path):
        self.db_path = Path(db_path)
        self.handoffs_dir = Path(handoffs_dir)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.handoffs_dir.mkdir(parents=True, exist_ok=True)
        self.db_lock = threading.RLock()
        self.wait_cond = threading.Condition()
        self.sse_lock = threading.Lock()
        self.sse_queues: List[List[Dict[str, Any]]] = []
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=30.0, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=10000;")
        return conn

    def _init_db(self) -> None:
        with self.db_lock:
            conn = self._connect()
            try:
                conn.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS projects (
                        slug TEXT PRIMARY KEY,
                        name TEXT NOT NULL,
                        description TEXT NOT NULL DEFAULT '',
                        created_at TEXT NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS messages (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        msg_uuid TEXT NOT NULL UNIQUE,
                        project TEXT NOT NULL DEFAULT 'default',
                        sender TEXT NOT NULL,
                        recipient TEXT NOT NULL,
                        subject TEXT NOT NULL,
                        body TEXT NOT NULL,
                        file_path TEXT NOT NULL,
                        ack_by TEXT NOT NULL DEFAULT '[]',
                        created_at TEXT NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS agent_status (
                        project TEXT NOT NULL DEFAULT 'default',
                        agent TEXT NOT NULL,
                        role TEXT NOT NULL,
                        branch TEXT NOT NULL,
                        commit_sha TEXT NOT NULL,
                        state TEXT NOT NULL,
                        detail TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        PRIMARY KEY (project, agent)
                    );

                    CREATE TABLE IF NOT EXISTS webhooks (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        project TEXT NOT NULL DEFAULT 'all',
                        agent TEXT NOT NULL,
                        url TEXT NOT NULL,
                        secret TEXT NOT NULL DEFAULT '',
                        created_at TEXT NOT NULL,
                        last_status TEXT NOT NULL DEFAULT 'never',
                        last_Attempt_at TEXT NOT NULL DEFAULT ''
                    );
                    """
                )
                # Ensure default projects exist
                now_iso = utc_now_iso()
                for p in DEFAULT_PROJECTS:
                    conn.execute(
                        """
                        INSERT INTO projects (slug, name, description, created_at)
                        VALUES (?, ?, ?, ?)
                        ON CONFLICT(slug) DO NOTHING
                        """,
                        (p["slug"], p["name"], p["description"], now_iso),
                    )
                conn.commit()
            finally:
                conn.close()

    def ensure_project(
        self, slug: str, name: Optional[str] = None, description: str = ""
    ) -> Dict[str, Any]:
        slug = sanitize_slug(slug)
        if slug == "all":
            return {
                "slug": "all",
                "name": "All Projects",
                "description": "Combined view across all projects",
                "created_at": utc_now_iso(),
            }
        display_name = (name or "").strip() or slug.replace("-", " ").title()
        now_iso = utc_now_iso()
        with self.db_lock:
            conn = self._connect()
            try:
                conn.execute(
                    """
                    INSERT INTO projects (slug, name, description, created_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(slug) DO UPDATE SET
                        name = CASE WHEN excluded.name != '' THEN excluded.name ELSE projects.name END,
                        description = CASE WHEN excluded.description != '' THEN excluded.description ELSE projects.description END
                    """,
                    (slug, display_name, description or "", now_iso),
                )
                conn.commit()
                row = conn.execute(
                    "SELECT * FROM projects WHERE slug = ?", (slug,)
                ).fetchone()
                return dict(row)
            finally:
                conn.close()

    def list_projects(self) -> List[Dict[str, Any]]:
        with self.db_lock:
            conn = self._connect()
            try:
                rows = conn.execute(
                    "SELECT * FROM projects ORDER BY created_at ASC, slug ASC"
                ).fetchall()
                projects = [dict(r) for r in rows]
                known = {p["slug"] for p in projects}

                # Auto-discover any project slugs present in messages or statuses
                extra_rows = conn.execute(
                    """
                    SELECT DISTINCT project FROM messages
                    UNION
                    SELECT DISTINCT project FROM agent_status
                    """
                ).fetchall()
                for r in extra_rows:
                    p_slug = r["project"]
                    if p_slug and p_slug != "all" and p_slug not in known:
                        proj = self.ensure_project(p_slug)
                        projects.append(proj)
                        known.add(p_slug)

                # Attach message & active agent counts per project
                msg_counts = {
                    r["project"]: r["cnt"]
                    for r in conn.execute(
                        "SELECT project, COUNT(*) AS cnt FROM messages GROUP BY project"
                    ).fetchall()
                }
                agent_counts = {
                    r["project"]: r["cnt"]
                    for r in conn.execute(
                        "SELECT project, COUNT(*) AS cnt FROM agent_status GROUP BY project"
                    ).fetchall()
                }
                for p in projects:
                    s = p["slug"]
                    p["message_count"] = msg_counts.get(s, 0)
                    p["agent_count"] = agent_counts.get(s, 0)
                return projects
            finally:
                conn.close()

    def _row_to_message(self, row: sqlite3.Row) -> Dict[str, Any]:
        try:
            ack_by = json.loads(row["ack_by"] or "[]")
        except Exception:
            ack_by = []
        return {
            "id": row["id"],
            "msg_uuid": row["msg_uuid"],
            "project": row["project"],
            "sender": row["sender"],
            "recipient": row["recipient"],
            "subject": row["subject"],
            "body": row["body"],
            "file_path": row["file_path"],
            "ack_by": ack_by,
            "created_at": row["created_at"],
        }

    def _row_to_status(self, row: sqlite3.Row) -> Dict[str, Any]:
        return {
            "project": row["project"],
            "agent": row["agent"],
            "role": row["role"],
            "branch": row["branch"],
            "commit_sha": row["commit_sha"],
            "state": row["state"],
            "detail": row["detail"],
            "updated_at": row["updated_at"],
        }

    def _row_to_webhook(self, row: sqlite3.Row) -> Dict[str, Any]:
        return {
            "id": row["id"],
            "project": row["project"],
            "agent": row["agent"],
            "url": row["url"],
            "has_secret": bool(row["secret"]),
            "created_at": row["created_at"],
            "last_status": row["last_status"],
            "last_Attempt_at": row["last_Attempt_at"],
            "last_attempt_at": row["last_Attempt_at"],
        }

    def broadcast_sse(self, event_type: str, data: Any) -> None:
        with self.sse_lock:
            for q in self.sse_queues:
                q.append({"event": event_type, "data": data})
        with self.wait_cond:
            self.wait_cond.notify_all()

    def create_message(
        self,
        sender: str,
        recipient: str,
        subject: str,
        body: Optional[str] = None,
        file_path: Optional[str] = None,
        project: str = DEFAULT_PROJECT,
    ) -> Dict[str, Any]:
        sender = sanitize_slug(sender, default="")
        recipient = sanitize_slug(recipient, default="")
        project = sanitize_slug(project, default=DEFAULT_PROJECT)
        if not sender:
            raise ValueError("sender is required")
        if not recipient:
            raise ValueError("recipient is required")

        subject = (subject or "").strip()
        if not subject:
            raise ValueError("subject is required")

        self.ensure_project(project)
        now_iso = utc_now_iso()
        msg_uuid = str(uuid.uuid4())

        resolved_file_path: Path
        if file_path:
            candidate = Path(file_path)
            if not candidate.is_absolute():
                candidate = self.handoffs_dir / candidate
            if not candidate.exists():
                raise ValueError(f"Referenced handoff file does not exist: {candidate}")
            resolved_file_path = candidate.resolve()
            if not body:
                body = resolved_file_path.read_text(encoding="utf-8", errors="replace")
        else:
            if body is None or not str(body).strip():
                raise ValueError("Either body or file_path must be provided")
            body = str(body)
            ts = utc_now_filename_ts()
            # Inline-message mirrors live in handoffs/relay/ so the main handoffs folder stays readable.
            mirror_dir = self.handoffs_dir / "relay"
            mirror_dir.mkdir(parents=True, exist_ok=True)
            filename = f"relay-{ts}-{sender}-to-{recipient}.md"
            resolved_file_path = (mirror_dir / filename).resolve()
            counter = 1
            while resolved_file_path.exists():
                filename = f"relay-{ts}-{counter}-{sender}-to-{recipient}.md"
                resolved_file_path = (mirror_dir / filename).resolve()
                counter += 1
            md_content = (
                f"# {subject}\n\n"
                f"- **Project:** `{project}`\n"
                f"- **From:** `{sender}`\n"
                f"- **To:** `{recipient}`\n"
                f"- **Timestamp (UTC):** `{now_iso}`\n"
                f"- **UUID:** `{msg_uuid}`\n\n"
                f"---\n\n"
                f"{body.rstrip()}\n"
            )
            resolved_file_path.write_text(md_content, encoding="utf-8")

        with self.db_lock:
            conn = self._connect()
            try:
                cur = conn.execute(
                    """
                    INSERT INTO messages (msg_uuid, project, sender, recipient, subject, body, file_path, ack_by, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        msg_uuid,
                        project,
                        sender,
                        recipient,
                        subject,
                        body,
                        str(resolved_file_path),
                        "[]",
                        now_iso,
                    ),
                )
                msg_id = cur.lastrowid
                conn.commit()
                row = conn.execute(
                    "SELECT * FROM messages WHERE id = ?", (msg_id,)
                ).fetchone()
                msg = self._row_to_message(row)
            finally:
                conn.close()

        with self.wait_cond:
            self.wait_cond.notify_all()

        self.broadcast_sse("message", msg)
        self._dispatch_webhooks_async(msg)
        return msg

    def list_messages(
        self,
        agent: Optional[str] = None,
        unread_only: bool = False,
        since_id: int = 0,
        limit: int = 200,
        project: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        with self.db_lock:
            conn = self._connect()
            try:
                if project and project.strip().lower() not in ("", "all"):
                    proj_slug = sanitize_slug(project)
                    rows = conn.execute(
                        "SELECT * FROM messages WHERE id > ? AND project = ? ORDER BY id ASC LIMIT ?",
                        (since_id, proj_slug, limit),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        "SELECT * FROM messages WHERE id > ? ORDER BY id ASC LIMIT ?",
                        (since_id, limit),
                    ).fetchall()
                messages = [self._row_to_message(r) for r in rows]
            finally:
                conn.close()

        if agent:
            agent = agent.strip().lower()
            if agent != "all":
                filtered = []
                for m in messages:
                    is_for_agent = m["recipient"] == agent or (
                        m["recipient"] == "all" and m["sender"] != agent
                    )
                    if unread_only:
                        if is_for_agent and agent not in m["ack_by"]:
                            filtered.append(m)
                    else:
                        if is_for_agent or m["sender"] == agent:
                            filtered.append(m)
                messages = filtered
        elif unread_only:
            messages = [m for m in messages if len(m["ack_by"]) == 0]

        return messages

    def get_unacknowledged_for_wait(
        self, agent: str, since_id: int = 0, project: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        agent = agent.strip().lower()
        with self.db_lock:
            conn = self._connect()
            try:
                if project and project.strip().lower() not in ("", "all"):
                    proj_slug = sanitize_slug(project)
                    rows = conn.execute(
                        "SELECT * FROM messages WHERE id > ? AND project = ? ORDER BY id ASC",
                        (since_id, proj_slug),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        "SELECT * FROM messages WHERE id > ? ORDER BY id ASC",
                        (since_id,),
                    ).fetchall()
                messages = [self._row_to_message(r) for r in rows]
            finally:
                conn.close()

        matched = []
        for m in messages:
            if m["recipient"] == agent or (
                m["recipient"] == "all" and m["sender"] != agent
            ):
                if agent not in m["ack_by"]:
                    matched.append(m)
        return matched

    def ack_message(self, msg_id: int, agent: str) -> Dict[str, Any]:
        agent = sanitize_slug(agent, default="")
        if not agent:
            raise ValueError("Valid agent name is required")

        with self.db_lock:
            conn = self._connect()
            try:
                row = conn.execute(
                    "SELECT * FROM messages WHERE id = ?", (msg_id,)
                ).fetchone()
                if not row:
                    raise KeyError(f"Message id {msg_id} not found")
                msg = self._row_to_message(row)
                ack_by = list(msg["ack_by"])
                if agent not in ack_by:
                    ack_by.append(agent)
                    conn.execute(
                        "UPDATE messages SET ack_by = ? WHERE id = ?",
                        (json.dumps(ack_by), msg_id),
                    )
                    conn.commit()
                row = conn.execute(
                    "SELECT * FROM messages WHERE id = ?", (msg_id,)
                ).fetchone()
                updated = self._row_to_message(row)
            finally:
                conn.close()

        self.broadcast_sse("message", updated)
        return updated

    def update_status(
        self,
        agent: str,
        state: str,
        role: Optional[str] = None,
        branch: Optional[str] = None,
        commit_sha: Optional[str] = None,
        detail: Optional[str] = None,
        project: str = DEFAULT_PROJECT,
    ) -> Dict[str, Any]:
        agent = sanitize_slug(agent, default="")
        if not agent:
            raise ValueError("agent is required")
        project = sanitize_slug(project, default=DEFAULT_PROJECT)
        state = state.strip().lower()
        if state not in VALID_STATES:
            raise ValueError(
                f"Invalid state '{state}'. Must be one of {sorted(VALID_STATES)}"
            )

        self.ensure_project(project)
        now_iso = utc_now_iso()
        with self.db_lock:
            conn = self._connect()
            try:
                existing = conn.execute(
                    "SELECT * FROM agent_status WHERE project = ? AND agent = ?",
                    (project, agent),
                ).fetchone()
                resolved_role = (
                    role
                    if role is not None
                    else (
                        existing["role"]
                        if existing
                        else DEFAULT_AGENT_ROLES.get(agent, f"Agent ({agent})")
                    )
                )
                resolved_branch = (
                    branch
                    if branch is not None
                    else (existing["branch"] if existing else "main")
                )
                resolved_commit = (
                    commit_sha
                    if commit_sha is not None
                    else (existing["commit_sha"] if existing else "HEAD")
                )
                resolved_detail = (
                    detail
                    if detail is not None
                    else (existing["detail"] if existing else "")
                )

                conn.execute(
                    """
                    INSERT INTO agent_status (project, agent, role, branch, commit_sha, state, detail, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(project, agent) DO UPDATE SET
                        role = excluded.role,
                        branch = excluded.branch,
                        commit_sha = excluded.commit_sha,
                        state = excluded.state,
                        detail = excluded.detail,
                        updated_at = excluded.updated_at
                    """,
                    (
                        project,
                        agent,
                        resolved_role,
                        resolved_branch,
                        resolved_commit,
                        state,
                        resolved_detail,
                        now_iso,
                    ),
                )
                conn.commit()
                row = conn.execute(
                    "SELECT * FROM agent_status WHERE project = ? AND agent = ?",
                    (project, agent),
                ).fetchone()
                status_obj = self._row_to_status(row)
            finally:
                conn.close()

        self.broadcast_sse("status", status_obj)
        return status_obj

    def list_statuses(self, project: Optional[str] = None) -> List[Dict[str, Any]]:
        with self.db_lock:
            conn = self._connect()
            try:
                if project and project.strip().lower() not in ("", "all"):
                    proj_slug = sanitize_slug(project)
                    rows = conn.execute(
                        "SELECT * FROM agent_status WHERE project = ? ORDER BY agent ASC",
                        (proj_slug,),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        "SELECT * FROM agent_status ORDER BY project ASC, agent ASC"
                    ).fetchall()
                statuses = [self._row_to_status(r) for r in rows]
            finally:
                conn.close()

        priority = {"claude": 0, "codex": 1, "gemini": 2, "human": 3}
        statuses.sort(key=lambda s: (s["project"], priority.get(s["agent"], 99), s["agent"]))
        return statuses

    def add_webhook(
        self, agent: str, url: str, secret: str = "", project: str = "all"
    ) -> Dict[str, Any]:
        agent = sanitize_slug(agent or "all", default="all")
        project = sanitize_slug(project or "all", default="all")
        url = (url or "").strip()
        if not (url.startswith("http://") or url.startswith("https://")):
            raise ValueError("Webhook url must start with http:// or https://")

        now_iso = utc_now_iso()
        with self.db_lock:
            conn = self._connect()
            try:
                cur = conn.execute(
                    """
                    INSERT INTO webhooks (project, agent, url, secret, created_at, last_status, last_Attempt_at)
                    VALUES (?, ?, ?, ?, ?, 'never', '')
                    """,
                    (project, agent, url, secret or "", now_iso),
                )
                wh_id = cur.lastrowid
                conn.commit()
                row = conn.execute(
                    "SELECT * FROM webhooks WHERE id = ?", (wh_id,)
                ).fetchone()
                return self._row_to_webhook(row)
            finally:
                conn.close()

    def list_webhooks(self, project: Optional[str] = None) -> List[Dict[str, Any]]:
        with self.db_lock:
            conn = self._connect()
            try:
                if project and project.strip().lower() not in ("", "all"):
                    proj_slug = sanitize_slug(project)
                    rows = conn.execute(
                        "SELECT * FROM webhooks WHERE project IN ('all', ?) ORDER BY id ASC",
                        (proj_slug,),
                    ).fetchall()
                else:
                    rows = conn.execute("SELECT * FROM webhooks ORDER BY id ASC").fetchall()
                return [self._row_to_webhook(r) for r in rows]
            finally:
                conn.close()

    def delete_webhook(
        self, webhook_id: Optional[int] = None, url: Optional[str] = None
    ) -> int:
        with self.db_lock:
            conn = self._connect()
            try:
                if webhook_id is not None:
                    cur = conn.execute("DELETE FROM webhooks WHERE id = ?", (webhook_id,))
                elif url:
                    cur = conn.execute("DELETE FROM webhooks WHERE url = ?", (url,))
                else:
                    raise ValueError("Specify id or url to delete webhook")
                conn.commit()
                return cur.rowcount
            finally:
                conn.close()

    def _dispatch_webhooks_async(self, msg: Dict[str, Any]) -> None:
        with self.db_lock:
            conn = self._connect()
            try:
                rows = conn.execute("SELECT * FROM webhooks").fetchall()
                webhooks = [dict(r) for r in rows]
            finally:
                conn.close()

        matching = []
        for wh in webhooks:
            wh_proj = wh.get("project") or "all"
            if wh_proj not in ("all", msg["project"]):
                continue
            wh_agent = wh["agent"]
            if (
                wh_agent == "all"
                or wh_agent == msg["recipient"]
                or (msg["recipient"] == "all" and wh_agent != msg["sender"])
            ):
                matching.append(wh)

        if not matching:
            return

        payload_dict = {
            "id": msg["id"],
            "msg_uuid": msg["msg_uuid"],
            "project": msg["project"],
            "sender": msg["sender"],
            "recipient": msg["recipient"],
            "subject": msg["subject"],
            "body": msg["body"],
            "file_path": msg["file_path"],
            "created_at": msg["created_at"],
        }
        payload_bytes = json.dumps(payload_dict).encode("utf-8")

        for wh in matching:
            t = threading.Thread(
                target=self._deliver_single_webhook,
                args=(wh, payload_bytes),
                daemon=True,
            )
            t.start()

    def _deliver_single_webhook(self, wh: Dict[str, Any], payload_bytes: bytes) -> None:
        secret = (wh.get("secret") or "").encode("utf-8")
        digest = hmac.new(secret, payload_bytes, hashlib.sha256).hexdigest()
        sig_header = f"sha256={digest}"
        attempt_ts = utc_now_iso()
        status_str = "unknown"

        try:
            req = urllib.request.Request(
                wh["url"],
                data=payload_bytes,
                method="POST",
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "WERelay/1.0",
                    "X-Relay-Signature": sig_header,
                },
            )
            with urllib.request.urlopen(req, timeout=10.0) as resp:
                status_str = f"{resp.status} OK"
        except urllib.error.HTTPError as e:
            status_str = f"HTTP {e.code}"
        except Exception as e:
            status_str = f"ERROR: {type(e).__name__}"

        with self.db_lock:
            conn = self._connect()
            try:
                conn.execute(
                    "UPDATE webhooks SET last_status = ?, last_Attempt_at = ? WHERE id = ?",
                    (status_str, attempt_ts, wh["id"]),
                )
                conn.commit()
            finally:
                conn.close()


class RelayHTTPServer(http.server.ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = os.name != "nt"

    def __init__(self, server_address, RequestHandlerClass, relay_state: RelayState):
        super().__init__(server_address, RequestHandlerClass)
        self.relay_state = relay_state


class RelayRequestHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: Any) -> None:
        # sys.stderr is None under pythonw.exe (no console), so skip logging there.
        if os.environ.get("RELAY_QUIET") == "1" or sys.stderr is None:
            return
        super().log_message(format, *args)

    def _send_json(self, status_code: int, payload: Any) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _same_origin(self) -> bool:
        # The dashboard is served by this server, so it needs no CORS. Browsers send an
        # Origin header on cross-site writes; refuse those. The CLI sends no Origin.
        origin = self.headers.get("Origin")
        if not origin:
            return True
        return urllib.parse.urlparse(origin).netloc == self.headers.get("Host", "")

    def _send_html(self, status_code: int, html_bytes: bytes) -> None:
        self.send_response(status_code)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(html_bytes)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(html_bytes)

    def _read_json_body(self) -> Dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0") or "0")
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        return json.loads(raw.decode("utf-8"))

    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        qs = urllib.parse.parse_qs(parsed.query)
        state: RelayState = self.server.relay_state  # type: ignore

        try:
            if path in ("/", "/index.html"):
                if INDEX_HTML_PATH.exists():
                    self._send_html(200, INDEX_HTML_PATH.read_bytes())
                else:
                    self._send_html(200, b"<h1>WE Relay running</h1>")
                return

            if path == "/v1/health":
                self._send_json(200, {"ok": True, "time": utc_now_iso()})
                return

            if path == "/v1/projects":
                projects = state.list_projects()
                self._send_json(200, {"projects": projects})
                return

            if path == "/v1/messages":
                agent = qs.get("agent", [None])[0]
                project = qs.get("project", [None])[0]
                unread = qs.get("unread", ["false"])[0].lower() in ("1", "true", "yes")
                since_id = int(qs.get("since_id", ["0"])[0] or "0")
                limit = int(qs.get("limit", ["200"])[0] or "200")
                msgs = state.list_messages(
                    agent=agent,
                    unread_only=unread,
                    since_id=since_id,
                    limit=limit,
                    project=project,
                )
                self._send_json(200, {"messages": msgs})
                return

            if path == "/v1/wait":
                agent = (qs.get("agent", [""])[0] or "").strip().lower()
                if not agent:
                    self._send_json(400, {"error": "Query param 'agent' is required"})
                    return
                project = qs.get("project", [None])[0]
                since_id = int(qs.get("since_id", ["0"])[0] or "0")
                timeout = float(qs.get("timeout", ["300"])[0] or "300")
                timeout = max(0.05, min(timeout, 3600.0))
                auto_ack = qs.get("auto_ack", ["false"])[0].lower() in (
                    "1",
                    "true",
                    "yes",
                )

                deadline = time.monotonic() + timeout
                with state.wait_cond:
                    while True:
                        msgs = state.get_unacknowledged_for_wait(
                            agent, since_id=since_id, project=project
                        )
                        if msgs:
                            break
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            break
                        state.wait_cond.wait(timeout=remaining)

                if msgs and auto_ack:
                    acked = []
                    for m in msgs:
                        acked.append(state.ack_message(m["id"], agent))
                    msgs = acked

                self._send_json(
                    200,
                    {
                        "messages": msgs,
                        "timed_out": len(msgs) == 0,
                    },
                )
                return

            if path == "/v1/status":
                project = qs.get("project", [None])[0]
                statuses = state.list_statuses(project=project)
                self._send_json(200, {"statuses": statuses})
                return

            if path == "/v1/webhooks":
                project = qs.get("project", [None])[0]
                webhooks = state.list_webhooks(project=project)
                self._send_json(200, {"webhooks": webhooks})
                return

            if path == "/v1/stream":
                self._handle_sse(state)
                return

            self._send_json(404, {"error": f"Not found: {path}"})
        except Exception as e:
            self._send_json(500, {"error": str(e)})

    def _handle_sse(self, state: RelayState) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.send_header("X-Accel-Buffering", "no")
        self.end_headers()

        queue: List[Dict[str, Any]] = []
        with state.sse_lock:
            state.sse_queues.append(queue)

        try:
            init_payload = json.dumps(
                {
                    "projects": state.list_projects(),
                    "statuses": state.list_statuses(),
                    "messages": state.list_messages(limit=200),
                }
            )
            self.wfile.write(f"event: init\ndata: {init_payload}\n\n".encode("utf-8"))
            self.wfile.flush()

            while True:
                with state.wait_cond:
                    if not queue:
                        state.wait_cond.wait(timeout=15.0)
                with state.sse_lock:
                    events = list(queue)
                    queue.clear()

                if not events:
                    self.wfile.write(b": heartbeat\n\n")
                    self.wfile.flush()
                    continue

                for ev in events:
                    ev_name = ev["event"]
                    ev_data = json.dumps(ev["data"])
                    self.wfile.write(
                        f"event: {ev_name}\ndata: {ev_data}\n\n".encode("utf-8")
                    )
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError):
            pass
        finally:
            with state.sse_lock:
                if queue in state.sse_queues:
                    state.sse_queues.remove(queue)

    def do_POST(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        state: RelayState = self.server.relay_state  # type: ignore

        if not self._same_origin():
            self._send_json(403, {"error": "Cross-origin request refused"})
            return

        try:
            data = self._read_json_body()
        except Exception as e:
            self._send_json(400, {"error": f"Invalid JSON body: {e}"})
            return

        try:
            if path == "/v1/projects":
                slug = data.get("slug") or data.get("name") or ""
                if not slug:
                    self._send_json(400, {"error": "'slug' or 'name' is required"})
                    return
                name = data.get("name") or slug
                description = data.get("description") or ""
                proj = state.ensure_project(slug=slug, name=name, description=description)
                state.broadcast_sse("project", proj)
                self._send_json(201, {"project": proj})
                return

            if path == "/v1/messages":
                sender = data.get("sender") or data.get("from") or ""
                recipient = data.get("recipient") or data.get("to") or ""
                subject = data.get("subject") or ""
                body = data.get("body")
                file_path = data.get("file_path") or data.get("file")
                project = data.get("project") or DEFAULT_PROJECT
                msg = state.create_message(
                    sender=sender,
                    recipient=recipient,
                    subject=subject,
                    body=body,
                    file_path=file_path,
                    project=project,
                )
                self._send_json(201, {"message": msg})
                return

            if path == "/v1/ack" or re.match(r"^/v1/messages/\d+/ack$", path):
                m = re.match(r"^/v1/messages/(\d+)/ack$", path)
                msg_id = int(m.group(1)) if m else int(data.get("id", 0))
                agent = data.get("agent") or ""
                if not msg_id or not agent:
                    self._send_json(400, {"error": "Both 'id' and 'agent' are required"})
                    return
                updated = state.ack_message(msg_id=msg_id, agent=agent)
                self._send_json(200, {"message": updated})
                return

            if path == "/v1/status":
                agent = data.get("agent") or ""
                st = data.get("state") or ""
                role = data.get("role")
                branch = data.get("branch")
                commit_sha = data.get("commit_sha") or data.get("commit")
                detail = data.get("detail")
                project = data.get("project") or DEFAULT_PROJECT
                status_obj = state.update_status(
                    agent=agent,
                    state=st,
                    role=role,
                    branch=branch,
                    commit_sha=commit_sha,
                    detail=detail,
                    project=project,
                )
                self._send_json(200, {"status": status_obj})
                return

            if path == "/v1/webhooks":
                agent = data.get("agent") or "all"
                url = data.get("url") or ""
                secret = data.get("secret") or ""
                project = data.get("project") or "all"
                wh = state.add_webhook(
                    agent=agent, url=url, secret=secret, project=project
                )
                self._send_json(201, {"webhook": wh})
                return

            self._send_json(404, {"error": f"Not found: {path}"})
        except KeyError as e:
            self._send_json(404, {"error": str(e)})
        except ValueError as e:
            self._send_json(400, {"error": str(e)})
        except Exception as e:
            self._send_json(500, {"error": str(e)})

    def do_DELETE(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        qs = urllib.parse.parse_qs(parsed.query)
        state: RelayState = self.server.relay_state  # type: ignore

        if not self._same_origin():
            self._send_json(403, {"error": "Cross-origin request refused"})
            return

        try:
            if path == "/v1/webhooks":
                data = {}
                try:
                    data = self._read_json_body()
                except Exception:
                    data = {}
                wh_id_raw = qs.get("id", [None])[0] or data.get("id")
                url = qs.get("url", [None])[0] or data.get("url")
                wh_id = int(wh_id_raw) if wh_id_raw is not None else None
                deleted = state.delete_webhook(webhook_id=wh_id, url=url)
                self._send_json(200, {"deleted": deleted})
                return

            self._send_json(404, {"error": f"Not found: {path}"})
        except ValueError as e:
            self._send_json(400, {"error": str(e)})
        except Exception as e:
            self._send_json(500, {"error": str(e)})


def create_server(
    host: str = "127.0.0.1",
    port: int = 8787,
    db_path: Optional[Path] = None,
    handoffs_dir: Optional[Path] = None,
) -> RelayHTTPServer:
    if host not in ("127.0.0.1", "localhost", "0.0.0.0"):
        raise ValueError("Host must be localhost/127.0.0.1 (or 0.0.0.0 inside container)")
    state = RelayState(
        db_path=db_path or default_db_path(),
        handoffs_dir=handoffs_dir or default_handoffs_dir(),
    )
    return RelayHTTPServer((host, port), RelayRequestHandler, state)


def main() -> None:
    parser = argparse.ArgumentParser(description="WE Relay server")
    parser.add_argument(
        "--host",
        default=os.environ.get("RELAY_HOST", "127.0.0.1"),
        help="Bind host (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("RELAY_PORT", "8787")),
        help="Bind port (default: 8787, auto-falls back to 8788 if 8787 is occupied)",
    )
    parser.add_argument(
        "--db",
        default=None,
        help="Path to SQLite database (default: $RELAY_DB_PATH or $RELAY_HOME/relay.db)",
    )
    parser.add_argument(
        "--handoffs-dir",
        default=None,
        help="Folder for Markdown copies of messages (default: $RELAY_HANDOFFS_DIR or $RELAY_HOME/handoffs)",
    )
    args = parser.parse_args()
    db_path = Path(args.db) if args.db else None
    handoffs_dir = Path(args.handoffs_dir) if args.handoffs_dir else None

    port = args.port
    try:
        srv = create_server(
            host=args.host, port=port, db_path=db_path, handoffs_dir=handoffs_dir
        )
    except OSError:
        if port == 8787:
            port = 8788
            srv = create_server(
                host=args.host, port=port, db_path=db_path, handoffs_dir=handoffs_dir
            )
        else:
            raise

    active_url = f"http://{args.host}:{port}"
    try:
        home = relay_home()
        home.mkdir(parents=True, exist_ok=True)
        (home / URL_FILE_NAME).write_text(active_url, encoding="utf-8")
    except Exception:
        pass

    print(
        f"[we-relay] Listening on {active_url} "
        f"(db={srv.relay_state.db_path}, handoffs={srv.relay_state.handoffs_dir})",
        flush=True,
    )
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\n[we-relay] Shutting down...", flush=True)
        srv.server_close()


if __name__ == "__main__":
    main()
