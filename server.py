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
import base64
import datetime
import hashlib
import hmac
import http.server
import json
import mimetypes
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
ASSETS_DIR = BASE_DIR / "assets"
# Attachment types a browser may show in place. Anything else (HTML, SVG, scripts...) is only ever
# downloaded, so an uploaded file can never run on the dashboard's origin.
INLINE_ATTACHMENT_TYPES = {
    "image/png", "image/jpeg", "image/gif", "image/webp",
    "audio/webm", "audio/ogg", "audio/mpeg", "audio/wav", "audio/mp4",
    "video/webm", "video/mp4", "application/pdf", "text/plain",
}
ASSET_TYPES = {
    ".webp": "image/webp",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".svg": "image/svg+xml",
    ".woff2": "font/woff2",
}
URL_FILE_NAME = ".relay-url"


def relay_home() -> Path:
    if os.environ.get("RELAY_HOME"):
        return Path(os.environ["RELAY_HOME"]).expanduser()
    if (BASE_DIR / "relay.db").exists() or (BASE_DIR / URL_FILE_NAME).exists():
        return BASE_DIR
    return Path.home() / ".we-relay"


def default_db_path() -> Path:
    return Path(os.environ.get("RELAY_DB_PATH") or relay_home() / "relay.db")


def default_handoffs_dir() -> Path:
    return Path(os.environ.get("RELAY_HANDOFFS_DIR") or relay_home() / "handoffs")


DEFAULT_PROJECT = "default"
# Senders that count as a person: a message from one of them wakes every waiting agent in the
# project, even when it is addressed to someone else. Set with RELAY_HUMANS or --humans.
DEFAULT_HUMANS = ("human",)
MAX_ATTACHMENT_BYTES = 25 * 1024 * 1024
VALID_STATES = {"working", "waiting_for_review", "frozen", "blocked", "done", "idle"}
VALID_DELIVERY_STATES = {"queued", "submitted", "delivered", "needs_attention"}
VALID_INTERACTION_TYPES = {"", "approval", "question"}
VALID_INTERACTION_STATUSES = {"", "pending", "approved", "denied", "answered"}

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


def sanitize_filename(name: str, default: str = "attachment.bin") -> str:
    raw = os.path.basename((name or "").strip())
    if not raw:
        return default
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", raw).strip("._-")
    return cleaned or default


def parse_humans(raw: Optional[str]) -> frozenset:
    names = {sanitize_slug(n, default="") for n in (raw or "").split(",")}
    return frozenset(n for n in names if n) or frozenset(DEFAULT_HUMANS)


class RelayState:
    def __init__(self, db_path: Path, handoffs_dir: Path, humans: Optional[str] = None):
        self.humans = parse_humans(humans if humans is not None else os.environ.get("RELAY_HUMANS"))
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
                        attachments TEXT NOT NULL DEFAULT '[]',
                        is_private INTEGER NOT NULL DEFAULT 0,
                        thread_id INTEGER,
                        reply_to_id INTEGER,
                        delivery_state TEXT NOT NULL DEFAULT 'queued',
                        is_interrupt INTEGER NOT NULL DEFAULT 0,
                        allow_peers TEXT NOT NULL DEFAULT '',
                        comm_rules TEXT NOT NULL DEFAULT '',
                        interaction_type TEXT NOT NULL DEFAULT '',
                        interaction_status TEXT NOT NULL DEFAULT '',
                        interaction_response TEXT NOT NULL DEFAULT '',
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
                # Ensure attachments and is_private columns exist on existing relay.db without data loss
                msg_cols = {
                    r["name"]
                    for r in conn.execute("PRAGMA table_info(messages)").fetchall()
                }
                if "attachments" not in msg_cols:
                    conn.execute(
                        "ALTER TABLE messages ADD COLUMN attachments TEXT NOT NULL DEFAULT '[]'"
                    )
                if "is_private" not in msg_cols:
                    conn.execute(
                        "ALTER TABLE messages ADD COLUMN is_private INTEGER NOT NULL DEFAULT 0"
                    )
                if "thread_id" not in msg_cols:
                    conn.execute(
                        "ALTER TABLE messages ADD COLUMN thread_id INTEGER"
                    )
                if "reply_to_id" not in msg_cols:
                    conn.execute(
                        "ALTER TABLE messages ADD COLUMN reply_to_id INTEGER"
                    )
                if "delivery_state" not in msg_cols:
                    conn.execute(
                        "ALTER TABLE messages ADD COLUMN delivery_state TEXT NOT NULL DEFAULT 'queued'"
                    )
                if "is_interrupt" not in msg_cols:
                    conn.execute(
                        "ALTER TABLE messages ADD COLUMN is_interrupt INTEGER NOT NULL DEFAULT 0"
                    )
                if "allow_peers" not in msg_cols:
                    conn.execute(
                        "ALTER TABLE messages ADD COLUMN allow_peers TEXT NOT NULL DEFAULT ''"
                    )
                if "comm_rules" not in msg_cols:
                    conn.execute(
                        "ALTER TABLE messages ADD COLUMN comm_rules TEXT NOT NULL DEFAULT ''"
                    )
                if "interaction_type" not in msg_cols:
                    conn.execute(
                        "ALTER TABLE messages ADD COLUMN interaction_type TEXT NOT NULL DEFAULT ''"
                    )
                if "interaction_status" not in msg_cols:
                    conn.execute(
                        "ALTER TABLE messages ADD COLUMN interaction_status TEXT NOT NULL DEFAULT ''"
                    )
                if "interaction_response" not in msg_cols:
                    conn.execute(
                        "ALTER TABLE messages ADD COLUMN interaction_response TEXT NOT NULL DEFAULT ''"
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

    @property
    def attachments_dir(self) -> Path:
        d = self.handoffs_dir / "relay" / "attachments"
        d.mkdir(parents=True, exist_ok=True)
        return d

    def save_attachment(
        self,
        filename: str,
        raw_bytes: bytes,
        content_type: Optional[str] = None,
        project: str = DEFAULT_PROJECT,
        sender: str = "human",
    ) -> Dict[str, Any]:
        clean_name = sanitize_filename(filename)
        ts = utc_now_filename_ts()
        att_dir = self.attachments_dir
        saved_name = f"{ts}-{clean_name}"
        dest = (att_dir / saved_name).resolve()
        counter = 1
        while dest.exists():
            saved_name = f"{ts}-{counter}-{clean_name}"
            dest = (att_dir / saved_name).resolve()
            counter += 1

        dest.write_bytes(raw_bytes)
        guessed_type, _ = mimetypes.guess_type(clean_name)
        resolved_type = (
            (content_type or "").strip()
            or guessed_type
            or "application/octet-stream"
        )
        return {
            "filename": clean_name,
            "saved_name": saved_name,
            "path": str(dest),
            "url": f"/v1/attachments/{urllib.parse.quote(saved_name)}",
            "size": len(raw_bytes),
            "content_type": resolved_type,
            "project": sanitize_slug(project, default=DEFAULT_PROJECT),
            "sender": sanitize_slug(sender, default="human"),
            "created_at": utc_now_iso(),
        }

    def get_attachment_path(self, saved_name: str) -> Optional[Path]:
        clean = os.path.basename((saved_name or "").strip())
        if not clean:
            return None
        root = self.attachments_dir.resolve()
        target = (root / clean).resolve()
        if not target.is_relative_to(root) or not target.is_file():
            return None
        return target

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

    def _row_to_message(self, row: sqlite3.Row, viewer_agent: Optional[str] = None) -> Dict[str, Any]:
        keys = row.keys()
        try:
            ack_by = json.loads(row["ack_by"] or "[]")
        except Exception:
            ack_by = []
        try:
            raw_att = row["attachments"] if "attachments" in keys else "[]"
            attachments = json.loads(raw_att or "[]")
        except Exception:
            attachments = []
        is_private = bool(row["is_private"]) if "is_private" in keys else False
        raw_reply_to = row["reply_to_id"] if "reply_to_id" in keys else None
        reply_to_id = int(raw_reply_to) if raw_reply_to is not None else None
        raw_thread = row["thread_id"] if "thread_id" in keys else None
        thread_id = int(raw_thread) if raw_thread is not None else int(row["id"])
        recipient = row["recipient"]
        cc = "" if (is_private or recipient == "all") else "all"
        raw_dstate = (row["delivery_state"] if "delivery_state" in keys else "") or ""
        delivery_state = raw_dstate if raw_dstate in VALID_DELIVERY_STATES else ("delivered" if ack_by else "queued")
        is_interrupt = bool(row["is_interrupt"]) if "is_interrupt" in keys else False
        raw_peers = (row["allow_peers"] if "allow_peers" in keys else "") or ""
        allow_peers = [sanitize_slug(p, default="") for p in raw_peers.split(",") if sanitize_slug(p, default="")]
        comm_rules = str(row["comm_rules"] if "comm_rules" in keys else "") or ""
        interaction_type = str(row["interaction_type"] if "interaction_type" in keys else "") or ""
        interaction_status = str(row["interaction_status"] if "interaction_status" in keys else "") or ""
        raw_iresp = str(row["interaction_response"] if "interaction_response" in keys else "") or ""
        try:
            interaction_response = json.loads(raw_iresp) if raw_iresp.startswith("{") else raw_iresp
        except Exception:
            interaction_response = raw_iresp
        msg: Dict[str, Any] = {
            "id": row["id"],
            "msg_uuid": row["msg_uuid"],
            "project": row["project"],
            "sender": row["sender"],
            "recipient": recipient,
            "cc": cc,
            "is_private": is_private,
            "thread_id": thread_id,
            "reply_to_id": reply_to_id,
            "delivery_state": delivery_state,
            "is_interrupt": is_interrupt,
            "allow_peers": allow_peers,
            "comm_rules": comm_rules,
            "interaction_type": interaction_type,
            "interaction_status": interaction_status,
            "interaction_response": interaction_response,
            "subject": row["subject"],
            "body": row["body"],
            "file_path": row["file_path"],
            "attachments": attachments,
            "ack_by": ack_by,
            "created_at": row["created_at"],
        }
        if viewer_agent and viewer_agent != "all":
            v = viewer_agent.strip().lower()
            if row["sender"] == v:
                msg["delivery"] = f"SENT (to {recipient})"
            elif recipient in (v, "all"):
                msg["delivery"] = "TO YOU"
            else:
                msg["delivery"] = f"FYI (to {recipient})"
        return msg

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
        recipient: str = "",
        subject: str = "",
        body: Optional[str] = None,
        file_path: Optional[str] = None,
        project: str = DEFAULT_PROJECT,
        attachments: Optional[List[Dict[str, Any]]] = None,
        is_private: Optional[bool] = False,
        reply_to_id: Optional[int] = None,
        is_interrupt: bool = False,
        allow_peers: Optional[Any] = None,
        comm_rules: str = "",
        interaction_type: str = "",
    ) -> Dict[str, Any]:
        parent_msg: Optional[Dict[str, Any]] = None
        resolved_reply_to: Optional[int] = int(reply_to_id) if reply_to_id is not None else None
        resolved_thread_id: Optional[int] = None

        if resolved_reply_to is not None:
            with self.db_lock:
                conn = self._connect()
                try:
                    parent_row = conn.execute(
                        "SELECT * FROM messages WHERE id = ?", (resolved_reply_to,)
                    ).fetchone()
                    if not parent_row:
                        raise KeyError(f"Message id {resolved_reply_to} not found")
                    parent_msg = self._row_to_message(parent_row)
                finally:
                    conn.close()
            resolved_thread_id = int(parent_msg["thread_id"])
            project = parent_msg["project"]

        if resolved_reply_to is None and (project or "").strip().lower() in ("*", "all"):
            all_projs = [p["slug"] for p in self.list_projects() if p.get("slug") and p["slug"] != "all"]
            if not all_projs:
                all_projs = [DEFAULT_PROJECT]
            created_msgs = [
                self.create_message(
                    sender=sender,
                    recipient=recipient,
                    subject=subject,
                    body=body,
                    file_path=file_path,
                    project=p_slug,
                    attachments=attachments,
                    is_private=is_private,
                    is_interrupt=is_interrupt,
                    allow_peers=allow_peers,
                    comm_rules=comm_rules,
                    interaction_type=interaction_type,
                )
                for p_slug in all_projs
            ]
            first = dict(created_msgs[0])
            first["broadcast_projects"] = all_projs
            first["broadcast_message_ids"] = [m["id"] for m in created_msgs]
            return first

        sender = sanitize_slug(sender, default="")
        if not sender:
            raise ValueError("sender is required")

        if parent_msg is not None:
            if not (recipient or "").strip():
                recipient = (
                    parent_msg["recipient"]
                    if sender == parent_msg["sender"]
                    else parent_msg["sender"]
                )
            if not (subject or "").strip():
                p_subj = (parent_msg.get("subject") or "").strip()
                subject = (
                    p_subj
                    if re.match(r"^\s*re\s*:", p_subj, re.IGNORECASE)
                    else f"Re: {p_subj}"
                )
            if is_private is None:
                is_private = bool(parent_msg.get("is_private", False))

        recipient = sanitize_slug(recipient, default="")
        project = sanitize_slug(project, default=DEFAULT_PROJECT)
        if not recipient:
            raise ValueError("recipient is required")

        subject = (subject or "").strip()
        if not subject:
            raise ValueError("subject is required")

        is_priv_int = 1 if (bool(is_private) and recipient != "all") else 0
        cc_val = "" if (is_priv_int or recipient == "all") else "all"
        is_int_val = 1 if bool(is_interrupt) else 0
        if isinstance(allow_peers, list):
            peers_list = [sanitize_slug(str(p), default="") for p in allow_peers if sanitize_slug(str(p), default="")]
        elif isinstance(allow_peers, str):
            peers_list = [sanitize_slug(p, default="") for p in allow_peers.split(",") if sanitize_slug(p, default="")]
        else:
            peers_list = []
        allow_peers_str = ",".join(peers_list)
        comm_rules_str = str(comm_rules or "").strip()
        itype_str = str(interaction_type or "").strip().lower()
        if itype_str not in VALID_INTERACTION_TYPES:
            raise ValueError(f"Invalid interaction_type '{itype_str}'. Must be one of {sorted(VALID_INTERACTION_TYPES)}")
        istatus_str = "pending" if itype_str in ("approval", "question") else ""

        normalized_attachments: List[Dict[str, Any]] = []
        for att in attachments or []:
            if not isinstance(att, dict):
                continue
            att_path = str(att.get("path") or "").strip()
            att_name = str(att.get("filename") or os.path.basename(att_path) or "attachment").strip()
            # The dashboard links to this url, so it may only point at our own attachment route.
            att_url = str(att.get("url") or "").strip()
            if not att_url.startswith("/v1/attachments/"):
                att_url = f"/v1/attachments/{urllib.parse.quote(os.path.basename(att_path))}" if att_path else ""
            normalized_attachments.append(
                {
                    "filename": att_name,
                    "path": att_path,
                    "url": att_url,
                    "size": int(att.get("size") or 0),
                    "content_type": str(att.get("content_type") or "application/octet-stream"),
                }
            )

        self.ensure_project(project)
        now_iso = utc_now_iso()
        msg_uuid = str(uuid.uuid4())

        # Format ### Attachments block if attachments are present and not already listed
        att_md_block = ""
        if normalized_attachments:
            lines = ["### Attachments"]
            for a in normalized_attachments:
                size_kb = f"{a['size'] / 1024:.1f} KB" if a.get("size") else ""
                meta = f" ({a['content_type']}{', ' + size_kb if size_kb else ''})" if a.get("content_type") else ""
                lines.append(f"- **{a['filename']}**{meta}: `{a['path']}`")
            att_md_block = "\n".join(lines)

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
            if att_md_block and "### Attachments" not in body:
                body = f"{body.rstrip()}\n\n{att_md_block}"
        else:
            if (body is None or not str(body).strip()) and not normalized_attachments:
                raise ValueError("Either body, file_path, or attachments must be provided")
            body = str(body or "").strip()
            if att_md_block and "### Attachments" not in body:
                body = f"{body}\n\n{att_md_block}".strip()
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
            cc_line = f"- **CC:** `{cc_val}`\n" if cc_val else ("- **Visibility:** `private`\n" if is_priv_int else "")
            thread_line = (
                f"- **Thread:** `#{resolved_thread_id}` (reply to `#{resolved_reply_to}`)\n"
                if resolved_reply_to is not None
                else ""
            )
            int_line = "- **Priority:** `INTERRUPT (urgent mid-turn)`\n" if is_int_val else ""
            peers_line = f"- **Allowed Peers:** `{allow_peers_str}`\n" if allow_peers_str else ""
            rules_line = f"- **Comm Rules:** {comm_rules_str}\n" if comm_rules_str else ""
            interact_line = f"- **Interaction Required:** `{itype_str}` (`{istatus_str}`)\n" if itype_str else ""
            md_content = (
                f"# {subject}\n\n"
                f"- **Project:** `{project}`\n"
                f"- **From:** `{sender}`\n"
                f"- **To:** `{recipient}`\n"
                f"{cc_line}"
                f"{thread_line}"
                f"{int_line}"
                f"{peers_line}"
                f"{rules_line}"
                f"{interact_line}"
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
                    INSERT INTO messages (
                        msg_uuid, project, sender, recipient, subject, body, file_path,
                        attachments, is_private, thread_id, reply_to_id,
                        delivery_state, is_interrupt, allow_peers, comm_rules,
                        interaction_type, interaction_status, interaction_response,
                        ack_by, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        msg_uuid,
                        project,
                        sender,
                        recipient,
                        subject,
                        body,
                        str(resolved_file_path),
                        json.dumps(normalized_attachments),
                        is_priv_int,
                        resolved_thread_id,
                        resolved_reply_to,
                        "queued",
                        is_int_val,
                        allow_peers_str,
                        comm_rules_str,
                        itype_str,
                        istatus_str,
                        "",
                        "[]",
                        now_iso,
                    ),
                )
                msg_id = cur.lastrowid
                if resolved_thread_id is None:
                    conn.execute(
                        "UPDATE messages SET thread_id = ? WHERE id = ?",
                        (msg_id, msg_id),
                    )
                conn.commit()
                row = conn.execute(
                    "SELECT * FROM messages WHERE id = ?", (msg_id,)
                ).fetchone()
                msg = self._row_to_message(row)
            finally:
                conn.close()

        if (
            resolved_reply_to is not None
            and parent_msg is not None
            and sender != parent_msg["sender"]
            and sender not in (parent_msg.get("ack_by") or [])
        ):
            try:
                self.ack_message(resolved_reply_to, sender)
            except Exception:
                pass

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
        thread_id: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        norm_agent = agent.strip().lower() if agent else None
        with self.db_lock:
            conn = self._connect()
            try:
                if thread_id is not None:
                    rows = conn.execute(
                        "SELECT * FROM messages WHERE COALESCE(thread_id, id) = ? ORDER BY id ASC LIMIT ?",
                        (int(thread_id), limit),
                    ).fetchall()
                else:
                    # A first load (since_id 0) gets the NEWEST `limit` messages, still oldest-first;
                    # an incremental read (since_id > 0) continues forward from that id.
                    order = "DESC" if since_id <= 0 else "ASC"
                    if project and project.strip().lower() not in ("", "all"):
                        proj_slug = sanitize_slug(project)
                        rows = conn.execute(
                            f"SELECT * FROM messages WHERE id > ? AND project = ? ORDER BY id {order} LIMIT ?",
                            (since_id, proj_slug, limit),
                        ).fetchall()
                    else:
                        rows = conn.execute(
                            f"SELECT * FROM messages WHERE id > ? ORDER BY id {order} LIMIT ?",
                            (since_id, limit),
                        ).fetchall()
                    if order == "DESC":
                        rows = list(reversed(rows))

                    # Ensure active threads always include their root message even if older than the limit window.
                    if since_id <= 0 and not unread_only and rows:
                        present_ids = {r["id"] for r in rows}
                        missing_roots = sorted(
                            {
                                int(r["thread_id"])
                                for r in rows
                                if r["reply_to_id"] is not None
                                and r["thread_id"] is not None
                                and int(r["thread_id"]) not in present_ids
                            }
                        )
                        if missing_roots:
                            placeholders = ",".join("?" for _ in missing_roots)
                            root_rows = conn.execute(
                                f"SELECT * FROM messages WHERE id IN ({placeholders}) ORDER BY id ASC",
                                tuple(missing_roots),
                            ).fetchall()
                            rows = list(root_rows) + list(rows)
                            rows.sort(key=lambda r: r["id"])

                messages = [self._row_to_message(r, viewer_agent=norm_agent) for r in rows]
            finally:
                conn.close()

        if norm_agent:
            if norm_agent != "all":
                filtered = []
                for m in messages:
                    is_for_agent = m["recipient"] == norm_agent or (
                        (m["recipient"] == "all" or not m.get("is_private", False))
                        and m["sender"] != norm_agent
                    )
                    if unread_only:
                        if is_for_agent and norm_agent not in m["ack_by"]:
                            filtered.append(m)
                    else:
                        if is_for_agent or m["sender"] == norm_agent:
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
                messages = [self._row_to_message(r, viewer_agent=agent) for r in rows]
            finally:
                conn.close()

        # Unread messages visible to `agent`: addressed to `agent`, to `all`, or Auto-CC (non-private).
        visible_unread = []
        has_wake_trigger = False
        for m in messages:
            if m["sender"] == agent or agent in m["ack_by"]:
                continue
            is_visible = (
                m["recipient"] == agent
                or m["recipient"] == "all"
                or not m.get("is_private", False)
            )
            if not is_visible:
                continue
            visible_unread.append(m)
            # Wake only on messages addressed to the agent, to 'all', sent by a person, or marked interrupt.
            # Unread FYI messages ride along in the same batch when woken.
            if (
                m["recipient"] == agent
                or m["recipient"] == "all"
                or m["sender"] in self.humans
                or m.get("is_interrupt")
            ):
                has_wake_trigger = True

        if not has_wake_trigger:
            return []
        visible_unread.sort(key=lambda x: (0 if x.get("is_interrupt") else 1, x["id"]))
        return visible_unread

    def ack_message(
        self, msg_id: int, agent: str, viewer_agent: Optional[str] = None
    ) -> Dict[str, Any]:
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
                new_dstate = msg.get("delivery_state") or "queued"
                if new_dstate in ("queued", "submitted"):
                    new_dstate = "delivered"
                if agent not in ack_by or new_dstate != row["delivery_state"]:
                    if agent not in ack_by:
                        ack_by.append(agent)
                    conn.execute(
                        "UPDATE messages SET ack_by = ?, delivery_state = ? WHERE id = ?",
                        (json.dumps(ack_by), new_dstate, msg_id),
                    )
                    conn.commit()
                row = conn.execute(
                    "SELECT * FROM messages WHERE id = ?", (msg_id,)
                ).fetchone()
                sse_updated = self._row_to_message(row)
                updated = self._row_to_message(row, viewer_agent=viewer_agent)
            finally:
                conn.close()

        self.broadcast_sse("message", sse_updated)
        return updated

    def update_delivery_state(
        self, msg_id: int, delivery_state: str
    ) -> Dict[str, Any]:
        dstate = (delivery_state or "").strip().lower()
        if dstate not in VALID_DELIVERY_STATES:
            raise ValueError(
                f"Invalid delivery_state '{dstate}'. Must be one of {sorted(VALID_DELIVERY_STATES)}"
            )
        with self.db_lock:
            conn = self._connect()
            try:
                row = conn.execute(
                    "SELECT * FROM messages WHERE id = ?", (msg_id,)
                ).fetchone()
                if not row:
                    raise KeyError(f"Message id {msg_id} not found")
                conn.execute(
                    "UPDATE messages SET delivery_state = ? WHERE id = ?",
                    (dstate, msg_id),
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

    def resolve_interaction(
        self,
        msg_id: int,
        action: str,
        responder: str,
        response_text: str = "",
        create_reply: bool = True,
    ) -> Dict[str, Any]:
        responder = sanitize_slug(responder, default="")
        if not responder:
            raise ValueError("Valid responder name is required")
        act = (action or "").strip().lower()
        action_map = {
            "approve": "approved",
            "approved": "approved",
            "deny": "denied",
            "denied": "denied",
            "answer": "answered",
            "answered": "answered",
        }
        if act not in action_map:
            raise ValueError(
                "Invalid interaction action. Must be one of: approve, deny, answer"
            )
        resolved_status = action_map[act]
        now_iso = utc_now_iso()
        resp_obj = {
            "by": responder,
            "status": resolved_status,
            "text": (response_text or "").strip(),
            "at": now_iso,
        }
        with self.db_lock:
            conn = self._connect()
            try:
                row = conn.execute(
                    "SELECT * FROM messages WHERE id = ?", (msg_id,)
                ).fetchone()
                if not row:
                    raise KeyError(f"Message id {msg_id} not found")
                parent_msg = self._row_to_message(row)
                ack_by = list(parent_msg["ack_by"])
                if responder not in ack_by:
                    ack_by.append(responder)
                itype = parent_msg.get("interaction_type") or (
                    "question" if resolved_status == "answered" else "approval"
                )
                conn.execute(
                    """
                    UPDATE messages
                    SET interaction_type = ?,
                        interaction_status = ?,
                        interaction_response = ?,
                        delivery_state = 'delivered',
                        ack_by = ?
                    WHERE id = ?
                    """,
                    (
                        itype,
                        resolved_status,
                        json.dumps(resp_obj),
                        json.dumps(ack_by),
                        msg_id,
                    ),
                )
                conn.commit()
                row = conn.execute(
                    "SELECT * FROM messages WHERE id = ?", (msg_id,)
                ).fetchone()
                updated = self._row_to_message(row)
            finally:
                conn.close()

        self.broadcast_sse("message", updated)

        if create_reply and responder != updated["sender"]:
            badge = {
                "approved": "✅ APPROVED",
                "denied": "❌ DENIED",
                "answered": "💬 ANSWERED",
            }[resolved_status]
            note = f"\n\n{resp_obj['text']}" if resp_obj["text"] else ""
            reply_body = f"**{badge}** by `{responder}` on message `#{msg_id}`.{note}"
            reply_msg = self.create_message(
                sender=responder,
                recipient=updated["sender"],
                body=reply_body,
                project=updated["project"],
                reply_to_id=msg_id,
                is_interrupt=True,
            )
            updated["reply_message"] = reply_msg

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
            "thread_id": msg["thread_id"],
            "reply_to_id": msg["reply_to_id"],
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

    def handle_error(self, request, client_address):
        # A client that leaves during a long wait (Ctrl+C on wait or listen) is normal, not an error.
        if isinstance(sys.exc_info()[1], (ConnectionResetError, ConnectionAbortedError, BrokenPipeError)):
            return
        super().handle_error(request, client_address)


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

    def _send_asset(self, rel_path: str) -> None:
        # Image files under assets/ only (logo, backgrounds); anything else is not found.
        root = ASSETS_DIR.resolve()
        target = (root / rel_path).resolve()
        if not target.is_relative_to(root) or not target.is_file() or target.suffix.lower() not in ASSET_TYPES:
            self._send_json(404, {"error": "Not found"})
            return
        body = target.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", ASSET_TYPES[target.suffix.lower()])
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "max-age=3600")
        self.end_headers()
        self.wfile.write(body)

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

            if path.startswith("/assets/"):
                self._send_asset(urllib.parse.unquote(path[len("/assets/"):]))
                return

            if path.startswith("/v1/attachments/"):
                saved_name = urllib.parse.unquote(path[len("/v1/attachments/"):])
                target = state.get_attachment_path(saved_name)
                if not target:
                    self._send_json(404, {"error": f"Attachment not found: {saved_name}"})
                    return
                body = target.read_bytes()
                ctype, _ = mimetypes.guess_type(target.name)
                inline = ctype in INLINE_ATTACHMENT_TYPES
                self.send_response(200)
                self.send_header("Content-Type", ctype if inline else "application/octet-stream")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "max-age=3600")
                self.send_header("X-Content-Type-Options", "nosniff")
                if not inline:
                    self.send_header(
                        "Content-Disposition",
                        f'attachment; filename="{target.name}"',
                    )
                self.end_headers()
                self.wfile.write(body)
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
                raw_thread = qs.get("thread_id", [None])[0]
                thread_id = int(raw_thread) if raw_thread else None
                msgs = state.list_messages(
                    agent=agent,
                    unread_only=unread,
                    since_id=since_id,
                    limit=limit,
                    project=project,
                    thread_id=thread_id,
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
                        acked.append(state.ack_message(m["id"], agent, viewer_agent=agent))
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

            if path == "/v1/agents/config":
                # API agents from agents.json: names of key variables and whether they are set, never keys.
                import agent_runner
                self._send_json(200, agent_runner.public_config())
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
        qs = urllib.parse.parse_qs(parsed.query)
        state: RelayState = self.server.relay_state  # type: ignore

        if not self._same_origin():
            self._send_json(403, {"error": "Cross-origin request refused"})
            return

        if path == "/v1/attachments":
            try:
                ctype_hdr = (self.headers.get("Content-Type") or "").lower()
                length = int(self.headers.get("Content-Length", "0") or "0")
                if length <= 0:
                    self._send_json(400, {"error": "Empty attachment payload"})
                    return
                if length > MAX_ATTACHMENT_BYTES * 4 // 3 + 4096:  # base64 is a third bigger
                    self.close_connection = True  # the body is left unread
                    self._send_json(413, {"error": f"Attachment too large (limit {MAX_ATTACHMENT_BYTES // (1024 * 1024)} MB)"})
                    return
                raw_body = self.rfile.read(length)
                if "application/json" in ctype_hdr:
                    data = json.loads(raw_body.decode("utf-8"))
                    filename = data.get("filename") or "attachment.bin"
                    content_type = data.get("content_type")
                    project = data.get("project") or DEFAULT_PROJECT
                    sender = data.get("sender") or "human"
                    b64_str = data.get("data_base64") or ""
                    if "," in b64_str and b64_str.lstrip().startswith("data:"):
                        b64_str = b64_str.split(",", 1)[1]
                    file_bytes = base64.b64decode(b64_str)
                else:
                    filename = (
                        qs.get("filename", [None])[0]
                        or self.headers.get("X-Filename")
                        or "attachment.bin"
                    )
                    content_type = ctype_hdr or None
                    project = qs.get("project", [DEFAULT_PROJECT])[0]
                    sender = qs.get("sender", ["human"])[0]
                    file_bytes = raw_body

                if len(file_bytes) > MAX_ATTACHMENT_BYTES:
                    self._send_json(413, {"error": f"Attachment too large (limit {MAX_ATTACHMENT_BYTES // (1024 * 1024)} MB)"})
                    return
                att = state.save_attachment(
                    filename=filename,
                    raw_bytes=file_bytes,
                    content_type=content_type,
                    project=project,
                    sender=sender,
                )
                self._send_json(201, {**att, "attachment": att})
            except Exception as e:
                self._send_json(400, {"error": f"Failed to save attachment: {e}"})
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

            if path in ("/v1/messages", "/v1/reply") or re.match(r"^/v1/messages/\d+/reply$", path):
                m_rep = re.match(r"^/v1/messages/(\d+)/reply$", path)
                raw_reply_to = (
                    m_rep.group(1)
                    if m_rep
                    else (
                        data.get("reply_to_id")
                        or data.get("parent_id")
                        or (data.get("id") if path == "/v1/reply" else None)
                    )
                )
                reply_to_id = int(raw_reply_to) if raw_reply_to is not None and str(raw_reply_to).strip() != "" else None
                sender = data.get("sender") or data.get("from") or ""
                recipient = data.get("recipient") or data.get("to") or ""
                subject = data.get("subject") or ""
                body = data.get("body")
                file_path = data.get("file_path") or data.get("file")
                project = data.get("project") or DEFAULT_PROJECT
                attachments = data.get("attachments") or []
                is_private = (
                    bool(data.get("is_private") or data.get("private"))
                    if ("is_private" in data or "private" in data)
                    else (None if reply_to_id is not None else False)
                )
                is_interrupt = bool(data.get("is_interrupt") or data.get("interrupt"))
                allow_peers = data.get("allow_peers") or ""
                comm_rules = str(data.get("comm_rules") or data.get("communication_rules") or "")
                interaction_type = str(data.get("interaction_type") or "")
                msg = state.create_message(
                    sender=sender,
                    recipient=recipient,
                    subject=subject,
                    body=body,
                    file_path=file_path,
                    project=project,
                    attachments=attachments,
                    is_private=is_private,
                    reply_to_id=reply_to_id,
                    is_interrupt=is_interrupt,
                    allow_peers=allow_peers,
                    comm_rules=comm_rules,
                    interaction_type=interaction_type,
                )
                self._send_json(201, {"message": msg})
                return

            if path == "/v1/delivery" or re.match(r"^/v1/messages/\d+/delivery$", path):
                m_del = re.match(r"^/v1/messages/(\d+)/delivery$", path)
                msg_id = int(m_del.group(1)) if m_del else int(data.get("id", 0))
                dstate = str(data.get("delivery_state") or data.get("state") or "")
                if not msg_id or not dstate:
                    self._send_json(400, {"error": "Both 'id' and 'delivery_state' are required"})
                    return
                updated = state.update_delivery_state(msg_id=msg_id, delivery_state=dstate)
                self._send_json(200, {"message": updated})
                return

            if path == "/v1/interact" or re.match(r"^/v1/messages/\d+/interact$", path):
                m_int = re.match(r"^/v1/messages/(\d+)/interact$", path)
                msg_id = int(m_int.group(1)) if m_int else int(data.get("id", 0))
                action = str(data.get("action") or data.get("status") or "")
                responder = str(data.get("responder") or data.get("by") or data.get("agent") or "human")
                response_text = str(data.get("response") or data.get("text") or "")
                create_reply = bool(data.get("create_reply", True))
                if not msg_id or not action:
                    self._send_json(400, {"error": "Both 'id' and 'action' (approve|deny|answer) are required"})
                    return
                updated = state.resolve_interaction(
                    msg_id=msg_id,
                    action=action,
                    responder=responder,
                    response_text=response_text,
                    create_reply=create_reply,
                )
                self._send_json(200, {"message": updated})
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
                # An explicit "" means "no commit" (API agents); only a missing field keeps the old value.
                commit_sha = data["commit_sha"] if "commit_sha" in data else data.get("commit")
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

            if path == "/v1/agents/test":
                # One tiny model request with the key from this server's environment; errors come back redacted.
                import agent_runner
                self._send_json(200, agent_runner.check_connection(str(data.get("name") or "")))
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
    humans: Optional[str] = None,
) -> RelayHTTPServer:
    if host not in ("127.0.0.1", "localhost", "0.0.0.0"):
        raise ValueError("Host must be localhost/127.0.0.1 (or 0.0.0.0 inside container)")
    state = RelayState(
        db_path=db_path or default_db_path(),
        handoffs_dir=handoffs_dir or default_handoffs_dir(),
        humans=humans,
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
    parser.add_argument(
        "--humans",
        default=None,
        help="Comma-separated sender names that count as a person, who always wake waiting agents "
        f"(default: $RELAY_HUMANS or {','.join(DEFAULT_HUMANS)})",
    )
    args = parser.parse_args()
    db_path = Path(args.db) if args.db else None
    handoffs_dir = Path(args.handoffs_dir) if args.handoffs_dir else None

    port = args.port
    try:
        srv = create_server(
            host=args.host, port=port, db_path=db_path, handoffs_dir=handoffs_dir, humans=args.humans
        )
    except OSError:
        if port == 8787:
            port = 8788
            srv = create_server(
                host=args.host, port=port, db_path=db_path, handoffs_dir=handoffs_dir, humans=args.humans
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
