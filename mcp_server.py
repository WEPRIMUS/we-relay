#!/usr/bin/env python3
"""
WE Relay MCP Server (mcp_server.py)
Pure Python stdlib Model Context Protocol (MCP) server over stdio.
Works locally (zero-config) or remotely (set RELAY_URL + RELAY_TOKEN) for
Claude Code, OpenAI Codex, Antigravity, Cursor, and any MCP client.
"""

import json
import sys
import urllib.parse
from typing import Any, Dict, List, Optional

import cli

PROTOCOL_VERSION = "2024-11-05"
SERVER_INFO = {"name": "we-relay", "version": "0.4.0"}

TOOLS: List[Dict[str, Any]] = [
    {
        "name": "relay_projects",
        "description": "List all projects on WE Relay, or create/update a project.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "add": {"type": "string", "description": "Optional slug to create/update"},
                "name": {"type": "string", "description": "Display name when creating a project"},
                "description": {"type": "string", "description": "Project description"},
            },
        },
    },
    {
        "name": "relay_status",
        "description": "View agent statuses in a project, or update an agent's live status.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "project": {"type": "string", "description": "Project slug or 'all' (default: default)"},
                "agent": {"type": "string", "description": "Agent name when updating status"},
                "state": {
                    "type": "string",
                    "enum": ["working", "waiting_for_review", "frozen", "blocked", "done", "idle"],
                    "description": "Omit to list statuses; set to update status",
                },
                "role": {"type": "string"},
                "branch": {"type": "string"},
                "commit": {"type": "string"},
                "detail": {"type": "string"},
            },
        },
    },
    {
        "name": "relay_inbox",
        "description": "Read messages for an agent (optionally unread only or within a thread).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "agent": {"type": "string", "description": "Agent name, e.g. cris, keel, atlas, laptop, or 'all'"},
                "project": {"type": "string", "description": "Project slug or 'all'"},
                "unread": {"type": "boolean", "description": "Only return unacknowledged messages"},
                "since_id": {"type": "integer", "description": "Only messages with id > since_id"},
                "thread_id": {"type": "integer", "description": "Only messages in this thread"},
                "limit": {"type": "integer", "description": "Max messages to return (default 50)"},
            },
            "required": ["agent"],
        },
    },
    {
        "name": "relay_send",
        "description": "Send a message or handoff on WE Relay (auto-mirrors to Markdown on the host).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "sender": {"type": "string", "description": "Your agent name"},
                "recipient": {"type": "string", "description": "Target agent name or 'all'"},
                "subject": {"type": "string", "description": "Concise subject line"},
                "body": {"type": "string", "description": "Markdown message body"},
                "project": {"type": "string", "description": "Project slug (default: default)"},
                "is_private": {"type": "boolean", "description": "True to hide from Auto-CC"},
                "is_interrupt": {"type": "boolean", "description": "True for urgent interrupt priority"},
                "interaction_type": {
                    "type": "string",
                    "enum": ["approval", "question"],
                    "description": "Optional structured approval/question gate",
                },
            },
            "required": ["sender", "recipient", "subject", "body"],
        },
    },
    {
        "name": "relay_reply",
        "description": "Reply in an existing message thread (auto-inherits project, recipient, subject, and ACKs parent).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "reply_to_id": {"type": "integer", "description": "Parent message ID to reply to"},
                "sender": {"type": "string", "description": "Your agent name"},
                "body": {"type": "string", "description": "Markdown reply body"},
                "recipient": {"type": "string", "description": "Optional recipient override"},
                "subject": {"type": "string", "description": "Optional subject override"},
                "is_interrupt": {"type": "boolean"},
            },
            "required": ["reply_to_id", "sender", "body"],
        },
    },
    {
        "name": "relay_ack",
        "description": "Acknowledge a message so it no longer appears in unread inbox.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "id": {"type": "integer", "description": "Message ID"},
                "agent": {"type": "string", "description": "Agent acknowledging"},
            },
            "required": ["id", "agent"],
        },
    },
    {
        "name": "relay_search",
        "description": "Full-text search (SQLite FTS5) across relay messages by keywords or #id.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Keywords or #message_id"},
                "project": {"type": "string", "description": "Optional project filter or 'all'"},
                "agent": {"type": "string", "description": "Optional viewer agent name"},
                "limit": {"type": "integer", "description": "Max results (default 30)"},
            },
            "required": ["query"],
        },
    },
    {
        "name": "relay_interact",
        "description": "Approve, deny, or answer an interaction-required message.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "id": {"type": "integer", "description": "Message ID"},
                "action": {"type": "string", "enum": ["approve", "deny", "answer"]},
                "responder": {"type": "string", "description": "Agent or human name"},
                "response": {"type": "string", "description": "Optional note or answer"},
            },
            "required": ["id", "action", "responder"],
        },
    },
]


def _call_tool(name: str, args: Dict[str, Any], base_url: Optional[str] = None) -> Any:
    url = (base_url or cli._resolve_default_url()).rstrip("/")
    default_proj = cli._default_project()

    if name == "relay_projects":
        if args.get("add"):
            return cli.http_json(
                "POST",
                f"{url}/v1/projects",
                {
                    "slug": args["add"],
                    "name": args.get("name") or args["add"],
                    "description": args.get("description") or "",
                },
            )
        return cli.http_json("GET", f"{url}/v1/projects")

    if name == "relay_status":
        proj = args.get("project") or default_proj
        if not args.get("state"):
            qs = urllib.parse.urlencode({"project": proj})
            return cli.http_json("GET", f"{url}/v1/status?{qs}")
        payload: Dict[str, Any] = {
            "project": proj,
            "agent": args.get("agent") or "agent",
            "state": args["state"],
        }
        for k, dst in (("role", "role"), ("branch", "branch"), ("commit", "commit_sha"), ("detail", "detail")):
            if args.get(k) is not None:
                payload[dst] = args[k]
        return cli.http_json("POST", f"{url}/v1/status", payload)

    if name == "relay_inbox":
        params: Dict[str, str] = {
            "agent": args["agent"],
            "project": args.get("project") or default_proj,
            "limit": str(args.get("limit") or 50),
        }
        if args.get("unread"):
            params["unread"] = "true"
        if args.get("since_id"):
            params["since_id"] = str(args["since_id"])
        if args.get("thread_id"):
            params["thread_id"] = str(args["thread_id"])
        qs = urllib.parse.urlencode(params)
        return cli.http_json("GET", f"{url}/v1/messages?{qs}")

    if name == "relay_send":
        payload = {
            "project": args.get("project") or default_proj,
            "sender": args["sender"],
            "recipient": args["recipient"],
            "subject": args["subject"],
            "body": args["body"],
            "is_private": bool(args.get("is_private", False)),
            "is_interrupt": bool(args.get("is_interrupt", False)),
        }
        if args.get("interaction_type"):
            payload["interaction_type"] = args["interaction_type"]
        return cli.http_json("POST", f"{url}/v1/messages", payload)

    if name == "relay_reply":
        payload = {
            "reply_to_id": int(args["reply_to_id"]),
            "sender": args["sender"],
            "body": args["body"],
            "recipient": args.get("recipient") or "",
            "subject": args.get("subject") or "",
            "is_interrupt": bool(args.get("is_interrupt", False)),
        }
        return cli.http_json("POST", f"{url}/v1/messages", payload)

    if name == "relay_ack":
        return cli.http_json("POST", f"{url}/v1/ack", {"id": int(args["id"]), "agent": args["agent"]})

    if name == "relay_search":
        params = {
            "q": args["query"],
            "project": args.get("project") or "all",
            "limit": str(args.get("limit") or 30),
        }
        if args.get("agent"):
            params["agent"] = args["agent"]
        qs = urllib.parse.urlencode(params)
        return cli.http_json("GET", f"{url}/v1/search?{qs}")

    if name == "relay_interact":
        return cli.http_json(
            "POST",
            f"{url}/v1/interact",
            {
                "id": int(args["id"]),
                "action": args["action"],
                "responder": args["responder"],
                "response": args.get("response") or "",
            },
        )

    raise ValueError(f"Unknown tool: {name}")


def handle_rpc_message(msg: Dict[str, Any], base_url: Optional[str] = None) -> Optional[Dict[str, Any]]:
    method = msg.get("method")
    msg_id = msg.get("id")

    # Notifications have no id and expect no response
    if msg_id is None:
        return None

    if method == "initialize":
        client_proto = (msg.get("params") or {}).get("protocolVersion") or PROTOCOL_VERSION
        return {
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": {
                "protocolVersion": client_proto,
                "capabilities": {"tools": {}},
                "serverInfo": SERVER_INFO,
            },
        }

    if method == "ping":
        return {"jsonrpc": "2.0", "id": msg_id, "result": {}}

    if method == "tools/list":
        return {"jsonrpc": "2.0", "id": msg_id, "result": {"tools": TOOLS}}

    if method == "tools/call":
        params = msg.get("params") or {}
        tool_name = params.get("name") or ""
        tool_args = params.get("arguments") or {}
        try:
            res = _call_tool(tool_name, tool_args, base_url=base_url)
            return {
                "jsonrpc": "2.0",
                "id": msg_id,
                "result": {
                    "content": [{"type": "text", "text": json.dumps(res, indent=2)}],
                    "isError": False,
                },
            }
        except Exception as e:
            return {
                "jsonrpc": "2.0",
                "id": msg_id,
                "result": {
                    "content": [{"type": "text", "text": f"Error: {e}"}],
                    "isError": True,
                },
            }

    return {
        "jsonrpc": "2.0",
        "id": msg_id,
        "error": {"code": -32601, "message": f"Method not found: {method}"},
    }


def serve_stdio() -> int:
    stdin = sys.stdin.buffer
    stdout = sys.stdout.buffer
    framed_mode = False

    while True:
        line = stdin.readline()
        if not line:
            break
        stripped = line.strip()
        if not stripped:
            continue

        if stripped.lower().startswith(b"content-length:"):
            framed_mode = True
            length = int(stripped.split(b":", 1)[1].strip())
            while True:
                hdr = stdin.readline()
                if not hdr or hdr in (b"\r\n", b"\n"):
                    break
            body = stdin.read(length)
            if not body:
                break
            req = json.loads(body.decode("utf-8"))
        else:
            req = json.loads(stripped.decode("utf-8"))

        resp = handle_rpc_message(req)
        if resp is not None:
            out_bytes = json.dumps(resp).encode("utf-8")
            if framed_mode:
                stdout.write(f"Content-Length: {len(out_bytes)}\r\n\r\n".encode("ascii") + out_bytes)
            else:
                stdout.write(out_bytes + b"\n")
            stdout.flush()
    return 0


if __name__ == "__main__":
    sys.exit(serve_stdio())
