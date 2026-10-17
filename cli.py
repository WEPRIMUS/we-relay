#!/usr/bin/env python3
"""
WE Relay CLI (cli.py)
Zero-dependency CLI for agents (claude, codex, gemini, human, or any name you choose)
to send messages, check inbox, acknowledge messages, block/wake up on new messages (`wait`),
update status, manage projects, and register webhooks.
"""

import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, Optional

URL_FILE_NAME = ".relay-url"


def _relay_home() -> Path:
    return Path(os.environ.get("RELAY_HOME") or Path.home() / ".we-relay").expanduser()


def _resolve_default_url() -> str:
    if os.environ.get("RELAY_URL"):
        return os.environ["RELAY_URL"]
    url_file = _relay_home() / URL_FILE_NAME
    if url_file.exists():
        try:
            val = url_file.read_text(encoding="utf-8").strip()
            if val:
                return val
        except Exception:
            pass
    return "http://127.0.0.1:8787"


DEFAULT_BASE_URL = _resolve_default_url()
DEFAULT_PROJECT = os.environ.get("RELAY_PROJECT", "default")


def http_json(
    method: str,
    url: str,
    payload: Optional[Dict[str, Any]] = None,
    timeout: float = 30.0,
) -> Dict[str, Any]:
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    headers = {"Accept": "application/json"}
    if data is not None:
        headers["Content-Type"] = "application/json"

    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        err_body = e.read().decode("utf-8", errors="replace")
        try:
            err_json = json.loads(err_body)
            msg = err_json.get("error", err_body)
        except Exception:
            msg = err_body
        raise RuntimeError(f"HTTP {e.code}: {msg}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"Connection failed ({url}): {e.reason}") from e


def format_message_markdown(m: Dict[str, Any]) -> str:
    ack_str = ", ".join(m.get("ack_by", [])) or "none"
    proj = m.get("project", DEFAULT_PROJECT)
    lines = [
        f"================================================================================",
        f"[MESSAGE #{m['id']}] [{proj}] {m['sender'].upper()} -> {m['recipient'].upper()} | {m['created_at']}",
        f"Subject:   {m['subject']}",
        f"File:      {m['file_path']}",
        f"ACKed by:  {ack_str}",
        f"--------------------------------------------------------------------------------",
        m.get("body", "").rstrip(),
        f"================================================================================",
    ]
    return "\n".join(lines)


def cmd_send(args: argparse.Namespace) -> int:
    body = args.body
    file_path = args.file
    if not body and not file_path:
        if not sys.stdin.isatty():
            body = sys.stdin.read()
        else:
            print(
                "Error: Provide --body, --file, or pipe markdown via stdin.",
                file=sys.stderr,
            )
            return 2

    if file_path:
        p = Path(file_path)
        if p.exists():
            file_path = str(p.resolve())

    payload: Dict[str, Any] = {
        "project": args.project,
        "sender": args.sender,
        "recipient": args.to,
        "subject": args.subject,
    }
    if body:
        payload["body"] = body
    if file_path:
        payload["file_path"] = file_path

    res = http_json("POST", f"{args.url}/v1/messages", payload)
    msg = res["message"]
    if args.json:
        print(json.dumps(msg, indent=2))
    else:
        print(
            f"Sent message #{msg['id']} [{msg['project']}] ({msg['sender']} -> {msg['recipient']}): {msg['subject']}\n"
            f"Mirrored file: {msg['file_path']}"
        )
    return 0


def cmd_inbox(args: argparse.Namespace) -> int:
    params = {"agent": args.agent, "project": args.project}
    if args.unread:
        params["unread"] = "true"
    if args.since_id:
        params["since_id"] = str(args.since_id)
    qs = urllib.parse.urlencode(params)
    res = http_json("GET", f"{args.url}/v1/messages?{qs}")
    msgs = res.get("messages", [])
    if args.json:
        print(json.dumps(msgs, indent=2))
        return 0

    if not msgs:
        state_lbl = "unread " if args.unread else ""
        print(f"No {state_lbl}messages for '{args.agent}' in project '{args.project}'.")
        return 0

    for m in msgs:
        print(format_message_markdown(m))
        print()
    return 0


def cmd_ack(args: argparse.Namespace) -> int:
    payload = {"agent": args.agent, "id": args.id}
    res = http_json("POST", f"{args.url}/v1/ack", payload)
    msg = res["message"]
    if args.json:
        print(json.dumps(msg, indent=2))
    else:
        print(
            f"Acknowledged message #{msg['id']} as '{args.agent}' (ack_by={msg['ack_by']})."
        )
    return 0


def cmd_wait(args: argparse.Namespace) -> int:
    params = {
        "agent": args.agent,
        "project": args.project,
        "since_id": str(args.since_id),
        "timeout": str(args.timeout),
        "auto_ack": "true" if args.auto_ack else "false",
    }
    qs = urllib.parse.urlencode(params)
    res = http_json(
        "GET",
        f"{args.url}/v1/wait?{qs}",
        timeout=float(args.timeout) + 15.0,
    )
    msgs = res.get("messages", [])
    timed_out = res.get("timed_out", False)

    if args.json:
        print(json.dumps(res, indent=2))
        return 0 if msgs else 1

    if timed_out or not msgs:
        print(
            f"[wait] Timed out after {args.timeout}s with no new messages for '{args.agent}' (project='{args.project}')."
        )
        return 1

    for m in msgs:
        print(format_message_markdown(m))
        print()
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    if not args.state:
        qs = urllib.parse.urlencode({"project": args.project})
        res = http_json("GET", f"{args.url}/v1/status?{qs}")
        statuses = res.get("statuses", [])
        if args.json:
            print(json.dumps(statuses, indent=2))
        else:
            for s in statuses:
                print(
                    f"[{s['project']}] [{s['agent'].upper():4s}] state={s['state']:18s} "
                    f"branch={s['branch']} ({s['commit_sha']}) | {s['detail']}"
                )
        return 0

    payload: Dict[str, Any] = {
        "project": args.project,
        "agent": args.agent,
        "state": args.state,
    }
    if args.role is not None:
        payload["role"] = args.role
    if args.branch is not None:
        payload["branch"] = args.branch
    if args.commit is not None:
        payload["commit_sha"] = args.commit
    if args.detail is not None:
        payload["detail"] = args.detail

    res = http_json("POST", f"{args.url}/v1/status", payload)
    st = res["status"]
    if args.json:
        print(json.dumps(st, indent=2))
    else:
        print(
            f"Updated status for '{st['agent']}' [{st['project']}]: state={st['state']} "
            f"branch={st['branch']} commit={st['commit_sha']} detail={st['detail']}"
        )
    return 0


def cmd_webhook_add(args: argparse.Namespace) -> int:
    payload = {
        "project": args.project,
        "agent": args.agent,
        "url": args.webhook_url,
        "secret": args.secret or "",
    }
    res = http_json("POST", f"{args.url}/v1/webhooks", payload)
    wh = res["webhook"]
    if args.json:
        print(json.dumps(wh, indent=2))
    else:
        print(
            f"Registered webhook #{wh['id']} for project='{wh['project']}' agent='{wh['agent']}' -> {wh['url']}"
        )
    return 0


def cmd_projects(args: argparse.Namespace) -> int:
    if args.add:
        payload = {
            "slug": args.add,
            "name": args.name or args.add,
            "description": args.description or "",
        }
        res = http_json("POST", f"{args.url}/v1/projects", payload)
        p = res["project"]
        if args.json:
            print(json.dumps(p, indent=2))
        else:
            print(f"Created/Updated project '{p['slug']}' ({p['name']})")
        return 0

    res = http_json("GET", f"{args.url}/v1/projects")
    projects = res.get("projects", [])
    if args.json:
        print(json.dumps(projects, indent=2))
    else:
        for p in projects:
            print(
                f"- {p['slug']:18s} | {p['name']} "
                f"(agents={p.get('agent_count', 0)}, messages={p.get('message_count', 0)})"
            )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="WE Relay CLI"
    )
    parser.add_argument(
        "--url",
        default=DEFAULT_BASE_URL,
        help=f"Relay server URL (default: {DEFAULT_BASE_URL})",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output raw JSON",
    )

    sub = parser.add_subparsers(dest="command", required=True)

    # send
    p_send = sub.add_parser("send", help="Send a message / handoff to an agent or 'all'")
    p_send.add_argument(
        "--project",
        default=DEFAULT_PROJECT,
        help=f"Project slug (default: {DEFAULT_PROJECT})",
    )
    p_send.add_argument("--from", dest="sender", required=True, help="Sender name, e.g. claude")
    p_send.add_argument("--to", required=True, help="Recipient name, e.g. codex, or 'all'")
    p_send.add_argument("--subject", required=True, help="Message subject line")
    p_send.add_argument("--file", help="Path to existing markdown handoff file")
    p_send.add_argument("--body", help="Markdown message body")
    p_send.set_defaults(func=cmd_send)

    # inbox
    p_inbox = sub.add_parser("inbox", help="View messages for an agent")
    p_inbox.add_argument(
        "--project",
        default=DEFAULT_PROJECT,
        help=f"Project slug or 'all' (default: {DEFAULT_PROJECT})",
    )
    p_inbox.add_argument("--agent", required=True, help="Agent name, e.g. claude, or 'all'")
    p_inbox.add_argument("--unread", action="store_true", help="Only show unacknowledged messages")
    p_inbox.add_argument("--since-id", type=int, default=0, help="Only messages with id > since_id")
    p_inbox.set_defaults(func=cmd_inbox)

    # ack
    p_ack = sub.add_parser("ack", help="Acknowledge a message")
    p_ack.add_argument("--agent", required=True, help="Agent acknowledging, e.g. claude")
    p_ack.add_argument("--id", type=int, required=True, help="Message ID to acknowledge")
    p_ack.set_defaults(func=cmd_ack)

    # wait
    p_wait = sub.add_parser(
        "wait",
        help="Block until an unread message arrives for the agent, print it, and exit 0",
    )
    p_wait.add_argument(
        "--project",
        default=DEFAULT_PROJECT,
        help=f"Project slug or 'all' (default: {DEFAULT_PROJECT})",
    )
    p_wait.add_argument("--agent", required=True, help="Agent waiting, e.g. claude")
    p_wait.add_argument("--timeout", type=int, default=600, help="Timeout in seconds (default: 600)")
    p_wait.add_argument("--since-id", type=int, default=0, help="Minimum message ID")
    p_wait.add_argument("--auto-ack", action="store_true", help="Automatically ACK returned messages")
    p_wait.set_defaults(func=cmd_wait)

    # status
    p_status = sub.add_parser("status", help="Update or view agent status")
    p_status.add_argument(
        "--project",
        default=DEFAULT_PROJECT,
        help=f"Project slug or 'all' (default: {DEFAULT_PROJECT})",
    )
    p_status.add_argument("--agent", required=False, default="all", help="Agent name, e.g. claude")
    p_status.add_argument(
        "--state",
        required=False,
        help="State (working|waiting_for_review|frozen|blocked|done|idle)",
    )
    p_status.add_argument("--role", help="Role description")
    p_status.add_argument("--branch", help="Current git branch")
    p_status.add_argument("--commit", help="Current git commit SHA")
    p_status.add_argument("--detail", help="Detailed status message")
    p_status.set_defaults(func=cmd_status)

    # webhook-add
    p_wh = sub.add_parser("webhook-add", help="Register a webhook URL")
    p_wh.add_argument(
        "--project",
        default="all",
        help="Project slug filter or 'all' (default: all)",
    )
    p_wh.add_argument("--agent", required=True, help="Target agent filter, e.g. claude, or 'all'")
    p_wh.add_argument("--url", dest="webhook_url", required=True, help="Webhook HTTP/HTTPS URL")
    p_wh.add_argument("--secret", default="", help="Optional HMAC-SHA256 secret")
    p_wh.set_defaults(func=cmd_webhook_add)

    # projects
    p_proj = sub.add_parser("projects", help="List or create projects")
    p_proj.add_argument("--add", help="Create or update project with this slug")
    p_proj.add_argument("--name", help="Display name for the project")
    p_proj.add_argument("--description", help="Project description")
    p_proj.set_defaults(func=cmd_projects)

    return parser


def main() -> int:
    # Windows consoles default to a legacy code page; print messages as UTF-8 so em dashes etc. survive.
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")
    parser = build_parser()
    args = parser.parse_args()
    try:
        return args.func(args)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
