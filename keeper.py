#!/usr/bin/env python3
"""Cadence keeper for the RGCIPZAH repost loop.

Why this exists
---------------
GitHub's cron scheduler is effectively dead on this account. Measured here:

  * a public probe repo with a 5-minute cron fired ONCE in 10.5 hours
  * the private worker repo with a 2-hour cron fired ONCE in ~21 hours
  * a second private repo with a 30-minute cron fired ONCE in ~20 hours

So `on: schedule` is a lottery, not a timer, and a 2-hour cadence can never
be built on top of it. The fix is to stop asking cron for the cadence.

This keeper runs inside one long job and keeps itself alive:

    keeper run -> spawn the next keeper run -> poll -> dispatch the private
               worker exactly when a post is due

The successor is spawned at the START of the run (not the end) so a crash
mid-window cannot kill the chain. The successor is told when to begin via
the `not_before` input, which keeps the chain from turning into a hot loop
if runs start failing immediately.

`workflow_dispatch` is reliable on this account - every manual dispatch
succeeded - so the chain does not depend on the flaky scheduler at all.

Why this repo is public
-----------------------
GitHub Actions minutes are free for public repositories. This keeper spends
most of its life asleep; it must not burn the private repo's minutes. The
private repo keeps the actual posting work and all the credentials.

Environment
-----------
WORKER_TOKEN        PAT with access to the private worker repo (repo +
                    workflow scope): reads repost_state.json and dispatches
                    the worker's workflow.
REPO_TOKEN          This repo's own GITHUB_TOKEN (actions:write): spawns the
                    next keeper run and counts the runs already in flight.
KEEPER_NOT_BEFORE   Epoch seconds. The run sleeps until then before working.
"""
from __future__ import annotations

import base64
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

API = "https://api.github.com"
STATE_FILE = "repost_state.json"

WORKER_REPO = os.environ.get("WORKER_REPO", "ryantheeditor28-ops/rgclipzah-loop")
WORKER_WORKFLOW = os.environ.get("WORKER_WORKFLOW", "loop.yml")
WORKER_REF = os.environ.get("WORKER_REF", "master")

KEEPER_REPO = os.environ.get(
    "KEEPER_REPO", os.environ.get("GITHUB_REPOSITORY", "ryantheeditor28-ops/rgclipzah-keeper")
)
KEEPER_WORKFLOW = os.environ.get("KEEPER_WORKFLOW", "keeper.yml")
KEEPER_REF = os.environ.get("KEEPER_REF", "main")

WORKER_TOKEN = os.environ.get("WORKER_TOKEN", "").strip()
REPO_TOKEN = os.environ.get("REPO_TOKEN", "").strip()

POLL_S = int(os.environ.get("KEEPER_POLL_S", "120"))
WINDOW_S = int(os.environ.get("KEEPER_WINDOW_S", str(300 * 60)))
COOLDOWN_S = int(os.environ.get("KEEPER_COOLDOWN_S", "600"))
MAX_CHAIN = int(os.environ.get("KEEPER_MAX_CHAIN", "3"))
CURRENT_RUN_ID = os.environ.get("GITHUB_RUN_ID", "")

LIVE_STATUSES = ("queued", "in_progress", "waiting", "pending", "requested")


def log(msg: str) -> None:
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[keeper {stamp}] {msg}", flush=True)


def api(method: str, path: str, token: str, payload: dict | None = None):
    """Call the GitHub API. Returns (status, parsed_body_or_text)."""
    url = path if path.startswith("http") else API + path
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("X-GitHub-Api-Version", "2022-11-28")
    req.add_header("User-Agent", "rgclipzah-keeper")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
            if not raw:
                return resp.status, None
            try:
                return resp.status, json.loads(raw)
            except json.JSONDecodeError:
                return resp.status, raw.decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", "replace")[:400]
    except Exception as exc:  # noqa: BLE001 - a blip must not kill the keeper
        return 0, str(exc)[:400]


# --------------------------------------------------------------------------
# worker repo
# --------------------------------------------------------------------------

def worker_state() -> dict | None:
    """Read repost_state.json from the private worker repo."""
    status, body = api(
        "GET",
        f"/repos/{WORKER_REPO}/contents/{STATE_FILE}?ref={WORKER_REF}",
        WORKER_TOKEN,
    )
    if status != 200 or not isinstance(body, dict):
        log(f"could not read worker state (HTTP {status}): {body}")
        return None
    try:
        raw = base64.b64decode(body.get("content", ""))
        return json.loads(raw.decode("utf-8"))
    except Exception as exc:  # noqa: BLE001
        log(f"worker state undecodable: {exc}")
        return None


def last_post(state: dict) -> datetime | None:
    """Newest *instant* among posted items.

    Items carry the local offset of whatever machine posted them (+08:00 for
    this PC, UTC for the Actions runner), so the newest post is the newest
    instant - not the biggest ISO string.
    """
    best: datetime | None = None
    for item in state.get("items", []):
        if item.get("status") != "posted":
            continue
        raw = item.get("at") or ""
        if not raw:
            continue
        try:
            when = datetime.fromisoformat(raw)
        except ValueError:
            continue
        if when.tzinfo is None:
            when = when.replace(tzinfo=timezone.utc)
        if best is None or when > best:
            best = when
    return best


def due_state(state: dict | None) -> tuple[bool, str]:
    """Is a post due right now? Mirrors scripts_agency/due_check.py."""
    if state is None:
        return False, "worker state unavailable"
    cfg = state.get("config") or {}
    if not cfg.get("enabled", True):
        return False, "engine disabled in repost_state.json"
    try:
        gap_h = float(cfg.get("min_post_gap_h", 2) or 0)
    except (TypeError, ValueError):
        gap_h = 2.0
    if gap_h <= 0:
        return True, "no post gap configured - always due"
    last = last_post(state)
    if last is None:
        return True, "nothing posted yet - due"
    waited = (datetime.now(timezone.utc) - last).total_seconds() / 3600.0
    if waited >= gap_h:
        return True, f"last post {waited:.2f}h ago, gap {gap_h:g}h - due"
    return False, (f"last post {waited:.2f}h ago, gap {gap_h:g}h - "
                   f"{(gap_h - waited) * 60:.0f} min left")


def dispatch_worker() -> bool:
    status, body = api(
        "POST",
        f"/repos/{WORKER_REPO}/actions/workflows/{WORKER_WORKFLOW}/dispatches",
        WORKER_TOKEN,
        {"ref": WORKER_REF},
    )
    ok = status in (201, 204)
    log(f"dispatch worker -> HTTP {status}{'' if ok else f' {body}'}")
    return ok


# --------------------------------------------------------------------------
# keeper chain
# --------------------------------------------------------------------------

def keeper_runs_live() -> list[dict]:
    """Keeper runs (other than this one) that are queued or running."""
    status, body = api(
        "GET",
        f"/repos/{KEEPER_REPO}/actions/workflows/{KEEPER_WORKFLOW}/runs?per_page=30",
        REPO_TOKEN,
    )
    if status != 200 or not isinstance(body, dict):
        return []
    live = []
    for run in body.get("workflow_runs", []):
        if str(run.get("id")) == str(CURRENT_RUN_ID):
            continue
        if run.get("status") in LIVE_STATUSES:
            live.append(run)
    return live


def spawn_successor(not_before: int) -> bool:
    """Queue the next keeper run. It sleeps until `not_before` before working."""
    status, body = api(
        "POST",
        f"/repos/{KEEPER_REPO}/actions/workflows/{KEEPER_WORKFLOW}/dispatches",
        REPO_TOKEN,
        {"ref": KEEPER_REF, "inputs": {"not_before": str(not_before)}},
    )
    ok = status in (201, 204)
    log(f"spawn successor (not_before={not_before}) -> HTTP {status}"
        f"{'' if ok else f' {body}'}")
    return ok


def sleep_until(epoch: int) -> None:
    while True:
        remaining = epoch - int(time.time())
        if remaining <= 0:
            return
        # wake in chunks so a long sleep still reports progress
        time.sleep(min(remaining, 60))


def main() -> int:
    if not WORKER_TOKEN:
        log("WORKER_TOKEN is not set - cannot talk to the worker repo")
        return 1
    if not REPO_TOKEN:
        log("REPO_TOKEN is not set - cannot maintain the keeper chain")
        return 1

    live = keeper_runs_live()
    if len(live) >= MAX_CHAIN:
        log(f"{len(live)} keeper runs already live - backing off")
        return 0

    not_before = int(os.environ.get("KEEPER_NOT_BEFORE", "0") or 0)
    wait_s = not_before - int(time.time())
    if wait_s > 0:
        log(f"waiting {wait_s // 60} min before the window opens")
        sleep_until(not_before)

    # Spawn the successor FIRST. If this run dies at any point - crash, OOM,
    # runner eviction - the chain still has a successor waiting.
    next_start = int(time.time()) + WINDOW_S
    spawn_successor(next_start)

    log(f"window open for {WINDOW_S // 60} min, polling every {POLL_S}s")
    deadline = time.monotonic() + WINDOW_S
    last_dispatch = 0.0

    while time.monotonic() < deadline:
        state = worker_state()
        due, why = due_state(state)
        now = time.monotonic()
        if due and (last_dispatch == 0.0 or now - last_dispatch >= COOLDOWN_S):
            if dispatch_worker():
                last_dispatch = now
        elif due:
            log(f"due but cooling down ({why})")
        else:
            log(f"not due: {why}")
        time.sleep(POLL_S)

    log("window closed - successor is already queued")
    return 0


if __name__ == "__main__":
    sys.exit(main())
