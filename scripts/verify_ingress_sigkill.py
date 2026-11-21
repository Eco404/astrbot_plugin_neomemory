"""Kill isolated synthetic writers at durable ingress/source commit boundaries.

No Provider, host process, or existing plugin database is used. SIGKILL leaves
SQLite connections open; a separate recovery process uses the production stores
and IngressService. This verifies process crashes, not loss of machine power.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import platform
import signal
import sqlite3
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.identity import EventIdentity  # noqa: E402
from core.ingress import IngressService  # noqa: E402
from core.source_ingest import CapturedSource  # noqa: E402
from storage.ingress_store import IngressStore  # noqa: E402
from storage.source_store import SourceStore  # noqa: E402

CASES = ("ingress_committed", "source_committed_before_ack")


def synthetic_source(number: int) -> CapturedSource:
    identity = EventIdentity(
        platform_instance_id="synthetic-crash-probe", platform_name="synthetic",
        message_type="FriendMessage", unified_msg_origin="synthetic:FriendMessage:member",
        session_id="member", group_id="", account_id="member", account_name="Synthetic",
        self_id="synthetic-agent", message_id=f"message-{number}",
    )
    return CapturedSource(identity=identity, actor_kind="user",
        content_text=f"Independent synthetic ingress message {number}.", observed_at=1000 + number,
        message_id=identity.message_id, actor_account_id=identity.account_id,
        actor_account_name=identity.account_name, persona_id="synthetic-persona")


def emit(value):
    print(json.dumps(value, ensure_ascii=False), flush=True)


async def open_stores(directory):
    pending, sources = IngressStore(directory / "ingress.db"), SourceStore(directory / "source.db")
    await pending.open()
    await sources.open()
    return pending, sources


async def writer(directory, case, lease_seconds):
    pending, sources = await open_stores(directory)
    await pending.enqueue(synthetic_source(1))
    await pending.enqueue(synthetic_source(2))

    async def boundary():
        health = await pending.health()
        row = await pending.database.fetchone("SELECT lease_until FROM ingress_records ORDER BY arrival_no LIMIT 1")
        emit({"boundary": case, "pid": os.getpid(), "pending_count": health["pending_count"],
              "source_count": (await sources.health())["source_count"], "lease_until": row["lease_until"],
              "journal_modes": {"ingress": (await pending.database.fetchone("PRAGMA journal_mode"))[0],
                                "source": (await sources.database.fetchone("PRAGMA journal_mode"))[0]},
              "synchronous": {"ingress": (await pending.database.fetchone("PRAGMA synchronous"))[0],
                              "source": (await sources.database.fetchone("PRAGMA synchronous"))[0]}})
        await asyncio.Event().wait()

    if case == "ingress_committed":
        await boundary()
    else:
        claim = await pending.claim(lease_seconds=lease_seconds)
        assert claim and claim.source_id == synthetic_source(1).source_id
        original_capture = sources.capture

        async def capture_then_wait(source):
            await original_capture(source)
            await boundary()

        sources.capture = capture_then_wait
        service = IngressService(pending, sources, SimpleNamespace())
        await service._capture(claim)
    raise AssertionError("Writer must be killed at its declared boundary")


async def recovery(directory, case):
    pending, sources = await open_stores(directory)
    service = IngressService(pending, sources, SimpleNamespace())
    try:
        before = {"pending_count": (await pending.health())["pending_count"],
                  "source_count": (await sources.health())["source_count"]}
        assert before == {"pending_count": 2, "source_count": int(case == "source_committed_before_ack")}
        pending_rows = await pending.list()
        lease_checked = None
        if case == "source_committed_before_ack":
            row = await pending.database.fetchone("SELECT lease_until FROM ingress_records WHERE state='running'")
            assert row and row["lease_until"] > time.time(), "Recovery must begin before the live lease expires"
            assert await service.process_once() is None
            assert (await sources.health())["source_count"] == 1
            lease_checked = True
            await asyncio.sleep(max(0, row["lease_until"] - time.time()) + .05)
        first, second = await service.process_once(), await service.process_once()
        assert first and second
        assert (first.source_id, second.source_id) == (synthetic_source(1).source_id, synthetic_source(2).source_id)
        assert (first.sequence_no, second.sequence_no) == (1, 2)
        assert first.inserted == (case == "ingress_committed")
        assert second.inserted
        assert await service.process_once() is None
        assert (await pending.health())["pending_count"] == 0
        records = await sources.list_records()
        assert len(records) == 2
        assert {record["content_text"] for record in records} == {synthetic_source(number).content_text for number in (1, 2)}
        checks = {}
        for name, store in (("ingress", pending), ("source", sources)):
            checks[name] = {"integrity": [row[0] for row in await store.database.fetchall("PRAGMA integrity_check")],
                            "foreign_key_violations": len(await store.database.fetchall("PRAGMA foreign_key_check"))}
            assert checks[name] == {"integrity": ["ok"], "foreign_key_violations": 0}
        return {"ok": True, "pid": os.getpid(), "before": before,
                "pending_states_before": [row["state"] for row in pending_rows],
                "unexpired_lease_blocks_takeover": lease_checked,
                "first_capture_inserted": first.inserted, "second_capture_inserted": second.inserted,
                "source_sequence_numbers": [first.sequence_no, second.sequence_no],
                "final_source_count": len(records), "final_pending_count": 0,
                "contents_preserved": True, "checks": checks}
    finally:
        await service.stop()
        await sources.close()
        await pending.close()


async def subprocess_result(*args, timeout=15):
    process = await asyncio.create_subprocess_exec(sys.executable, str(Path(__file__).resolve()), *map(str, args),
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout)
    except BaseException:
        if process.returncode is None:
            process.kill()
        await process.wait()
        raise
    if process.returncode:
        raise RuntimeError(f"Probe subprocess exited {process.returncode}: {stderr.decode()}")
    return json.loads(stdout)


async def run(directory, repeats, lease_seconds):
    directory.mkdir(parents=True, exist_ok=False)
    report = {"tested_at": datetime.now(ZoneInfo("Asia/Shanghai")).isoformat(timespec="seconds"),
              "git_head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
              "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              "python": platform.python_version(), "sqlite": sqlite3.sqlite_version,
              "platform": platform.platform(), "scope": "synthetic_process_sigkill_not_power_loss",
              "test_lease_seconds": lease_seconds, "repeats": repeats,
              "http_requests": 0, "cases": []}
    report_path = directory / "report.json"
    try:
        for case in CASES:
            for iteration in range(1, repeats + 1):
                case_dir = directory / f"{case}-{iteration}"
                case_dir.mkdir()
                process = await asyncio.create_subprocess_exec(sys.executable, str(Path(__file__).resolve()),
                    "--worker", "write", "--case", case, "--output", str(case_dir), "--lease-seconds", str(lease_seconds),
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                try:
                    line = await asyncio.wait_for(process.stdout.readline(), 15)
                    if not line:
                        await process.wait()
                        raise RuntimeError((await process.stderr.read()).decode())
                    barrier = json.loads(line)
                    assert barrier["boundary"] == case and barrier["pending_count"] == 2
                    wal_sizes = {name: (case_dir / (name + ".db-wal")).stat().st_size for name in ("ingress", "source")}
                    assert all(wal_sizes.values())
                    process.send_signal(signal.SIGKILL)
                    await asyncio.wait_for(process.wait(), 5)
                    assert process.returncode == -signal.SIGKILL
                    result = await subprocess_result("--worker", "recover", "--case", case,
                        "--output", case_dir, timeout=lease_seconds + 15)
                    assert result["pid"] != barrier["pid"]
                    entry = {"case": case, "iteration": iteration, "barrier": barrier,
                             "writer_exit_code": process.returncode, "wal_bytes_at_kill": wal_sizes,
                             "recovery": result}
                    report["cases"].append(entry)
                    emit({"case": case, "iteration": iteration, "ok": result["ok"]})
                finally:
                    if process.returncode is None:
                        process.kill()
                        await process.wait()
        report["ok"] = True
    finally:
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="New isolated output directory")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--lease-seconds", type=float, default=3)
    parser.add_argument("--worker", choices=("write", "recover"), help=argparse.SUPPRESS)
    parser.add_argument("--case", choices=CASES, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.repeats < 1 or not 0 < args.lease_seconds <= 30:
        parser.error("repeats must be positive; test lease must be in (0,30] seconds")
    if args.worker == "write":
        asyncio.run(writer(args.output.resolve(), args.case, args.lease_seconds))
    elif args.worker == "recover":
        emit(asyncio.run(recovery(args.output.resolve(), args.case)))
    else:
        result = asyncio.run(run(args.output.resolve(), args.repeats, args.lease_seconds))
        emit({"ok": result["ok"], "case_count": len(result["cases"]), "report": str(args.output / "report.json")})


if __name__ == "__main__":
    main()
