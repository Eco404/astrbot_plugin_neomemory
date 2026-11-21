"""The frozen evaluation request budget survives races and process restarts."""

import multiprocessing
import os
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import pytest

from scripts.fidelity_budget import BudgetError, RequestLedger, SlotGuard

MANIFEST = "a" * 64


def attempt_in_process(path, operation, arm, case=None):
    """Top-level entry point so independent spawned processes share only SQLite."""
    ledger = RequestLedger(Path(path), MANIFEST)
    try:
        if operation == "claim":
            ledger.claim(arm)
        else:
            ledger.reserve_request(arm, case)
    except BudgetError as exc:
        return type(exc).__name__, str(exc)
    return "admitted", None


def create(tmp_path, arms=("m1-v3", "m1-v5"), cases=("initial", "correction")):
    return RequestLedger.create(tmp_path / "requests.db", MANIFEST, arms, cases)


def test_ledger_permissions_and_creation_does_not_overwrite(tmp_path):
    ledger = create(tmp_path)
    assert os.stat(ledger.path).st_mode & 0o777 == 0o600
    ledger.claim("m1-v3")
    ledger.reserve_request("m1-v3", "initial")
    before = ledger.path.read_bytes()
    with pytest.raises(FileExistsError):
        create(tmp_path)
    assert ledger.path.read_bytes() == before
    assert ledger.counts() == {"m1-v3": 1, "m1-v5": 0}


@pytest.mark.parametrize("operation", ["claim", "reserve"])
def test_only_one_process_can_admit_the_same_arm_or_slot(tmp_path, operation):
    ledger = create(tmp_path)
    if operation == "reserve":
        ledger.claim("m1-v3")
    with ProcessPoolExecutor(max_workers=4, mp_context=multiprocessing.get_context("spawn")) as pool:
        futures = [pool.submit(attempt_in_process, str(ledger.path), operation,
                               "m1-v3", "initial") for _ in range(8)]
        outcomes = [future.result(timeout=15) for future in futures]
    assert sum(result[0] == "admitted" for result in outcomes) == 1
    assert sum(result[0] == "BudgetError" for result in outcomes) == 7
    assert ledger.counts()["m1-v3"] == int(operation == "reserve")


def test_distinct_processes_preserve_all_distinct_slot_reservations(tmp_path):
    arms = ("m1-v3", "m1-v5", "m2-v3", "m2-v5")
    cases = tuple(f"case-{i}" for i in range(8))
    ledger = create(tmp_path, arms, cases)
    for arm in arms:
        ledger.claim(arm)
    with ProcessPoolExecutor(max_workers=4, mp_context=multiprocessing.get_context("spawn")) as pool:
        futures = [pool.submit(attempt_in_process, str(ledger.path), "reserve", arm, case)
                   for arm in arms for case in cases]
        assert all(future.result(timeout=15)[0] == "admitted" for future in futures)
    assert ledger.counts() == dict.fromkeys(arms, 8)
    reopened = RequestLedger(ledger.path, MANIFEST)
    assert sum(reopened.counts().values()) == 32
    with pytest.raises(BudgetError, match="request_slot_unavailable"):
        reopened.reserve_request("m2-v5", "case-7")
    with pytest.raises(BudgetError, match="request_slot_unavailable"):
        reopened.reserve_request("m2-v5", "case-8")
    assert sum(reopened.counts().values()) == 32


def test_reopen_cannot_reclaim_arm_or_refund_uncertain_request(tmp_path):
    ledger = create(tmp_path)
    ledger.claim("m1-v3")
    ledger.reserve_request("m1-v3", "initial")
    # A process may die after reservation and before dispatch. A new instance
    # cannot infer delivery and must retain the conservatively consumed slot.
    reopened = RequestLedger(ledger.path, MANIFEST)
    with pytest.raises(BudgetError, match="unknown_or_already_started_arm"):
        reopened.claim("m1-v3")
    with pytest.raises(BudgetError, match="request_slot_unavailable"):
        reopened.reserve_request("m1-v3", "initial")
    assert reopened.counts() == {"m1-v3": 1, "m1-v5": 0}
    # A still-unused arm remains available and has its own independent slots.
    reopened.claim("m1-v5")
    reopened.reserve_request("m1-v5", "initial")
    assert reopened.counts() == {"m1-v3": 1, "m1-v5": 1}


@pytest.mark.parametrize("operation", ["claim", "reserve", "counts"])
def test_manifest_mismatch_rejects_every_ledger_operation_without_mutation(tmp_path, operation):
    ledger = create(tmp_path)
    ledger.claim("m1-v3")
    wrong = RequestLedger(ledger.path, "b" * 64)
    with pytest.raises(BudgetError, match="budget_manifest_mismatch"):
        if operation == "claim":
            wrong.claim("m1-v5")
        elif operation == "reserve":
            wrong.reserve_request("m1-v3", "initial")
        else:
            wrong.counts()
    assert ledger.counts() == {"m1-v3": 0, "m1-v5": 0}
    ledger.claim("m1-v5")


def test_unclaimed_unknown_arms_and_unknown_slots_do_not_consume_budget(tmp_path):
    ledger = create(tmp_path)
    for arm in ("m1-v3", "unknown"):
        with pytest.raises(BudgetError, match="arm_not_claimed"):
            ledger.reserve_request(arm, "initial")
    with pytest.raises(BudgetError, match="unknown_or_already_started_arm"):
        ledger.claim("unknown")
    ledger.claim("m1-v3")
    with pytest.raises(BudgetError, match="request_slot_unavailable"):
        ledger.reserve_request("m1-v3", "unknown")
    assert ledger.counts() == {"m1-v3": 0, "m1-v5": 0}


@pytest.mark.parametrize("unsafe", ["missing", "symlink"])
def test_missing_or_symlink_ledger_is_not_silently_created_or_followed(tmp_path, unsafe):
    ledger = create(tmp_path)
    other = tmp_path / "other.db"
    if unsafe == "symlink":
        other.symlink_to(ledger.path)
    invalid = RequestLedger(other, MANIFEST)
    with pytest.raises(BudgetError, match="missing_or_unsafe_budget_ledger"):
        invalid.claim("m1-v3")
    assert ledger.counts() == {"m1-v3": 0, "m1-v5": 0}
    if unsafe == "missing":
        assert not other.exists()


@pytest.mark.asyncio
async def test_hook_requires_frozen_active_window_and_never_reopens_its_slot(tmp_path):
    ledger = create(tmp_path)
    guard = SlotGuard(ledger, "m1-v3")
    with pytest.raises(BudgetError, match="request_outside_frozen_window"):
        await guard.hook(None)
    guard.case = "initial"
    with pytest.raises(BudgetError, match="arm_not_claimed"):
        await guard.hook(None)
    ledger.claim("m1-v3")
    await guard.hook(None)
    with pytest.raises(BudgetError, match="request_slot_unavailable"):
        await guard.hook(None)
    guard.case = None
    with pytest.raises(BudgetError, match="request_outside_frozen_window"):
        await guard.hook(None)
    guard.case = "initial"
    with pytest.raises(BudgetError, match="request_slot_unavailable"):
        await guard.hook(None)
    guard.case = "correction"
    await guard.hook(None)
    assert ledger.counts() == {"m1-v3": 2, "m1-v5": 0}
