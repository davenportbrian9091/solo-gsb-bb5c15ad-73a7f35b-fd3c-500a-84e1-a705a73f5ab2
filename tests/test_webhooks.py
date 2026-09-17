"""End-to-end tests for the reliable webhook delivery API."""
from __future__ import annotations

import hashlib
import hmac
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import httpx
import pytest

from .conftest import receipts, wait_for

SECRET = "test-secret"


def _sign(body: bytes) -> str:
    return "sha256=" + hmac.new(SECRET.encode(), body, hashlib.sha256).hexdigest()


def _post_event(base_url: str, target_path: str, key: str | None = None,
                payload=None, extra_query: str = "") -> httpx.Response:
    key = key or uuid.uuid4().hex
    body = (
        b'{"idempotency_key":"' + key.encode()
        + b'","target":"' + f"{base_url}{target_path}{extra_query}".encode()
        + b'","payload":' + (payload or b'{"v":1}') + b"}"
    )
    return httpx.post(
        f"{base_url}/events",
        content=body,
        headers={"X-Webhook-Signature": _sign(body), "Content-Type": "application/json"},
    )


def _get_event(base_url: str, event_id: str) -> dict:
    return httpx.get(f"{base_url}/events/{event_id}").json()


def _parse_iso(value: str):
    from datetime import datetime
    return datetime.fromisoformat(value)


# ---------------------------------------------------------------- signature

def test_missing_or_bad_signature_is_rejected(server):
    body = (
        b'{"idempotency_key":"bad-sig","target":"'
        + f"{server.base_url}/t/record".encode()
        + b'","payload":{}}'
    )
    assert httpx.post(f"{server.base_url}/events", content=body).status_code == 401
    resp = httpx.post(
        f"{server.base_url}/events",
        content=body,
        headers={"X-Webhook-Signature": "sha256=deadbeef"},
    )
    assert resp.status_code == 401


def test_tampered_body_fails_signature(server):
    body = (
        b'{"idempotency_key":"tamper","target":"'
        + f"{server.base_url}/t/record".encode()
        + b'","payload":{"a":1}}'
    )
    resp = httpx.post(
        f"{server.base_url}/events",
        content=body,
        headers={"X-Webhook-Signature": _sign(body + b" ")},
    )
    assert resp.status_code == 401


# ------------------------------------------------------------- happy path

def test_event_is_delivered_and_queryable(server):
    resp = _post_event(server.base_url, "/t/record")
    assert resp.status_code == 201
    event = resp.json()
    assert event["status"] in ("queued", "processing")

    final = wait_for(lambda: (lambda e: e if e["status"] == "delivered" else None)(
        _get_event(server.base_url, event["id"])
    ))
    assert final["attempts_count"] == 1
    attempt = final["attempts"][0]
    assert attempt["attempt_no"] == 1
    assert attempt["status_code"] == 200
    assert attempt["error"] is None
    assert attempt["attempted_at"]
    assert final["delivered_at"]

    # Outbound request was signed with the shared secret and carried headers.
    mine = [r for r in receipts if r["event_id"] == event["id"]]
    assert len(mine) == 1
    assert mine[0]["idempotency_key"] == event["idempotency_key"]
    assert mine[0]["signature"] == _sign(mine[0]["body"].encode())

    # List endpoint shows it too.
    listed = httpx.get(f"{server.base_url}/events", params={"status": "delivered"}).json()
    assert any(e["id"] == event["id"] for e in listed["events"])


# ------------------------------------------------------------ idempotency

def test_duplicate_idempotency_key_returns_original_and_is_not_reenqueued(server):
    key = f"dup-{uuid.uuid4().hex}"
    before = len([r for r in receipts if r["idempotency_key"] == key])

    r1 = _post_event(server.base_url, "/t/record", key=key)
    assert r1.status_code == 201
    wait_for(lambda: _get_event(server.base_url, r1.json()["id"])["status"] == "delivered")

    r2 = _post_event(server.base_url, "/t/record", key=key)
    assert r2.status_code == 200
    assert r2.headers.get("idempotent-replay") == "true"
    assert r2.json()["id"] == r1.json()["id"]

    time.sleep(0.5)
    delivered = [r for r in receipts if r["idempotency_key"] == key]
    assert len(delivered) == before + 1  # exactly one outbound delivery ever


# ------------------------------------------------- retries with 1/2/4 backoff

def test_retries_use_1_2_4_backoff_then_dead_letter(server):
    resp = _post_event(server.base_url, "/t/fail-always")
    event_id = resp.json()["id"]
    final = wait_for(
        lambda: (lambda e: e if e["status"] == "dead" else None)(
            _get_event(server.base_url, event_id)
        ),
        timeout=30,
    )
    assert final["attempts_count"] == 4
    assert [a["status_code"] for a in final["attempts"]] == [500, 500, 500, 500]
    assert final["delivered_at"] is None

    times = [_parse_iso(a["attempted_at"]) for a in final["attempts"]]
    gaps = [(times[i] - times[i - 1]).total_seconds() for i in range(1, 4)]
    # Gaps include request latency; allow slack but verify the 1/2/4 schedule.
    assert 0.9 <= gaps[0] <= 2.5, gaps
    assert 1.8 <= gaps[1] <= 3.5, gaps
    assert 3.6 <= gaps[2] <= 5.5, gaps

    # Dead-letter events are listed under status=dead.
    dead = httpx.get(f"{server.base_url}/events", params={"status": "dead"}).json()
    assert any(e["id"] == event_id for e in dead["events"])


def test_event_recovers_after_two_failures(server):
    resp = _post_event(
        server.base_url, "/dev/sink", extra_query="?fail_times=2&fail_status=503"
    )
    event_id = resp.json()["id"]
    final = wait_for(
        lambda: (lambda e: e if e["status"] == "delivered" else None)(
            _get_event(server.base_url, event_id)
        ),
        timeout=20,
    )
    codes = [a["status_code"] for a in final["attempts"]]
    assert codes == [503, 503, 200]


# ---------------------------------------------------- per-endpoint ordering

def test_events_to_same_endpoint_are_strictly_ordered(server):
    from .conftest import _gates
    token = f"order-{uuid.uuid4().hex}"
    _gates[token] = threading.Event()

    first = _post_event(server.base_url, "/t/block", extra_query=f"?token={token}")
    first_id = first.json()["id"]
    # Ensure the first delivery has claimed and is blocking.
    wait_for(
        lambda: _get_event(server.base_url, first_id)["status"] == "processing",
        timeout=10,
    )

    second = _post_event(server.base_url, "/t/block", extra_query=f"?token={token}")
    second_id = second.json()["id"]
    time.sleep(0.5)
    # The second event must stay queued while the first is in flight.
    assert _get_event(server.base_url, second_id)["status"] == "queued"

    # Release the gate: both must resolve, and recorded delivery order
    # (verified through /events ids) is first -> second.
    httpx.post(f"{server.base_url}/t/release", params={"token": token})
    wait_for(lambda: _get_event(server.base_url, first_id)["status"] == "delivered")
    wait_for(lambda: _get_event(server.base_url, second_id)["status"] == "delivered")

    mine = [r for r in receipts]  # block sink does not record; assert via timestamps
    t1 = _parse_iso(_get_event(server.base_url, first_id)["delivered_at"])
    t2 = _parse_iso(_get_event(server.base_url, second_id)["delivered_at"])
    assert t1 <= t2


def test_earlier_failure_blocks_later_event_until_it_dies(server):
    """While event A retries/dies, B to the same target must not start."""
    target = f"{server.base_url}/t/fail-always"

    first = _post_event(server.base_url, "/t/fail-always")
    first_id = first.json()["id"]
    body = (
        b'{"idempotency_key":"' + uuid.uuid4().hex.encode()
        + b'","target":"' + target.encode() + b'","payload":{}}'
    )
    second_resp = httpx.post(
        f"{server.base_url}/events",
        content=body,
        headers={"X-Webhook-Signature": _sign(body)},
    )
    second_id = second_resp.json()["id"]

    # While the first is failing/retrying, the second never starts:
    # it has zero attempts and stays queued.
    deadline = time.time() + 3
    while time.time() < deadline:
        first_e = _get_event(server.base_url, first_id)
        second_e = _get_event(server.base_url, second_id)
        if first_e["status"] in ("queued", "processing"):
            assert second_e["attempts_count"] == 0
        time.sleep(0.1)

    final_first = wait_for(
        lambda: (lambda e: e if e["status"] == "dead" else None)(
            _get_event(server.base_url, first_id)
        ), timeout=30
    )
    # Only after the first is dead does the second start (and it also fails).
    final_second = wait_for(
        lambda: (lambda e: e if e["status"] == "dead" else None)(
            _get_event(server.base_url, second_id)
        ), timeout=30
    )
    last_attempt_first = _parse_iso(final_first["attempts"][-1]["attempted_at"])
    first_attempt_second = _parse_iso(final_second["attempts"][0]["attempted_at"])
    assert last_attempt_first <= first_attempt_second


# -------------------------------------------------------- dead-letter replay

def _post_replay(base_url: str, event_id: str) -> httpx.Response:
    return httpx.post(f"{base_url}/events/{event_id}/replay")


def _wait_status(base_url: str, event_id: str, status: str, timeout=30) -> dict:
    return wait_for(
        lambda: (lambda e: e if e["status"] == status else None)(
            _get_event(base_url, event_id)
        ),
        timeout=timeout,
    )


def test_replay_dead_event_runs_new_round_and_delivers(server):
    # /dev/sink?fail_times=4 fails the first 4 attempts (-> dead), then
    # answers 200, so the first attempt of the replayed round succeeds.
    resp = _post_event(server.base_url, "/dev/sink", extra_query="?fail_times=4")
    assert resp.status_code == 201
    event_id = resp.json()["id"]
    key = resp.json()["idempotency_key"]

    dead = _wait_status(server.base_url, event_id, "dead")
    assert dead["attempts_count"] == 4
    assert dead["replay_count"] == 0
    assert dead["last_replayed_at"] is None

    replay = _post_replay(server.base_url, event_id)
    assert replay.status_code == 200
    body = replay.json()
    assert body["status"] == "queued"
    assert body["replay_count"] == 1
    assert body["last_replayed_at"]
    assert body["idempotency_key"] == key  # idempotency key unchanged
    assert [a["attempt_no"] for a in body["attempts"]] == [1, 2, 3, 4]

    final = _wait_status(server.base_url, event_id, "delivered", timeout=20)
    # History preserved; attempt numbers continue across the replay.
    assert final["attempts_count"] == 5
    assert [a["attempt_no"] for a in final["attempts"]] == [1, 2, 3, 4, 5]
    assert [a["status_code"] for a in final["attempts"]] == [500, 500, 500, 500, 200]
    assert final["replay_count"] == 1
    assert final["delivered_at"]

    # Replay metadata is exposed by every query endpoint.
    by_key = httpx.get(f"{server.base_url}/events/key/{key}").json()
    assert by_key["replay_count"] == 1
    assert by_key["last_replayed_at"] == final["last_replayed_at"]
    listed = httpx.get(
        f"{server.base_url}/events", params={"status": "delivered"}
    ).json()
    assert any(
        e["id"] == event_id and e["replay_count"] == 1 for e in listed["events"]
    )


def test_replay_restarts_full_backoff_round_before_dying_again(server):
    # fail_times=100: the target never succeeds, so the replayed round must
    # run its own complete 1s/2s/4s schedule and die after 4 more attempts.
    resp = _post_event(server.base_url, "/dev/sink", extra_query="?fail_times=100")
    event_id = resp.json()["id"]
    _wait_status(server.base_url, event_id, "dead")

    assert _post_replay(server.base_url, event_id).status_code == 200

    final = wait_for(
        lambda: (lambda e: e if e["status"] == "dead" and e["replay_count"] == 1
                 else None)(_get_event(server.base_url, event_id)),
        timeout=30,
    )
    # A complete second round ran: 4 fresh attempts, continuous numbering.
    assert final["attempts_count"] == 8
    assert [a["attempt_no"] for a in final["attempts"]] == list(range(1, 9))
    assert [a["status_code"] for a in final["attempts"]] == [500] * 8

    times = [_parse_iso(a["attempted_at"]) for a in final["attempts"]]
    gaps = [(times[i] - times[i - 1]).total_seconds() for i in range(1, 8)]
    # The replayed round starts at once, then backs off 1s / 2s / 4s again.
    assert gaps[3] <= 2.5, gaps
    assert 0.9 <= gaps[4] <= 2.5, gaps
    assert 1.8 <= gaps[5] <= 3.5, gaps
    assert 3.6 <= gaps[6] <= 5.5, gaps


def test_replay_rejects_non_dead_and_unknown_events(server):
    resp = _post_event(server.base_url, "/t/record")
    event_id = resp.json()["id"]

    # Fresh event: queued or processing, either way not replayable.
    replay = _post_replay(server.base_url, event_id)
    assert replay.status_code == 409
    assert "dead" in replay.json()["detail"]

    _wait_status(server.base_url, event_id, "delivered", timeout=20)
    replay = _post_replay(server.base_url, event_id)
    assert replay.status_code == 409  # delivered is terminal
    assert "delivered" in replay.json()["detail"]

    missing = _post_replay(server.base_url, "no-such-event")
    assert missing.status_code == 404


def test_concurrent_replays_requeue_exactly_once(server):
    from .conftest import _fail_counts

    resp = _post_event(server.base_url, "/t/fail-always")
    event_id = resp.json()["id"]
    _wait_status(server.base_url, event_id, "dead")

    with ThreadPoolExecutor(max_workers=8) as pool:
        responses = list(
            pool.map(lambda _: _post_replay(server.base_url, event_id), range(8))
        )
    codes = [r.status_code for r in responses]
    assert codes.count(200) == 1
    assert codes.count(409) == len(responses) - 1

    final = wait_for(
        lambda: (lambda e: e if e["status"] == "dead" and e["attempts_count"] == 8
                 else None)(_get_event(server.base_url, event_id)),
        timeout=30,
    )
    # Exactly one replay round ran: replay_count incremented once, exactly
    # 4 new attempts, and exactly 8 outbound deliveries in total.
    assert final["replay_count"] == 1
    assert [a["attempt_no"] for a in final["attempts"]] == list(range(1, 9))
    assert _fail_counts[event_id] == 8
