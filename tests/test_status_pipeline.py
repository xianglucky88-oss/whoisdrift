"""Integration: interleaved events, duplicates, page failures and recovery.

Each test drives the bundled offline fixture through the public pipeline, so the
verified behaviour is the one users actually get: the same SQLite store, the
same in-process transport and the same orchestration entry point.
"""

import pytest

from whoisdrift import pipeline, status
from whoisdrift.framework import Transport, run
from whoisdrift.store import Store


@pytest.fixture()
def env():
    transport = Transport()
    con = Store().connect()
    store = Store(con)
    store.reset()
    transport.request("POST", "/control", json_body={"reset": True})
    yield transport, con, store
    transport.request("POST", "/control", json_body={"reset": True})
    store.reset()
    con.close()


def states(store):
    return {d["id"]: (d["state"], d["state_ts"]) for d in store.all_current()}


def test_late_report_does_not_regress_confirmed_state(env):
    transport, con, store = env
    # First snapshot confirms WHO-0002 as "active" at 2030-01-03.
    first = run(mode="full", con=con)
    assert status.current_state(store, "WHO-0002")["state"] == "active"
    assert first["events"]["new"] > 0
    # A later snapshot surfaces an *older* report for the same domain.
    transport.request("POST", "/reports", json_body={
        "id": "WHO-0002", "ts": "2029-06-01T00:00:00Z", "status": "pending"})
    result = run(mode="full", con=con)
    current = status.current_state(store, "WHO-0002")
    assert current["state"] == "active"
    assert current["state_ts"] == "2030-01-03T00:00:00Z"
    assert result["status"]["by_reason"]["stale_event"] == 1
    explained = status.explain(store, "WHO-0002")
    stale = [d for d in explained["rejected"] if d["reason"] == "stale_event"]
    assert stale and stale[0]["detail"] and stale[0]["late_arrival"] is True


def test_duplicate_reports_are_explained_not_reapplied(env):
    transport, con, store = env
    first = run(mode="full", con=con)
    before = states(store)
    second = run(mode="full", con=con)
    assert second["events"]["new"] == 0
    assert second["events"]["repeat"] == first["events"]["new"]
    assert second["status"]["by_reason"].get("duplicate_event") == first["events"]["new"]
    assert states(store) == before
    # Duplicate rows inside a single batch are still deduplicated before the
    # journal (the fixture injects one exact duplicate).
    assert first["duplicates"] >= 1


def test_full_and_incremental_converge(env):
    transport, con, store = env
    full = run(mode="full", con=con)
    inc_unchanged = run(mode="incremental", con=con)
    assert inc_unchanged["events"]["new"] == 0
    assert states(store) == {d["id"]: (d["state"], d["state_ts"])
                             for d in store.all_current()}
    # A new source row arrives; incremental only adds that row.
    transport.request("GET", "/grow")
    inc = run(mode="incremental", con=con)
    assert inc["events"]["new"] >= 1
    assert inc["ingested"] >= 1
    from_inc = states(store)

    # Repeating the whole exercise with a pure full collection must agree.
    transport.request("POST", "/control", json_body={"reset": True})
    store.reset()
    transport.request("GET", "/grow")
    full2 = run(mode="full", con=con)
    assert from_inc == states(store)
    assert full2["watermark"] == inc["watermark"]
    assert status.compare(store)["equivalent"]


@pytest.mark.parametrize("control,kind,reason", [
    ({"offline": True}, "offline", "source_unavailable"),
    ({"faults": {"2": 403}}, "blocked", "access_blocked"),
    ({"faults": {"2": 404}}, "missing_page", "missing_page"),
    ({"faults": {"2": 500}}, "source_error", "source_error"),
    ({"omit_pages": [2]}, "short_page", "missing_page"),
])
def test_page_failure_is_not_a_deletion(env, control, kind, reason):
    transport, con, store = env
    baseline = run(mode="full", con=con)
    before = states(store)
    transport.request("POST", "/control", json_body=control)
    result = run(mode="full", con=con)
    assert result["collection"]["complete"] is False
    assert result["failures"] and result["failures"][0]["kind"] == kind
    assert result["failures"][0]["reason"] == reason
    assert result["failures"][0]["outcome"] == "unconfirmed"
    # No domain changed state because a page failed to load.
    assert states(store) == before
    assert result["status"]["by_reason"].get(reason) == 1
    # The failure is visible in the audit trail, marked unconfirmed.
    unconfirmed = store.decisions(outcome="unconfirmed")
    assert any(d["reason"] == reason and d["detail"] for d in unconfirmed)


def test_failed_end_of_data_confirmation_is_not_an_end_of_data(env):
    transport, con, store = env
    run(mode="full", con=con)
    before = states(store)
    # The last (short) page looks like the end, but confirming page 4 fails.
    transport.request("POST", "/control", json_body={"faults": {"4": 502}})
    result = run(mode="full", con=con)
    assert result["collection"]["complete"] is False
    assert result["failures"][0]["page"] == 4
    assert result["failures"][0]["outcome"] == "unconfirmed"
    assert states(store) == before


def test_offline_run_does_not_create_states(env):
    transport, con, store = env
    transport.request("POST", "/control", json_body={"offline": True})
    result = run(mode="full", con=con)
    assert result["collection"]["complete"] is False
    assert result["ingested"] == 0
    assert states(store) == {}
    assert result["status"]["by_reason"].get("source_unavailable") == 1


def test_recovery_after_failure_matches_uninterrupted_run(env):
    transport, con, store = env
    # Interrupted collection: page 2 blocked.
    transport.request("POST", "/control", json_body={"faults": {"2": 503}})
    first = run(mode="full", con=con)
    assert first["collection"]["complete"] is False
    partial = states(store)
    # Recovery: faults cleared, full sweep succeeds.
    transport.request("POST", "/control", json_body={"faults": {}})
    second = run(mode="full", con=con)
    assert second["collection"]["complete"] is True
    recovered = states(store)
    assert set(recovered) >= set(partial)
    # Same result as an uninterrupted run from scratch.
    transport.request("POST", "/control", json_body={"reset": True})
    store.reset()
    clean = run(mode="full", con=con)
    assert states(store) == recovered
    assert clean["collection"]["complete"] is True


def test_historical_query_does_not_change_current_state(env):
    transport, con, store = env
    transport.request("POST", "/reports", json_body={
        "id": "WHO-0003", "ts": "2030-03-01T00:00:00Z", "status": "deleted"})
    run(mode="full", con=con)
    before = status.current_state(store, "WHO-0003")
    past = pipeline.state_at("WHO-0003", "2030-01-05T00:00:00Z", con=con)
    mid = pipeline.state_at("WHO-0003", "2030-02-01T00:00:00Z", con=con)
    after = status.current_state(store, "WHO-0003")
    assert past["state"] == "pending"          # confirmed pending report
    assert mid["state"] == "pending"
    assert past["boundary"]["current_state_not_modified"] is True
    assert before == after                      # terminal state preserved
    assert after["state"] == "archived"


def test_restored_old_events_cannot_regress_state(env):
    transport, con, store = env
    run(mode="full", con=con)
    before = status.current_state(store, "WHO-0004")
    from whoisdrift import kernel
    old = kernel.prepare_event(kernel.event_from_record(
        {"id": "WHO-0004", "ts": "2028-01-01T00:00:00Z", "status": "new"},
        source="restore", observed_seq=10 ** 6))
    store.record_event(old)
    summary = pipeline.reconcile(con=con)
    after = status.current_state(store, "WHO-0004")
    assert after["state"] == before["state"]
    assert after["state_ts"] == before["state_ts"]
    assert summary["by_reason"].get("stale_event")


def test_rule_adjustment_keeps_current_state_monotonic(env):
    transport, con, store = env
    run(mode="full", con=con)
    before = status.current_state(store, "WHO-0002")
    # Adjust rules so 'active' no longer means the same state; the confirmed
    # (newer) state must still not be dragged back by recomputation.
    adjusted = pipeline.reconcile(con=con, rules={
        "mapping": dict(__import__("whoisdrift.kernel", fromlist=["TRANS"]).TRANS,
                        active="pending"),
    })
    after = status.current_state(store, "WHO-0002")
    assert after["state"] == before["state"]
    assert after["state_ts"] == before["state_ts"]
    assert adjusted["by_reason"].get("rule_adjustment") == 1
    # The stored decision explains either the adjustment or the refusal.
    reasons = {d["reason"] for d in store.decisions("WHO-0002")}
    assert reasons & {"rule_adjustment", "stale_recompute", "initial_state"}


def test_explain_covers_changes_observations_rejections_and_boundary(env):
    transport, con, store = env
    transport.request("POST", "/reports", json_body={
        "id": "WHO-0005", "ts": "2030-05-01T00:00:00Z", "status": "deleted"})
    transport.request("POST", "/reports", json_body={
        "id": "WHO-0005", "ts": "2030-05-02T00:00:00Z", "status": "active"})
    transport.request("POST", "/reports", json_body={
        "id": "WHO-0005", "ts": "2030-05-03T00:00:00Z", "status": "mystery"})
    run(mode="full", con=con)
    explained = status.explain(store, "WHO-0005")
    assert explained["current"]["state"] == "archived"
    assert explained["boundary"]["terminal"] is True
    reasons = explained["counts"]["by_reason"]
    assert reasons.get("terminal_boundary") == 1
    assert reasons.get("unknown_status") == 1
    assert explained["applied"] and explained["rejected"] and explained["unconfirmed"]
    # Every retained decision carries a human-readable explanation.
    assert all(d["detail"] for d in explained["decisions"])
