"""Status-change semantics: monotonic resolution, audit trail, boundaries."""

import importlib

import pytest

from whoisdrift import kernel


def ev(rid, ts, status, **kw):
    d = {"id": rid, "ts": ts, "status": status}
    d.update(kw)
    return d


# --------------------------------------------------------------------------- #
# Effective changes, rejected events, unconfirmed observations.
# --------------------------------------------------------------------------- #
def test_late_report_cannot_drag_state_back():
    events = [
        ev("d", "2030-01-10T00:00:00Z", "active"),
        ev("d", "2030-01-01T00:00:00Z", "pending"),  # delivered late
    ]
    result = kernel.resolve_domain(events)
    assert result["state"] == "active"
    assert result["state_ts"] == "2030-01-10T00:00:00Z"
    reasons = {d["reason"] for d in result["rejected"]}
    assert "stale_event" in reasons
    stale = next(d for d in result["rejected"] if d["reason"] == "stale_event")
    assert stale["detail"] and stale["to_state"] == "active"


def test_forward_change_is_applied_and_explained():
    events = [
        ev("d", "2030-01-01T00:00:00Z", "pending"),
        ev("d", "2030-01-02T00:00:00Z", "active"),
        ev("d", "2030-01-03T00:00:00Z", "sold"),
    ]
    result = kernel.resolve_domain(events)
    assert result["state"] == "closed"
    assert [d["reason"] for d in result["applied"]] == [
        "initial_state", "status_changed", "status_changed"]
    assert all(d["detail"] for d in result["applied"])


def test_unknown_status_and_missing_timestamp_are_unconfirmed_not_deletion():
    events = [
        ev("d", "2030-01-01T00:00:00Z", "active"),
        ev("d", "2030-01-02T00:00:00Z", "banana"),
        ev("d", "", "sold"),
        ev("d", None, "deleted"),
    ]
    result = kernel.resolve_domain(events)
    assert result["state"] == "active"
    assert result["terminal"] is False
    reasons = {d["reason"] for d in result["unconfirmed"]}
    assert {"unknown_status", "missing_timestamp"} <= reasons
    assert not result["applied"][1:]
    assert "not evidence of deletion" in kernel.REASON_DETAIL["unknown_status"]


def test_failure_observations_are_never_deletions():
    for kind, reason in [("offline", "source_unavailable"),
                         ("blocked", "access_blocked"),
                         ("missing_page", "missing_page"),
                         ("timeout", "source_unavailable"),
                         ("rate_limited", "rate_limited"),
                         ("unreadable", "unreadable_response")]:
        result = kernel.resolve_domain([kernel.failure_event(kind, "d")])
        assert result["state"] == "unknown"
        assert result["confirmed"] is False
        assert result["boundary"]["reason"] == "no_confirmed_observation"
        assert result["unconfirmed"][0]["reason"] == reason
        assert result["unconfirmed"][0]["outcome"] == "unconfirmed"


def test_failure_does_not_change_existing_state():
    events = [ev("d", "2030-01-01T00:00:00Z", "active")]
    events.append(kernel.failure_event("blocked", "d"))
    result = kernel.resolve_domain(events)
    assert result["state"] == "active"
    assert result["unconfirmed"][0]["reason"] == "access_blocked"


# --------------------------------------------------------------------------- #
# Duplicates and same-timestamp conflicts.
# --------------------------------------------------------------------------- #
def test_duplicate_report_is_recorded_once_in_history():
    events = [
        ev("d", "2030-01-01T00:00:00Z", "active"),
        ev("d", "2030-01-01T00:00:00Z", "active"),
        ev("d", "2030-01-01T00:00:00Z", "active"),
    ]
    result = kernel.resolve_domain(events)
    assert result["state"] == "active"
    assert result["counts"]["applied"] == 1
    assert result["counts"]["by_reason"]["duplicate_report"] == 2


def test_same_timestamp_conflict_is_deterministic():
    events = [
        ev("d", "2030-01-01T00:00:00Z", "active"),
        ev("d", "2030-01-01T00:00:00Z", "sold"),
    ]
    forward = kernel.resolve_domain(events)
    backward = kernel.resolve_domain(list(reversed(events)))
    assert forward["state"] == backward["state"] == "closed"
    assert forward["state_ts"] == backward["state_ts"]
    # Whichever report arrives second, the conflict is explained: either it
    # won precedence (conflict_resolved) or lost it (conflict_same_timestamp).
    reasons = {d["reason"] for d in forward["decisions"]}
    assert {"conflict_resolved", "conflict_same_timestamp"} & reasons


def test_input_events_are_not_mutated():
    events = [ev("d", "2030-01-02T00:00:00Z", "active"),
              ev("d", "2030-01-01T00:00:00Z", "pending")]
    snapshot = [dict(e) for e in events]
    kernel.resolve_domain(events)
    assert events == snapshot


# --------------------------------------------------------------------------- #
# Terminal boundary.
# --------------------------------------------------------------------------- #
def test_terminal_boundary_blocks_resurrection_but_accepts_reconfirmation():
    events = [
        ev("d", "2030-01-01T00:00:00Z", "active"),
        ev("d", "2030-01-02T00:00:00Z", "deleted"),
        ev("d", "2030-01-03T00:00:00Z", "active"),    # rejected
        ev("d", "2030-01-04T00:00:00Z", "deleted"),   # reconfirmation
    ]
    result = kernel.resolve_domain(events)
    assert result["state"] == "archived"
    assert result["terminal"] is True
    assert result["boundary"]["reason"] == "terminal_state"
    assert any(d["reason"] == "terminal_boundary" for d in result["rejected"])
    assert result["state_ts"] == "2030-01-04T00:00:00Z"


def test_rule_adjustment_can_opt_in_to_resurrection():
    events = [
        ev("d", "2030-01-01T00:00:00Z", "active"),
        ev("d", "2030-01-02T00:00:00Z", "deleted"),
        ev("d", "2030-01-03T00:00:00Z", "active"),
    ]
    rules = kernel.make_rules({"allow_resurrection": True})
    result = kernel.resolve_domain(events, rules)
    assert result["state"] == "active"
    assert result["terminal"] is False


# --------------------------------------------------------------------------- #
# Historical queries are read-only.
# --------------------------------------------------------------------------- #
def test_state_at_uses_event_time_and_does_not_touch_current():
    events = [
        ev("d", "2030-01-01T00:00:00Z", "pending"),
        ev("d", "2030-01-05T00:00:00Z", "active"),
        ev("d", "2030-01-09T00:00:00Z", "sold"),
    ]
    before = kernel.resolve_domain(events)
    past = kernel.state_at(events, "2030-01-06T00:00:00Z")
    after = kernel.resolve_domain(events)
    assert past["state"] == "active"
    assert past["excluded_events"] == 1
    assert past["boundary"]["current_state_not_modified"] is True
    assert before == after
    assert kernel.resolve_domain(events)["state"] == "closed"


def test_state_at_before_first_observation_explains_the_boundary():
    events = [ev("d", "2030-01-05T00:00:00Z", "active")]
    past = kernel.state_at(events, "2030-01-01T00:00:00Z")
    assert past["state"] == "unknown"
    assert past["confirmed"] is False
    assert past["boundary"]["reason"] == "no_observation_before_as_of"
    assert past["excluded_events"] == 1


def test_state_at_observed_basis_distinguishes_seen_from_happened():
    events = [
        ev("d", "2030-01-09T00:00:00Z", "sold", observed_seq=1,
           observed_at="2030-01-10"),
        ev("d", "2030-01-02T00:00:00Z", "active", observed_seq=2,
           observed_at="2030-01-11"),
    ]
    # What had been *seen* by 2030-01-10: only the (later-dated) sold report.
    seen = kernel.state_at(events, "2030-01-10", basis="observed")
    assert seen["selected_events"] == 1
    # What *happened* by 2030-01-03: the active report, even though it arrived late.
    happened = kernel.state_at(events, "2030-01-03T00:00:00Z", basis="event")
    assert happened["state"] == "active"
    assert kernel.resolve_domain(events)["state"] == "closed"


def test_terminal_boundary_is_visible_in_historical_view():
    events = [
        ev("d", "2030-01-01T00:00:00Z", "active"),
        ev("d", "2030-01-05T00:00:00Z", "deleted"),
    ]
    past = kernel.state_at(events, "2030-01-06T00:00:00Z")
    assert past["state"] == "archived"
    assert past["boundary"]["reason"] == "terminal_state"


# --------------------------------------------------------------------------- #
# Compatibility of the original entry points.
# --------------------------------------------------------------------------- #
def test_legacy_process_and_reduce_are_unchanged():
    module = importlib.import_module("whoisdrift.kernel")
    assert module.process({"status": "active"}) == {
        "state_norm": "active", "invalid_status": 0}
    assert module.process({"status": "nonsense"}) == {
        "state_norm": "unknown", "invalid_status": 1}
    records = [
        {"id": "a", "ts": "1", "state_norm": "active"},
        {"id": "a", "ts": "2", "state_norm": "pending"},
    ]
    out = module.reduce(records)
    assert out[1]["regression"] == 1


def test_timestamp_ordering_accepts_iso_and_compact_values():
    assert kernel.ts_key("2030-01-01T00:00:00Z") == kernel.ts_key("2030-01-01T00:00:00+00:00")
    assert kernel.ts_key("t2") > kernel.ts_key("t1")
    assert kernel.ts_key("") < kernel.ts_key("t1")
    assert not kernel.has_timestamp("")
    assert not kernel.has_timestamp(None)
    assert kernel.has_timestamp("2030-01-01T00:00:00Z")
