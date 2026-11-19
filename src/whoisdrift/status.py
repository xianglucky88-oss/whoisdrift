
"""Auditable domain-status service on top of :mod:`whoisdrift.kernel`/``store``.

Public API
----------
``ingest_events(store, events, ...)``
    Journal raw observations, resolve them and explain every decision.
``reconcile(store, rules=None)``
    Re-derive the current state from the full event journal (used after a rule
    adjustment or after restoring old data).  The result can never move the
    current state backwards in event time unless ``allow_rollback=True``.
``state_at(store, domain_id, as_of, basis="event")``
    Read-only historical query; it does not touch the materialised state.
``explain(store, domain_id)``
    Everything a user needs to understand a domain: current state, effective
    changes, unconfirmed observations, rejected events and the boundary.
``compare(store)``
    Full-vs-incremental equivalence check over stored records and journal.
"""

from __future__ import annotations

from . import kernel


def _reason(key):
    return kernel.REASON_DETAIL.get(key, "")


def event_batch(records, run_id, start_seq=0, source="crawl", batch=None):
    """Convert source records to audit events with observation ordering.

    ``batch`` identifies the collection snapshot the reports came from (it
    defaults to ``run_id``).  Reports from one snapshot are treated as an
    unordered set and ordered by event time; reports from *later* snapshots are
    applied afterwards, which is how a genuinely late report is recognised.
    """
    import hashlib
    import json
    batch = run_id if batch is None else batch
    events = []
    for offset, record in enumerate(records, start=1):
        seq = start_seq + offset
        ev = kernel.event_from_record(
            kernel.source_view(record), source=source, observed_seq=seq,
            observed_at="%s#%06d" % (run_id, seq), batch=batch)
        # Content-addressed identity used to compare the record table with the
        # event journal.  Keyed on the *source* content rather than the enriched
        # payload, because derived fields (state_norm/invalid_status/regression)
        # depend on the batch a record was processed in; full and incremental
        # collection of the same source row must agree.
        blob = json.dumps(kernel.source_view(record), sort_keys=True,
                          ensure_ascii=False)
        ev["record_hash"] = hashlib.sha256(blob.encode()).hexdigest()[:24]
        events.append(ev)
    return events


def ingest_failures(store, failures, rules=None, run_id=None, start_seq=0):
    """Journal collection/transport failures as unconfirmed observations.

    ``failures`` items carry ``kind`` (e.g. ``offline``, ``blocked``,
    ``missing_page``), and optionally ``page``/``status``/``detail``.  Each
    becomes an event with no status value: a failed RDAP request, a block, an
    offline source or a missing page is *not* evidence of deletion.
    """
    k = kernel
    rules = k.make_rules(rules)
    if run_id is None:
        run_id = "run-%06d" % store.next_counter("run_seq")
    out, decisions = [], []
    for offset, f in enumerate(failures, start=1):
        reason = k.failure_reason(f.get("kind"))
        ev = k.failure_event(f.get("kind"), observed_seq=start_seq + offset,
                             observed_at="%s#f%06d" % (run_id, offset),
                             page=f.get("page"), status=f.get("status"),
                             detail=f.get("detail"))
        ev["event_key"] = "failure:run:%s:%d" % (run_id, offset)
        ev["source"] = "failure"
        inserted, _repeat = store.record_event(ev)
        out.append({"kind": f.get("kind"), "page": f.get("page"),
                    "status": f.get("status"), "reason": reason,
                    "detail": _reason(reason), "outcome": "unconfirmed",
                    "applied": False, "recorded": inserted})
        if inserted:
            decisions.append({
                "id": "", "event_key": ev["event_key"], "ts": "",
                "raw_status": "", "state": "unknown", "outcome": "unconfirmed",
                "reason": reason, "detail": _reason(reason), "applied": False,
                "from_state": None, "to_state": None,
                "observed_seq": ev.get("observed_seq"), "source": "failure",
            })
    if decisions:
        store.record_decisions(decisions, rules["version"], run_id)
    return out


def _decisions_summary(decisions):
    by_reason = {}
    for d in decisions:
        by_reason[d["reason"]] = by_reason.get(d["reason"], 0) + 1
    return {
        "applied": sum(1 for d in decisions if d["outcome"] == "applied"),
        "rejected": sum(1 for d in decisions if d["outcome"] == "rejected"),
        "unconfirmed": sum(1 for d in decisions if d["outcome"] == "unconfirmed"),
        "by_reason": by_reason,
    }


def _stale_recompute_decision(rid, result, previous):
    return {
        "id": rid,
        "event_key": "stale_recompute:%s:%s" % (rid, result.get("state_ts")),
        "ts": result.get("state_ts"),
        "raw_status": "", "state": result.get("state"),
        "outcome": "rejected", "reason": "stale_recompute",
        "detail": _reason("stale_recompute"), "applied": False,
        "from_state": previous.get("state") if previous else None,
        "to_state": previous.get("state") if previous else None,
        "observed_seq": None, "source": "reconcile",
    }


#: Reasons that are meaningful for every run even when they recur identically.
_ALWAYS_LOG = {"rule_adjustment", "stale_recompute", "duplicate_event"}


def _new_decisions(store, decisions, domain_id=None):
    """Drop decisions that are already in the journal (idempotent ingestion).

    Without this, re-resolving a domain's whole history on every run would copy
    the same ``initial_state``/``status_changed`` decisions into the log again.
    Recurring explanations (duplicate reports, rule adjustments) are kept.
    """
    existing = store.decision_keys(domain_id)
    out = []
    for d in decisions:
        if d["reason"] in _ALWAYS_LOG:
            out.append(d)
            continue
        if (d.get("event_key"), d["reason"]) in existing:
            continue
        out.append(d)
    return out


def _record_history_once(store, rid, decisions, rules, run_id):
    existing = store.history_event_keys(rid)
    for d in decisions:
        if d["outcome"] != "applied":
            continue
        key = d.get("event_key")
        if key in existing:
            continue
        store.record_history(rid, d, d.get("from_state"), rules["version"],
                             run_id)
        existing.add(key)


def ingest_events(store, events, rules=None, run_id=None, allow_rollback=False,
                  record_repeats=True):
    """Journal observations and explain every decision.

    Repeated reports (same domain, event time and status) are recorded once and
    explained as ``duplicate_event`` on subsequent runs.  Unconfirmed
    observations – failures, blocked requests, missing pages, unknown statuses,
    missing timestamps – never change the current state.
    """
    k = kernel
    rules = k.make_rules(rules)
    if run_id is None:
        run_id = "run-%06d" % store.next_counter("run_seq")

    fresh, repeats = [], []
    for ev in events:
        prepared = k.prepare_event(ev, rules)
        inserted, _repeat = store.record_event(prepared)
        (fresh if inserted else repeats).append(prepared)

    decisions = []
    for rid in sorted({ev["id"] for ev in fresh if ev["id"]}):
        result = k.resolve_domain(store.events(rid), rules)
        previous = store.current(rid)
        persisted = store.set_current(result, run_id=run_id, previous=previous,
                                      allow_rollback=allow_rollback)
        if persisted.get("reason") == "stale_recompute":
            decisions.append(_stale_recompute_decision(rid, result, previous))
            continue
        decisions.extend(_new_decisions(store, result["decisions"], rid))
        _record_history_once(store, rid, result["decisions"], rules, run_id)

    if record_repeats:
        for ev in repeats:
            if not ev["id"]:
                continue
            current = store.current(ev["id"]) or {}
            decisions.append({
                "id": ev["id"], "event_key": ev["event_key"], "ts": ev["ts"],
                "raw_status": ev["raw_status"], "state": ev["state"],
                "outcome": "rejected", "reason": "duplicate_event",
                "detail": _reason("duplicate_event"), "applied": False,
                "from_state": current.get("state"),
                "to_state": current.get("state"),
                "observed_seq": ev.get("observed_seq"),
                "source": ev.get("source"),
            })

    if decisions:
        store.record_decisions(decisions, rules["version"], run_id)

    return {
        "run_id": run_id,
        "new_events": len(fresh),
        "repeat_events": len(repeats),
        "decisions": decisions,
        "rules_version": rules["version"],
    }


def reconcile(store, rules=None, run_id=None, allow_rollback=False):
    """Re-derive current state from the full journal under the given rules.

    This is the safe path after a rule adjustment or after restoring an older
    database snapshot: events are re-classified and re-resolved, but the
    materialised state never moves backwards in event time (each regression is
    recorded as ``stale_recompute`` with the candidate state it rejected).
    """
    k = kernel
    rules = k.make_rules(rules)
    if run_id is None:
        run_id = "run-%06d" % store.next_counter("run_seq")

    journal = store.events()
    # Refresh the stored classification so the journal reflects current rules.
    for ev in journal:
        if ev.get("failure"):
            continue
        prepared = k.prepare_event(ev, rules)
        store.update_event_classification(
            prepared["event_key"], prepared["state"], prepared["valid"])

    domains = {ev["id"] for ev in journal if ev["id"]}
    previous = {rid: store.current(rid) for rid in domains}
    decisions = []
    for rid in sorted(domains):
        result = k.resolve_domain(store.events(rid), rules)
        persisted = store.set_current(result, run_id=run_id,
                                      previous=previous[rid],
                                      allow_rollback=allow_rollback)
        if persisted.get("reason") == "stale_recompute":
            decisions.append(_stale_recompute_decision(rid, result,
                                                       previous[rid]))
            continue
        decisions.extend(_new_decisions(store, result["decisions"], rid))
        _record_history_once(store, rid, result["decisions"], rules, run_id)

    # Rule adjustments are themselves auditable.
    decisions.append({
        "id": "", "event_key": "rule_adjustment:%s" % rules["version"],
        "ts": "", "raw_status": "", "state": "",
        "outcome": "applied", "reason": "rule_adjustment",
        "detail": _reason("rule_adjustment"), "applied": True,
        "from_state": None, "to_state": None, "observed_seq": None,
        "source": "reconcile",
    })
    store.record_decisions(decisions, rules["version"], run_id)
    summary = _decisions_summary(decisions)
    summary.update({"run_id": run_id, "domains": len(domains),
                    "rules_version": rules["version"]})
    return summary


def backfill(store, rules=None, run_id=None):
    """Seed the event journal from an existing (possibly legacy) record table.

    Older databases only have the ``records`` table.  Backfilling lets their
    content be explained and reconciled without changing what the records table
    contains.  It is idempotent: an existing journal entry is never duplicated.
    Records are ordered by their timestamp so the reconstructed history is the
    same one a fresh collection would produce.
    """
    k = kernel
    rules = k.make_rules(rules)
    if run_id is None:
        run_id = "backfill-%06d" % store.next_counter("run_seq")

    records = sorted(store.all_records(),
                     key=lambda r: (k.ts_key(r.get("ts")), str(r.get("id", ""))))
    start = int(store.meta_get("observed_seq", "0") or 0)
    events = event_batch(records, run_id, start_seq=start)
    store.meta_set("observed_seq", start + len(events))
    summary = ingest_events(store, events, rules=rules, run_id=run_id)
    summary["backfilled_records"] = len(records)
    return summary


def current_state(store, domain_id):
    """Materialised current state for one domain (``None`` if never observed)."""
    return store.current(str(domain_id))


def state_at(store, domain_id, as_of, basis="event", rules=None):
    """Read-only historical query over the stored journal.

    Does not write to the database and does not change the current state.
    Failure/observation events are included so the boundary explains whether
    the picture at ``as_of`` was complete or merely what had been observed.
    """
    events = store.events(str(domain_id))
    return kernel.state_at(events, as_of, basis=basis, rules=rules)


def explain(store, domain_id):
    """Return a complete, user-facing explanation for one domain."""
    rid = str(domain_id)
    current = store.current(rid)
    events = store.events(rid)
    decisions = store.decisions(rid)
    history = store.history(rid)
    by_reason = {}
    for d in decisions:
        by_reason[d["reason"]] = by_reason.get(d["reason"], 0) + 1

    if current is None:
        boundary = {"reason": "never_observed",
                    "detail": _reason("never_observed"),
                    "terminal": False}
    else:
        boundary = {
            "reason": current["reason"],
            "detail": current["detail"],
            "terminal": bool(current["terminal"]),
            "state_ts": current["state_ts"],
            "confirmed": bool(current["confirmed"]),
        }
    return {
        "id": rid,
        "current": current,
        "boundary": boundary,
        "events": events,
        "decisions": decisions,
        "applied": [d for d in decisions if d["outcome"] == "applied"],
        "rejected": [d for d in decisions if d["outcome"] == "rejected"],
        "unconfirmed": [d for d in decisions if d["outcome"] == "unconfirmed"],
        "history": history,
        "counts": {
            "events": len(events),
            "decisions": len(decisions),
            "by_reason": by_reason,
        },
    }


def compare(store):
    """Compare persisted records with the source-level event journal.

    ``equivalent`` is true when every stored record has a matching journal entry
    (comparison is on the *source* view, so kernel-derived fields that depend on
    the collection batch cannot cause spurious differences).  Used to verify
    that full and incremental collection converge, and that re-running is a
    no-op.
    """
    import hashlib
    import json

    def digest(record):
        blob = json.dumps(kernel.source_view(record), sort_keys=True,
                          ensure_ascii=False)
        return hashlib.sha256(blob.encode()).hexdigest()[:24]

    record_hashes = {digest(r) for r in store.all_records()}
    journalled = set()
    for ev in store.events():
        if ev.get("failure"):
            continue
        if ev.get("record_hash"):
            journalled.add(ev["record_hash"])
        elif isinstance(ev.get("payload"), dict) and "id" in ev["payload"]:
            journalled.add(digest(ev["payload"]))
    missing = sorted(record_hashes - journalled)
    orphan = sorted(journalled - record_hashes)
    return {
        "records": len(record_hashes),
        "journalled": len(journalled),
        "missing_from_journal": len(missing),
        "orphan_in_journal": len(orphan),
        "equivalent": not missing and not orphan,
        "domains": len(store.all_current()),
    }
