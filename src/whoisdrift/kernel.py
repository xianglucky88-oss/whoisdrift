
"""Domain status state machine with monotonic resolution and auditable reasons.

The original public ``process``/``reduce`` helpers are kept byte-for-byte
compatible with the previous implementation so stored content hashes, existing
databases and the offline fixture keep working unchanged.

New code should use :func:`resolve_domain`, :func:`resolve` and
:func:`state_at`.  They implement an event-time ordered state machine:

* a report older than the latest confirmed report is rejected as
  ``stale_event`` and can never drag the current state back in time;
* a report that repeats an already-seen (domain, event time, status) identity is
  reported as ``duplicate_report`` and changes nothing;
* reports without a usable timestamp or with an unknown status are recorded as
  *unconfirmed observations* – a failed, blocked, offline or unreadable
  observation is never interpreted as a deletion;
* a terminal state (``archived``, produced by an explicit ``deleted`` report)
  forms a boundary: a later contradictory report is rejected as
  ``terminal_boundary`` instead of resurrecting the domain, while a later
  reconfirmation of the terminal state is still accepted;
* every decision carries a machine-readable ``reason`` plus a human readable
  ``detail``, so effective changes, unconfirmed observations, rejected events
  and the termination boundary can all be explained to callers.

All helpers are pure: they never mutate the input events.  Historical ("as of")
queries therefore cannot change the current state.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone

# --------------------------------------------------------------------------- #
# Rule tables (kept identical to the original implementation).
# --------------------------------------------------------------------------- #
TRANS = {"new": "pending", "draft": "pending", "active": "active",
         "pending": "pending", "sold": "closed", "withdrawn": "closed",
         "deleted": "archived"}
ORDER = {"unknown": 0, "pending": 1, "active": 2, "closed": 3, "archived": 4}

RULES_VERSION = "2026-11-18.1"

#: States that close a domain's lifecycle.  A later report must not resurrect it.
TERMINAL_STATES = frozenset({"archived"})

#: Reasons understood by the pipeline, grouped by outcome category.
REASON_DETAIL = {
    # effective changes
    "initial_state":
        "first confirmed status report establishes the current state",
    "status_changed":
        "a newer report changed the confirmed state",
    "confirmed_same_state":
        "a newer report reconfirmed the same state and advanced the freshness anchor",
    "conflict_resolved":
        "a conflicting report at the same event timestamp won deterministic "
        "precedence and set the confirmed state",
    # rejected
    "stale_event":
        "report is older than the latest confirmed report; ignored so a late "
        "delivery cannot drag the current state back in time",
    "duplicate_report":
        "the same domain, event timestamp and status were already recorded",
    "duplicate_event":
        "this report was already recorded in a previous run",
    "conflict_same_timestamp":
        "a conflicting report at the same event timestamp lost deterministic "
        "precedence; the confirmed state is unchanged",
    "terminal_boundary":
        "the domain already reached a terminal state; a later report is recorded "
        "but cannot resurrect it (termination boundary)",
    "stale_recompute":
        "recomputation would move the confirmed state back in time; retained the "
        "newer confirmed state",
    # unconfirmed observations
    "unknown_status":
        "status value is not in the rule table; the observation cannot confirm a "
        "state (a failed or unreadable observation is not evidence of deletion)",
    "missing_timestamp":
        "report has no usable event timestamp; it cannot be ordered against the "
        "confirmed history and is kept as an unconfirmed observation",
    "missing_domain_id":
        "report has no domain id; it cannot be attributed and is unconfirmed",
    "source_unavailable":
        "the data source could not be reached; this is an availability failure, "
        "not evidence of deletion",
    "access_blocked":
        "the data source rejected the request (authentication/authorisation); "
        "this is not evidence of deletion",
    "rate_limited":
        "the data source throttled the request; this is not evidence of deletion",
    "missing_page":
        "a page of the result set was missing; absence from an incomplete "
        "collection is not evidence of deletion",
    "source_error":
        "the data source returned an error; this is not evidence of deletion",
    "unreadable_response":
        "the response could not be parsed; this is not evidence of deletion",
    "never_observed":
        "the domain was never observed; a missing observation does not imply "
        "deletion",
    # boundaries / bookkeeping
    "rule_adjustment":
        "current state recomputed under adjusted rules; the freshness anchor was "
        "not moved back in time",
    "no_confirmed_observation":
        "no confirmed observation exists for this domain yet",
    "no_observation_before_as_of":
        "no confirmed observation exists at or before the requested time",
    "as_of_cutoff":
        "state as of the requested time; later events are excluded from this "
        "historical view",
    "terminal_state":
        "domain reached a terminal state; later contradictory reports are "
        "rejected at this boundary",
    "open":
        "domain remains in a non-terminal state",
}

#: Failure kinds accepted by :func:`failure_reason`.
FAILURE_KINDS = {
    "offline": "source_unavailable",
    "network": "source_unavailable",
    "timeout": "source_unavailable",
    "unavailable": "source_unavailable",
    "blocked": "access_blocked",
    "forbidden": "access_blocked",
    "unauthorized": "access_blocked",
    "rate_limited": "rate_limited",
    "throttled": "rate_limited",
    "missing_page": "missing_page",
    "page_missing": "missing_page",
    "short_page": "missing_page",
    "not_found": "missing_page",
    "source_error": "source_error",
    "server_error": "source_error",
    "unreadable": "unreadable_response",
    "parse_error": "unreadable_response",
}


# --------------------------------------------------------------------------- #
# Legacy helpers – unchanged so stored content hashes stay compatible.
# --------------------------------------------------------------------------- #
def process(record):
    s = str(record.get("status", "")).lower()
    state = TRANS.get(s, "unknown")
    return {"state_norm": state, "invalid_status": int(s not in TRANS)}


def reduce(records):
    last = {}
    for r in sorted(records, key=lambda x: (str(x.get("id")), str(x.get("ts", "")))):
        rid = r.get("id"); st = r.get("state_norm", "unknown")
        r["regression"] = int(rid in last and ORDER.get(st, 0) < ORDER.get(last[rid], 0))
        if ORDER.get(st, 0) >= ORDER.get(last.get(rid, "unknown"), 0):
            last[rid] = st
    return records


# --------------------------------------------------------------------------- #
# Rules.
# --------------------------------------------------------------------------- #
def make_rules(overrides=None):
    """Return a normalised rule mapping.

    ``overrides`` may contain ``mapping`` (raw status -> state), ``order``
    (state -> precedence), ``terminal`` (states that end the lifecycle) and
    ``allow_resurrection``.  Unknown raw statuses are deliberately *not*
    merged with the defaults, so callers that adjust rules get exactly what
    they ask for and the effect is auditable.
    """
    rules = {
        "mapping": dict(TRANS),
        "order": dict(ORDER),
        "terminal": set(TERMINAL_STATES),
        "allow_resurrection": False,
    }
    if overrides:
        if overrides.get("mapping") is not None:
            rules["mapping"] = dict(overrides["mapping"])
        if overrides.get("order") is not None:
            rules["order"] = dict(overrides["order"])
        if overrides.get("terminal") is not None:
            rules["terminal"] = set(overrides["terminal"])
        if "allow_resurrection" in overrides:
            rules["allow_resurrection"] = bool(overrides["allow_resurrection"])
    rules["version"] = str((overrides or {}).get("version", RULES_VERSION))
    return rules


def classify(raw_status, rules=None):
    """Map a raw source status to ``(normalized_state, is_valid)``."""
    rules = rules or make_rules()
    s = "" if raw_status is None else str(raw_status).strip().lower()
    state = rules["mapping"].get(s)
    if state is None:
        return "unknown", False
    return state, True


def precedence(state, rules=None):
    rules = rules or make_rules()
    return rules["order"].get(state, 0)


def failure_reason(kind):
    """Map a failure kind to the audit reason used in the decision log."""
    return FAILURE_KINDS.get(str(kind).strip().lower(), "source_error")


# --------------------------------------------------------------------------- #
# Event identity and ordering.
# --------------------------------------------------------------------------- #
def parse_ts(value):
    """Parse an ISO-8601 timestamp (or epoch) into an aware UTC datetime."""
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s[:-1] + "+00:00" if s.endswith("Z") else s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def ts_key(value):
    """Return a totally ordered, deterministic key for an event timestamp.

    ISO-8601 timestamps and numeric epochs compare by instant.  Other non-empty
    strings fall back to deterministic lexical ordering (handy for compact
    fixtures such as ``t1``/``t2``) and always sort before real timestamps.
    Missing values sort first and are reported as unconfirmed.
    """
    dt = parse_ts(value)
    if dt is not None:
        return (2, dt.timestamp())
    s = "" if value is None else str(value).strip()
    if not s:
        return (0, 0.0)
    try:
        return (2, float(s))
    except ValueError:
        return (1, s)


def has_timestamp(value):
    return ts_key(value)[0] > 0


def event_key(domain_id, ts, raw_status):
    """Stable identity for a status report (domain + event time + status)."""
    blob = "%s\x1f%s\x1f%s" % (domain_id, "" if ts is None else ts,
                               "" if raw_status is None else raw_status)
    return hashlib.sha256(blob.encode()).hexdigest()[:24]


def event_identity(ev):
    return (ev["id"], ts_key(ev["ts"]), ev["raw_status"])


def prepare_event(event, rules=None):
    """Normalise an event/record dict without mutating the input."""
    rules = rules or make_rules()
    d = dict(event or {})
    rid = d.get("id", d.get("domain", d.get("domain_id", "")))
    rid = "" if rid is None else str(rid)
    ts = d.get("ts", d.get("timestamp", d.get("event_ts", "")))
    ts = "" if ts is None else str(ts)
    raw = d.get("raw_status", d.get("status", ""))
    raw = "" if raw is None else str(raw)
    state, valid = classify(raw, rules)
    key = d.get("event_key") or event_key(rid, ts, raw)
    seq = d.get("observed_seq", d.get("seq"))
    observed_at = d.get("observed_at", d.get("seen_at", ""))
    failure = d.get("failure")
    if failure:
        # An availability/authorisation failure has no status value; it is an
        # unconfirmed observation, never a deletion.
        state, valid, raw = "unknown", False, ""
        key = d.get("event_key") or "failure:%s:%s" % (rid, key)
    return {
        "id": rid,
        "ts": ts,
        "raw_status": raw,
        "state": state,
        "valid": int(bool(valid)),
        "failure": "" if not failure else str(failure),
        "event_key": str(key),
        "observed_seq": seq,
        "observed_at": "" if observed_at is None else str(observed_at),
        "batch": d.get("batch"),
        "payload": d.get("payload"),
        "record_hash": d.get("record_hash"),
        "source": d.get("source", "failure" if failure else "event"),
    }


def source_view(record):
    """Return the source-level view of a record (kernel-derived keys removed).

    The kernel adds ``state_norm``/``invalid_status``/``regression`` as derived
    fields.  They depend on the batch a record was processed in, so they are not
    part of the record's source identity and are stripped when comparing full
    and incremental collection.
    """
    derived = {"state_norm", "invalid_status", "regression"}
    return {k: v for k, v in (record or {}).items() if k not in derived}


def failure_event(kind, domain_id="", observed_seq=None, observed_at="",
                  status=None, page=None, detail=None):
    """Build an audit event describing a failure to *observe* a domain.

    ``kind`` is one of ``offline``, ``blocked``, ``missing_page``, ``timeout``,
    ``rate_limited``, ``unreadable`` ... (see :data:`FAILURE_KINDS`).  The event
    is deliberately unconfirmed: a failed RDAP request, a block, an offline
    source or a missing page never means the domain was deleted.
    """
    payload = {"status": status, "page": page, "detail": detail, "kind": kind}
    return {
        "id": "" if domain_id is None else str(domain_id),
        "ts": "",
        "raw_status": "",
        "failure": str(kind),
        "source": "failure",
        "observed_seq": observed_seq,
        "observed_at": "" if observed_at is None else str(observed_at),
        "payload": payload,
        "event_key": "failure:%s:%s:%s:%s" % (
            domain_id or "", observed_seq if observed_seq is not None else "",
            page if page is not None else "", kind),
    }


def event_from_record(record, source="crawl", observed_seq=None, observed_at="",
                      batch=None):
    """Build a raw event dict from a source record."""
    return {
        "id": "" if record.get("id") is None else str(record.get("id")),
        "ts": "" if record.get("ts") is None else str(record.get("ts")),
        "raw_status": "" if record.get("status") is None else str(record.get("status")),
        "source": source,
        "observed_seq": observed_seq,
        "observed_at": observed_at,
        "batch": batch,
        "payload": dict(record),
    }


# --------------------------------------------------------------------------- #
# Resolution.
# --------------------------------------------------------------------------- #
def _state_dict(state, ts, confirmed, terminal, reason, seq=None,
                rules_version=None):
    return {
        "state": state,
        "state_ts": ts,
        "confirmed": bool(confirmed),
        "terminal": bool(terminal),
        "reason": reason,
        "detail": REASON_DETAIL.get(reason, ""),
        "last_seq": seq,
        "rules_version": rules_version or RULES_VERSION,
    }


def _decision(ev, outcome, reason, from_state=None, to_state=None,
              applied=False, extra=None, late_arrival=False):
    d = {
        "id": ev["id"],
        "event_key": ev["event_key"],
        "ts": ev["ts"],
        "raw_status": ev["raw_status"],
        "state": ev["state"],
        "outcome": outcome,
        "reason": reason,
        "detail": REASON_DETAIL.get(reason, ""),
        "applied": bool(applied),
        "from_state": from_state,
        "to_state": to_state,
        "observed_seq": ev.get("observed_seq"),
        "source": ev.get("source"),
        "late_arrival": bool(late_arrival),
    }
    if extra:
        d.update(extra)
    return d


def _arrival_order(events, rules):
    """Prepare events and order them by observation batch, then event time.

    Ordering has two levels:

    * **Across batches** (``batch`` = the collection snapshot that observed the
      report) events are processed in the order the snapshots arrived.  A
      late-delivered old report therefore arrives *after* the newer state it
      contradicts and is rejected as ``stale_event`` instead of dragging the
      current state back in time.
    * **Within one batch** reports are ordered by event time.  A single
      collection is an unordered snapshot, so ordering its rows chronologically
      keeps out-of-order pages from being mistaken for stale data.

    Without any batch information the input order is taken as arrival order,
    which gives callers handling a stream the obvious semantics.  The sort is
    stable, so ties keep a deterministic order.
    """
    prepared = [prepare_event(e, rules) for e in events]
    has_batch = any(ev.get("batch") is not None for ev in prepared)
    if not has_batch:
        return prepared

    def key(ev):
        batch = ev.get("batch")
        return (batch is None, "" if batch is None else str(batch),
                ts_key(ev["ts"]), precedence(ev["state"], rules),
                ev["raw_status"], ev["event_key"])

    prepared.sort(key=key)
    return prepared

    prepared.sort(key=key)
    return prepared


def _late_arrivals(prepared):
    """Mark reports whose event time precedes the newest time of an earlier batch.

    The marker is explanatory only: it says "this report was delivered after we
    already knew about a newer report", which is exactly the case the user cares
    about when a late notification arrives.  Reports that are simply out of order
    *within* one snapshot are not flagged, because a snapshot has no inherent
    order.
    """
    newest_per_batch = {}
    flags = []
    for ev in prepared:
        batch = ev.get("batch")
        if not has_timestamp(ev["ts"]):
            flags.append(False)
            continue
        key = ts_key(ev["ts"])
        earlier_max = None
        for other_batch, other_max in newest_per_batch.items():
            if other_batch == batch:
                continue
            if earlier_max is None or other_max > earlier_max:
                earlier_max = other_max
        flags.append(earlier_max is not None and key < earlier_max)
        current = newest_per_batch.get(batch)
        if current is None or key > current:
            newest_per_batch[batch] = key
    return flags


def resolve_domain(events, rules=None):
    """Resolve the status history of one domain.

    The input is never mutated.  Each report is compared with the state already
    confirmed by earlier reports:

    * a report older than that state is ``stale_event`` (rejected);
    * a repeat of an already-seen identity is ``duplicate_report``;
    * a report without a usable timestamp or status is ``unconfirmed``;
    * a contradictory report after a terminal state is ``terminal_boundary``.

    The result contains the final confirmed state, every per-event decision,
    outcome groupings, counts and an explicit termination boundary description.
    """
    rules = rules or make_rules()
    prepared = _arrival_order(events, rules)
    late_flags = _late_arrivals(prepared)

    current = None
    decisions = []
    seen = set()
    first_id = next((ev["id"] for ev in prepared if ev["id"]), "")

    def decide(ev, index, outcome, reason, **kw):
        kw.setdefault("late_arrival", late_flags[index])
        decisions.append(_decision(ev, outcome, reason, **kw))

    for index, ev in enumerate(prepared):
        if ev.get("failure"):
            if ev["event_key"] in seen:
                decide(ev, index, "rejected", "duplicate_event",
                       from_state=current["state"] if current else "unknown",
                       to_state=current["state"] if current else "unknown")
                continue
            seen.add(ev["event_key"])
            reason = failure_reason(ev["failure"])
            decide(ev, index, "unconfirmed", reason,
                   from_state=current["state"] if current else "unknown",
                   to_state=current["state"] if current else "unknown")
            continue
        if not ev["id"]:
            decide(ev, index, "unconfirmed", "missing_domain_id")
            continue
        if not ev["valid"]:
            decide(ev, index, "unconfirmed", "unknown_status")
            continue
        if not has_timestamp(ev["ts"]):
            decide(ev, index, "unconfirmed", "missing_timestamp")
            continue

        identity = event_identity(ev)
        if identity in seen:
            decide(ev, index, "rejected", "duplicate_report",
                   from_state=current["state"] if current else "unknown",
                   to_state=current["state"] if current else "unknown")
            continue
        seen.add(identity)

        if current is None:
            current = _state_dict(ev["state"], ev["ts"], True,
                                  ev["state"] in rules["terminal"],
                                  "initial_state", ev.get("observed_seq"),
                                  rules["version"])
            decide(ev, index, "applied", "initial_state", from_state="unknown",
                   to_state=ev["state"], applied=True)
            continue

        cur_ts, ev_ts = ts_key(current["state_ts"]), ts_key(ev["ts"])

        # 1) Never move backwards in event time.
        if ev_ts < cur_ts:
            decide(ev, index, "rejected", "stale_event",
                   from_state=current["state"], to_state=current["state"])
            continue

        # 2) Same event time: deterministic precedence resolves conflicts.
        if ev_ts == cur_ts:
            if ev["state"] == current["state"]:
                decide(ev, index, "rejected", "duplicate_report",
                       from_state=current["state"], to_state=current["state"])
            elif precedence(ev["state"], rules) > precedence(current["state"], rules):
                old = current["state"]
                current = _state_dict(ev["state"], ev["ts"], True,
                                      ev["state"] in rules["terminal"],
                                      "conflict_resolved",
                                      ev.get("observed_seq"), rules["version"])
                decide(ev, index, "applied", "conflict_resolved", from_state=old,
                       to_state=ev["state"], applied=True)
            else:
                decide(ev, index, "rejected", "conflict_same_timestamp",
                       from_state=current["state"], to_state=current["state"])
            continue

        # 3) Newer event.  A terminal state is a hard boundary for contradictory
        #    reports, but reconfirming the terminal state itself is fine.
        if (current["terminal"] and ev["state"] != current["state"]
                and not rules["allow_resurrection"]):
            decide(ev, index, "rejected", "terminal_boundary",
                   from_state=current["state"], to_state=current["state"])
            continue

        if ev["state"] == current["state"]:
            current = _state_dict(ev["state"], ev["ts"], True,
                                  ev["state"] in rules["terminal"],
                                  "confirmed_same_state",
                                  ev.get("observed_seq"), rules["version"])
            decide(ev, index, "applied", "confirmed_same_state",
                   from_state=ev["state"], to_state=ev["state"], applied=True)
        else:
            old = current["state"]
            current = _state_dict(ev["state"], ev["ts"], True,
                                  ev["state"] in rules["terminal"],
                                  "status_changed",
                                  ev.get("observed_seq"), rules["version"])
            decide(ev, index, "applied", "status_changed", from_state=old,
                   to_state=ev["state"], applied=True)

    if current is None:
        current = _state_dict("unknown", None, False, False,
                              "no_confirmed_observation", None, rules["version"])

    applied = [d for d in decisions if d["outcome"] == "applied"]
    rejected = [d for d in decisions if d["outcome"] == "rejected"]
    unconfirmed = [d for d in decisions if d["outcome"] == "unconfirmed"]
    by_reason = {}
    for d in decisions:
        by_reason[d["reason"]] = by_reason.get(d["reason"], 0) + 1

    if current["terminal"]:
        boundary = {
            "reason": "terminal_state",
            "detail": ("domain reached terminal state %r at %s; later "
                       "contradictory reports are recorded but cannot resurrect "
                       "it" % (current["state"], current["state_ts"])),
            "terminal": True,
        }
    elif current["confirmed"]:
        boundary = {"reason": "open",
                    "detail": REASON_DETAIL["open"], "terminal": False}
    else:
        boundary = {"reason": "no_confirmed_observation",
                    "detail": REASON_DETAIL["no_confirmed_observation"],
                    "terminal": False}

    result = dict(current)
    result.update({
        "id": first_id,
        "decisions": decisions,
        "applied": applied,
        "rejected": rejected,
        "unconfirmed": unconfirmed,
        "counts": {
            "events": len(decisions),
            "applied": len(applied),
            "rejected": len(rejected),
            "unconfirmed": len(unconfirmed),
            "by_reason": by_reason,
        },
        "boundary": {
            **boundary,
            "first_event_ts": prepared[0]["ts"] if prepared else None,
            "last_event_ts": current["state_ts"],
            "excluded_future_events": 0,
        },
        "rules_version": rules["version"],
    })
    return result


def resolve(events, rules=None):
    """Resolve a mixed event stream, grouping by domain id.

    Returns ``{"states": {id: result}, "decisions": [...], ...}``.  ``current``
    is included as a convenience when the stream contains exactly one domain.
    """
    rules = rules or make_rules()
    prepared = [prepare_event(e, rules) for e in events]
    groups, order = {}, []
    for ev in prepared:
        rid = ev["id"]
        if rid not in groups:
            groups[rid] = []
            order.append(rid)
        groups[rid].append(ev)

    states, all_decisions = {}, []
    for rid in order:
        res = resolve_domain(groups[rid], rules)
        states[rid] = res
        all_decisions.extend(res["decisions"])

    applied = [d for d in all_decisions if d["outcome"] == "applied"]
    rejected = [d for d in all_decisions if d["outcome"] == "rejected"]
    unconfirmed = [d for d in all_decisions if d["outcome"] == "unconfirmed"]
    by_reason = {}
    for d in all_decisions:
        by_reason[d["reason"]] = by_reason.get(d["reason"], 0) + 1

    out = {
        "states": states,
        "decisions": all_decisions,
        "applied": applied,
        "rejected": rejected,
        "unconfirmed": unconfirmed,
        "counts": {
            "domains": len(states),
            "events": len(all_decisions),
            "applied": len(applied),
            "rejected": len(rejected),
            "unconfirmed": len(unconfirmed),
            "by_reason": by_reason,
        },
        "rules_version": rules["version"],
    }
    if len(states) == 1:
        out["current"] = next(iter(states.values()))
    return out


# --------------------------------------------------------------------------- #
# Historical (point-in-time) queries – pure, never mutate current state.
# --------------------------------------------------------------------------- #
def _observed_before(ev, as_of):
    seq = ev.get("observed_seq")
    if isinstance(as_of, int):
        return seq is not None and int(seq) <= as_of
    cutoff = str(as_of)
    observed = str(ev.get("observed_at") or "")
    return observed != "" and observed <= cutoff


def state_at(events, as_of, basis="event", rules=None):
    """Return the domain state as it stood at ``as_of`` without writing anything.

    ``basis="event"`` (default) selects reports whose *event timestamp* is at or
    before ``as_of``.  ``basis="observed"`` selects reports that were *known* at
    ``as_of`` (an observation sequence number or observation timestamp), which
    distinguishes "what happened by then" from "what we had seen by then".

    The boundary explains why the view stops at ``as_of`` and reports how many
    later events were excluded.  ``current_state_not_modified`` is always true:
    the query is read-only.
    """
    rules = rules or make_rules()
    prepared = [prepare_event(e, rules) for e in events]
    if basis == "observed":
        selected = [e for e in prepared if _observed_before(e, as_of)]
    elif basis == "event":
        cutoff = ts_key(as_of)
        selected = [e for e in prepared
                    if has_timestamp(e["ts"]) and ts_key(e["ts"]) <= cutoff]
    else:
        raise ValueError("unknown basis: %s" % basis)

    excluded = len(prepared) - len(selected)
    result = resolve_domain(selected, rules)
    result["as_of"] = as_of
    result["basis"] = basis
    result["selected_events"] = len(selected)
    result["excluded_events"] = excluded
    boundary = dict(result["boundary"])
    if not selected:
        boundary.update({
            "reason": "no_observation_before_as_of",
            "detail": REASON_DETAIL["no_observation_before_as_of"],
        })
    elif boundary.get("reason") != "terminal_state":
        # Preserve a terminal boundary if it happened at or before as_of.
        boundary.update({
            "reason": "as_of_cutoff",
            "detail": ("state as of %s; %d later event(s) excluded from this "
                       "historical view" % (as_of, excluded)),
        })
    boundary["excluded_future_events"] = excluded
    boundary["current_state_not_modified"] = True
    result["boundary"] = boundary
    return result


# Common aliases so callers can use natural names without guessing.
state_as_of = state_at
as_of = state_at
resolve_events = resolve
