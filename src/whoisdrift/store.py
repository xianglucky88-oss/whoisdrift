
"""Real sqlite3 store: content-hash primary key, INSERT-OR-IGNORE dedup, watermark.

The original ``records`` table stays exactly as it was: its primary key is the
content hash and re-inserting identical content is a no-op, so existing
databases and their hashes remain compatible.

New tables add an auditable trail without changing those semantics:

``status_events``
    every raw observation received from a source, with its event time, the
    observation sequence/timestamp, and a stable ``event_key``;
``status_decisions``
    every resolution decision (applied / rejected / unconfirmed) with the
    machine-readable ``reason`` and human readable ``detail``;
``status_current``
    the materialised current state per domain, refreshed from the event log so
    the current state only ever moves forward in event time;
``domain_state_history``
    an append-only per-domain state log (effective changes only).
"""

import hashlib
import json
import sqlite3

from .config import DB_PATH

SCHEMA = (
    "CREATE TABLE IF NOT EXISTS records("
    "content_hash TEXT PRIMARY KEY, id TEXT, ts TEXT, payload TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT)",
    "CREATE TABLE IF NOT EXISTS status_events("
    "event_key TEXT PRIMARY KEY, domain_id TEXT, event_ts TEXT, raw_status TEXT,"
    "state TEXT, valid INTEGER, failure TEXT, source TEXT, observed_seq INTEGER,"
    "observed_at TEXT, batch TEXT, record_hash TEXT, payload TEXT)",
    "CREATE TABLE IF NOT EXISTS status_decisions("
    "seq INTEGER PRIMARY KEY AUTOINCREMENT, event_key TEXT, domain_id TEXT,"
    "event_ts TEXT, raw_status TEXT, state TEXT, outcome TEXT, reason TEXT,"
    "detail TEXT, from_state TEXT, to_state TEXT, applied INTEGER,"
    "observed_seq INTEGER, source TEXT, rules_version TEXT, run_id TEXT,"
    "late_arrival INTEGER DEFAULT 0,"
    "UNIQUE(event_key, run_id))",
    "CREATE TABLE IF NOT EXISTS status_current("
    "domain_id TEXT PRIMARY KEY, state TEXT, state_ts TEXT, confirmed INTEGER,"
    "terminal INTEGER, reason TEXT, detail TEXT, last_seq INTEGER,"
    "rules_version TEXT, updated_run TEXT)",
    "CREATE TABLE IF NOT EXISTS domain_state_history("
    "id INTEGER PRIMARY KEY AUTOINCREMENT, domain_id TEXT, event_key TEXT,"
    "state TEXT, state_ts TEXT, from_state TEXT, reason TEXT, to_state TEXT,"
    "rules_version TEXT, run_id TEXT, observed_seq INTEGER)",
    "CREATE INDEX IF NOT EXISTS idx_events_domain_ts "
    "ON status_events(domain_id, event_ts)",
    "CREATE INDEX IF NOT EXISTS idx_decisions_domain_seq "
    "ON status_decisions(domain_id, observed_seq)",
    "CREATE INDEX IF NOT EXISTS idx_history_domain_ts "
    "ON domain_state_history(domain_id, state_ts)",
)


class Store:
    def __init__(self, con=None):
        self.con = con
        if con is not None:
            self._init()

    def connect(self):
        con = sqlite3.connect(DB_PATH)
        self.__init__(con)
        return con

    def _init(self):
        self.con.execute(
            "CREATE TABLE IF NOT EXISTS records("
            "content_hash TEXT PRIMARY KEY, id TEXT, ts TEXT, payload TEXT NOT NULL)")
        self.con.execute(
            "CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT)")
        for stmt in SCHEMA:
            self.con.execute(stmt)
        # Forward-migrate journals created by earlier versions of this module.
        cols = {r[1] for r in self.con.execute("PRAGMA table_info(status_events)")}
        if "batch" not in cols:
            self.con.execute("ALTER TABLE status_events ADD COLUMN batch TEXT")
        dcols = {r[1] for r in self.con.execute("PRAGMA table_info(status_decisions)")}
        if "late_arrival" not in dcols:
            self.con.execute(
                "ALTER TABLE status_decisions ADD COLUMN late_arrival INTEGER DEFAULT 0")
        self.con.commit()

    # ------------------------------------------------------------------ #
    # Original content-hash API (unchanged behaviour).
    # ------------------------------------------------------------------ #
    @staticmethod
    def hash(record):
        blob = json.dumps(record, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(blob.encode()).hexdigest()[:24]

    def upsert(self, record):
        cur = self.con.execute(
            "INSERT OR IGNORE INTO records(content_hash,id,ts,payload) VALUES(?,?,?,?)",
            (self.hash(record), str(record.get("id", "")),
             str(record.get("ts", "")), json.dumps(record, ensure_ascii=False)))
        self.con.commit()
        return cur.rowcount > 0

    def count(self):
        return self.con.execute("SELECT COUNT(*) FROM records").fetchone()[0]

    def all_records(self):
        return [json.loads(p) for (_h, p) in
                self.con.execute("SELECT content_hash,payload FROM records")]

    def get(self, content_hash):
        row = self.con.execute(
            "SELECT payload FROM records WHERE content_hash=?", (content_hash,)).fetchone()
        return json.loads(row[0]) if row else None

    def watermark(self):
        row = self.con.execute("SELECT v FROM meta WHERE k='watermark'").fetchone()
        return row[0] if row else None

    def set_watermark(self, value):
        self.con.execute(
            "INSERT INTO meta(k,v) VALUES('watermark',?) "
            "ON CONFLICT(k) DO UPDATE SET v=excluded.v", (str(value),))
        self.con.commit()

    def reset(self):
        self.con.execute("DELETE FROM records")
        self.con.execute("DELETE FROM meta")
        for table in ("status_events", "status_decisions", "status_current",
                      "domain_state_history"):
            try:
                self.con.execute("DELETE FROM %s" % table)
            except sqlite3.OperationalError:
                pass
        self.con.commit()

    # ------------------------------------------------------------------ #
    # Generic metadata / deterministic counters.
    # ------------------------------------------------------------------ #
    def meta_get(self, key, default=None):
        row = self.con.execute("SELECT v FROM meta WHERE k=?", (str(key),)).fetchone()
        return row[0] if row else default

    def meta_set(self, key, value):
        self.con.execute(
            "INSERT INTO meta(k,v) VALUES(?,?) "
            "ON CONFLICT(k) DO UPDATE SET v=excluded.v", (str(key), str(value)))
        self.con.commit()

    def next_counter(self, key):
        value = int(self.meta_get(key, "0") or 0) + 1
        self.meta_set(key, value)
        return value

    def has_hash(self, content_hash):
        return self.con.execute(
            "SELECT 1 FROM records WHERE content_hash=?",
            (str(content_hash),)).fetchone() is not None

    def hashes(self):
        return {r[0] for r in self.con.execute("SELECT content_hash FROM records")}

    # ------------------------------------------------------------------ #
    # Event journal.
    # ------------------------------------------------------------------ #
    @staticmethod
    def _event_row(ev):
        return (
            str(ev.get("event_key")),
            str(ev.get("id", "")),
            str(ev.get("ts", "")),
            str(ev.get("raw_status", "")),
            str(ev.get("state", "unknown")),
            int(bool(ev.get("valid"))),
            str(ev.get("failure") or ""),
            str(ev.get("source", "")),
            ev.get("observed_seq"),
            str(ev.get("observed_at") or ""),
            None if ev.get("batch") is None else str(ev.get("batch")),
            ev.get("record_hash"),
            json.dumps(ev.get("payload"), ensure_ascii=False, sort_keys=True)
            if ev.get("payload") is not None else None,
        )

    def record_event(self, ev):
        """Insert a raw observation if its stable identity is new.

        Returns ``(inserted, is_repeat)``.  Repeats are remembered so the
        decision log can explain them as ``duplicate_event``.
        """
        cur = self.con.execute(
            "INSERT OR IGNORE INTO status_events(event_key,domain_id,event_ts,"
            "raw_status,state,valid,failure,source,observed_seq,observed_at,"
            "batch,record_hash,payload) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
            self._event_row(ev))
        self.con.commit()
        inserted = cur.rowcount > 0
        return inserted, not inserted

    def update_event_classification(self, event_key, state, valid):
        self.con.execute(
            "UPDATE status_events SET state=?, valid=? WHERE event_key=?",
            (str(state), int(bool(valid)), str(event_key)))
        self.con.commit()

    def events(self, domain_id=None):
        sql = ("SELECT event_key,domain_id,event_ts,raw_status,state,valid,"
               "failure,source,observed_seq,observed_at,batch,record_hash,payload "
               "FROM status_events")
        args = ()
        if domain_id is not None:
            sql += " WHERE domain_id=?"
            args = (str(domain_id),)
        sql += " ORDER BY rowid"
        rows = self.con.execute(sql, args).fetchall()
        out = []
        for (key, rid, ts, raw, state, valid, failure, source, seq,
             observed_at, batch, rec_hash, payload) in rows:
            out.append({
                "event_key": key, "id": rid, "ts": ts, "raw_status": raw,
                "state": state, "valid": int(valid or 0), "failure": failure,
                "source": source, "observed_seq": seq,
                "observed_at": observed_at, "batch": batch,
                "record_hash": rec_hash,
                "payload": json.loads(payload) if payload else None,
            })
        return out

    def event_count(self):
        return self.con.execute("SELECT COUNT(*) FROM status_events").fetchone()[0]

    def event_record_hashes(self):
        return {r[0] for r in self.con.execute(
            "SELECT DISTINCT record_hash FROM status_events "
            "WHERE record_hash IS NOT NULL")}

    # ------------------------------------------------------------------ #
    # Decision + state journal.
    # ------------------------------------------------------------------ #
    def decision_keys(self, domain_id=None):
        """Set of ``(event_key, reason)`` decisions already journaled."""
        sql = "SELECT event_key, reason FROM status_decisions"
        args = ()
        if domain_id is not None:
            sql += " WHERE domain_id=?"
            args = (str(domain_id),)
        return {(r[0], r[1]) for r in self.con.execute(sql, args)}

    def history_event_keys(self, domain_id=None):
        sql = "SELECT event_key FROM domain_state_history"
        args = ()
        if domain_id is not None:
            sql += " WHERE domain_id=?"
            args = (str(domain_id),)
        return {r[0] for r in self.con.execute(sql, args)}

    def record_decisions(self, decisions, rules_version, run_id):
        for d in decisions:
            self.con.execute(
                "INSERT OR IGNORE INTO status_decisions(event_key,domain_id,"
                "event_ts,raw_status,state,outcome,reason,detail,from_state,"
                "to_state,applied,observed_seq,source,rules_version,run_id,"
                "late_arrival) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (str(d.get("event_key")), str(d.get("id", "")),
                 str(d.get("ts", "")), str(d.get("raw_status", "")),
                 str(d.get("state", "")), str(d.get("outcome", "")),
                 str(d.get("reason", "")), str(d.get("detail", "")),
                 None if d.get("from_state") is None else str(d.get("from_state")),
                 None if d.get("to_state") is None else str(d.get("to_state")),
                 int(bool(d.get("applied"))), d.get("observed_seq"),
                 str(d.get("source", "")), str(rules_version), str(run_id),
                 int(bool(d.get("late_arrival")))))
        self.con.commit()

    def decisions(self, domain_id=None, outcome=None):
        sql = ("SELECT event_key,domain_id,event_ts,raw_status,state,outcome,"
               "reason,detail,from_state,to_state,applied,observed_seq,source,"
               "rules_version,run_id,late_arrival FROM status_decisions")
        clauses, args = [], []
        if domain_id is not None:
            clauses.append("domain_id=?")
            args.append(str(domain_id))
        if outcome is not None:
            clauses.append("outcome=?")
            args.append(str(outcome))
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY seq"
        cols = ("event_key", "id", "ts", "raw_status", "state", "outcome",
                "reason", "detail", "from_state", "to_state", "applied",
                "observed_seq", "source", "rules_version", "run_id",
                "late_arrival")
        out = []
        for row in self.con.execute(sql, args):
            d = dict(zip(cols, row))
            d["applied"] = bool(d["applied"])
            d["late_arrival"] = bool(d["late_arrival"])
            out.append(d)
        return out

    def decision_count(self):
        return self.con.execute("SELECT COUNT(*) FROM status_decisions").fetchone()[0]

    def decision_counts(self, run_id=None):
        """Count decisions by outcome and reason (optionally for one run)."""
        sql = "SELECT outcome, reason, COUNT(*) FROM status_decisions"
        args = ()
        if run_id is not None:
            sql += " WHERE run_id=?"
            args = (str(run_id),)
        sql += " GROUP BY outcome, reason"
        by_outcome, by_reason = {}, {}
        for outcome, reason, n in self.con.execute(sql, args):
            by_outcome[outcome] = by_outcome.get(outcome, 0) + n
            by_reason[reason] = by_reason.get(reason, 0) + n
        return {"applied": by_outcome.get("applied", 0),
                "rejected": by_outcome.get("rejected", 0),
                "unconfirmed": by_outcome.get("unconfirmed", 0),
                "by_reason": by_reason}

    def set_current(self, state, run_id=None, previous=None,
                    allow_rollback=False):
        """Persist the materialised current state, refusing to move it backwards.

        ``previous`` is the stored state dict (or ``None``).  A regression is
        refused when the incoming state is *older in event time*, or when it is
        at the same event time but has lower rule precedence (the typical effect
        of an adjusted status mapping).  The stored value is kept and the
        returned dict explains why (``stale_recompute``).
        ``allow_rollback`` is an explicit opt-in used only by maintenance code.
        """
        from . import kernel
        if previous is not None and not allow_rollback:
            new_key = kernel.ts_key(state.get("state_ts"))
            old_key = kernel.ts_key(previous.get("state_ts"))
            if state.get("confirmed") and previous.get("confirmed"):
                regressed = new_key < old_key
                if not regressed and new_key == old_key:
                    new_rank = kernel.precedence(state.get("state"))
                    old_rank = kernel.precedence(previous.get("state"))
                    regressed = new_rank < old_rank
                if regressed:
                    kept = dict(previous)
                    kept["reason"] = "stale_recompute"
                    kept["detail"] = kernel.REASON_DETAIL["stale_recompute"]
                    kept["rejected_state"] = state.get("state")
                    kept["rejected_state_ts"] = state.get("state_ts")
                    return kept
        self.con.execute(
            "INSERT INTO status_current(domain_id,state,state_ts,confirmed,"
            "terminal,reason,detail,last_seq,rules_version,updated_run) "
            "VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(domain_id) DO UPDATE SET "
            "state=excluded.state, state_ts=excluded.state_ts, "
            "confirmed=excluded.confirmed, terminal=excluded.terminal, "
            "reason=excluded.reason, detail=excluded.detail, "
            "last_seq=excluded.last_seq, rules_version=excluded.rules_version, "
            "updated_run=excluded.updated_run",
            (str(state.get("id", "")), str(state.get("state", "unknown")),
             None if state.get("state_ts") is None else str(state.get("state_ts")),
             int(bool(state.get("confirmed"))), int(bool(state.get("terminal"))),
             str(state.get("reason", "")), str(state.get("detail", "")),
             state.get("last_seq"), str(state.get("rules_version", "")),
             None if run_id is None else str(run_id)))
        self.con.commit()
        return state

    def current(self, domain_id):
        row = self.con.execute(
            "SELECT domain_id,state,state_ts,confirmed,terminal,reason,detail,"
            "last_seq,rules_version,updated_run FROM status_current "
            "WHERE domain_id=?", (str(domain_id),)).fetchone()
        if not row:
            return None
        cols = ("id", "state", "state_ts", "confirmed", "terminal", "reason",
                "detail", "last_seq", "rules_version", "updated_run")
        d = dict(zip(cols, row))
        d["confirmed"] = bool(d["confirmed"])
        d["terminal"] = bool(d["terminal"])
        return d

    def all_current(self):
        return [d for d in (self.current(r[0]) for r in self.con.execute(
            "SELECT domain_id FROM status_current ORDER BY domain_id")) if d]

    def record_history(self, domain_id, decision, from_state, rules_version, run_id):
        self.con.execute(
            "INSERT INTO domain_state_history(domain_id,event_key,state,state_ts,"
            "from_state,reason,to_state,rules_version,run_id,observed_seq) "
            "VALUES(?,?,?,?,?,?,?,?,?,?)",
            (str(domain_id), str(decision.get("event_key", "")),
             str(decision.get("state", "")), str(decision.get("ts", "")),
             None if from_state is None else str(from_state),
             str(decision.get("reason", "")), str(decision.get("to_state", "")),
             str(rules_version), str(run_id), decision.get("observed_seq")))
        self.con.commit()

    def history(self, domain_id=None):
        sql = ("SELECT domain_id,event_key,state,state_ts,from_state,reason,"
               "to_state,rules_version,run_id,observed_seq FROM domain_state_history")
        args = ()
        if domain_id is not None:
            sql += " WHERE domain_id=?"
            args = (str(domain_id),)
        sql += " ORDER BY id"
        cols = ("id", "event_key", "state", "state_ts", "from_state", "reason",
                "to_state", "rules_version", "run_id", "observed_seq")
        return [dict(zip(cols, row)) for row in self.con.execute(sql, args)]

    def clear_history(self, domain_id=None):
        if domain_id is None:
            self.con.execute("DELETE FROM domain_state_history")
        else:
            self.con.execute("DELETE FROM domain_state_history WHERE domain_id=?",
                             (str(domain_id),))
        self.con.commit()
