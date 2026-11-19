"""Backwards compatibility: unchanged records, legacy databases, offline runs."""

import json
import sqlite3

from whoisdrift import pipeline, status
from whoisdrift.framework import Transport, run
from whoisdrift.store import Store


# Legacy databases and unchanged content must keep working exactly as before.
LEGACY_RECORD_SET_SHA = "1859eddd5822ecb7711b360b5a0dfe303f84385ad63ebde3a2454936358d8a02"


def _fresh():
    transport = Transport()
    transport.request("POST", "/control", json_body={"reset": True})
    con = Store().connect()
    store = Store(con)
    store.reset()
    return transport, con, store


def test_full_collection_records_are_unchanged():
    """The records table content must stay byte-identical to the old pipeline."""
    transport, con, store = _fresh()
    try:
        run(mode="full", con=con)
        records = sorted(store.all_records(), key=lambda r: r["id"])
        digest = __import__("hashlib").sha256(
            json.dumps(records, sort_keys=True).encode()).hexdigest()
        assert digest == LEGACY_RECORD_SET_SHA
        assert store.count() == 9
    finally:
        transport.request("POST", "/control", json_body={"reset": True})
        store.reset()
        con.close()


def test_legacy_database_upgrades_in_place(tmp_path):
    path = tmp_path / "legacy.db"
    con = sqlite3.connect(str(path))
    con.execute("CREATE TABLE records(content_hash TEXT PRIMARY KEY, id TEXT, "
                "ts TEXT, payload TEXT NOT NULL)")
    con.execute("CREATE TABLE meta(k TEXT PRIMARY KEY, v TEXT)")
    record = {"id": "legacy-1", "ts": "2030-01-01T00:00:00Z", "value": 1,
              "status": "active", "name": "x", "registrar": "r",
              "state_norm": "active", "invalid_status": 0, "regression": 0}
    con.execute("INSERT INTO records VALUES(?,?,?,?)",
                (Store.hash(record), "legacy-1", record["ts"],
                 json.dumps(record)))
    con.execute("INSERT INTO meta VALUES('watermark','2030-01-01T00:00:00Z')")
    con.commit()

    store = Store(con)  # attaches and adds the new tables without touching old ones
    assert store.count() == 1
    assert store.watermark() == "2030-01-01T00:00:00Z"
    assert store.get(Store.hash(record))["id"] == "legacy-1"
    assert store.upsert(record) is False  # content-hash dedup unchanged
    assert store.event_count() == 0 and store.decision_count() == 0

    # Backfill makes the existing content explainable without changing it.
    summary = pipeline.backfill(con=con)
    assert summary["backfilled_records"] == 1
    assert store.count() == 1
    assert status.current_state(store, "legacy-1")["state"] == "active"
    # Idempotent: backfilling again adds nothing.
    again = pipeline.backfill(con=con)
    assert again["new_events"] == 0
    assert store.event_count() == 1
    con.close()


def test_reprocessing_existing_database_is_safe():
    transport, con, store = _fresh()
    try:
        first = run(mode="full", con=con)
        records_before = sorted(store.all_records(), key=lambda r: r["id"])
        second = run(mode="full", con=con)
        assert second["ingested"] == 0
        assert second["count"] == first["count"]
        assert sorted(store.all_records(), key=lambda r: r["id"]) == records_before
        assert status.compare(store)["equivalent"]
    finally:
        transport.request("POST", "/control", json_body={"reset": True})
        store.reset()
        con.close()


def test_compare_detects_journal_drift():
    transport, con, store = _fresh()
    try:
        run(mode="full", con=con)
        assert status.compare(store)["equivalent"]
        # A record inserted without a journal entry shows up as a difference.
        store.upsert({"id": "manual", "ts": "2031-01-01T00:00:00Z",
                      "status": "active", "name": "manual", "value": 0,
                      "registrar": "manual"})
        report = status.compare(store)
        assert report["equivalent"] is False
        assert report["missing_from_journal"] == 1
    finally:
        transport.request("POST", "/control", json_body={"reset": True})
        store.reset()
        con.close()
