"""Local transport, parsing, storage, and session helpers."""
from __future__ import annotations
import hashlib
import importlib
import json

from lxml import html as _lh

from .config import SPEC
from .store import Store
from . import status


# --------------------------------------------------------------------------- #
# Transport helpers.
# --------------------------------------------------------------------------- #
class Transport:
    """Client for the bundled local service."""

    def __init__(self):
        import os
        import sys
        for p in (str(SPEC_ROOT()), str(SPEC_ROOT() / "src")):
            if p not in sys.path:
                sys.path.insert(0, p)
        from starlette.testclient import TestClient
        app = importlib.import_module("mockapp.main").app
        self.client = TestClient(app)
        self.cookies: dict[str, str] = {}

    def request(self, method, path, params=None, json_body=None, headers=None):
        q = {k: v for k, v in (params or {}).items() if v not in (None, "")}
        resp = self.client.request(method, path, params=q, json=json_body,
                                   headers=headers or {}, cookies=self.cookies)
        for k, v in resp.cookies.items():
            self.cookies[k] = v
        return resp.status_code, dict(resp.headers), resp.text


def SPEC_ROOT():
    from .config import ROOT
    return ROOT


class Sleeper:
    """Records requested delays without blocking the caller."""

    def __init__(self):
        self.slept = 0.0
        self.calls = 0

    def sleep(self, seconds):
        self.slept += seconds
        self.calls += 1


class Http:
    """HTTP client with bounded retry support."""

    def __init__(self, transport: Transport, sleeper: Sleeper,
                 max_retries=4, base_backoff=0.5, politeness=0.25):
        self.t = transport
        self.sleeper = sleeper
        self.max_retries = max_retries
        self.base_backoff = base_backoff
        self.politeness = politeness

    def get(self, path, params=None, headers=None):
        attempt = 0
        while True:
            self.sleeper.sleep(self.politeness)
            status, hdr, body = self.t.request("GET", path, params, headers=headers)
            if status in (429, 500, 502, 503) and attempt < self.max_retries:
                self.sleeper.sleep(self.base_backoff * (2 ** attempt))
                attempt += 1
                continue
            return status, hdr, body, attempt


# --------------------------------------------------------------------------- #
# Response parsing.
# --------------------------------------------------------------------------- #
def _coerce(name, val):
    if name in SPEC.int_fields:
        try:
            return int(val)
        except (TypeError, ValueError):
            return val
    return val


def parse(resp_body, fmt):
    cols = SPEC.columns
    if fmt == "json":
        items = json.loads(resp_body).get("items", [])
        return [{c: _coerce(c, it.get(c)) for c in cols} for it in items]
    if fmt == "html":
        doc = _lh.fromstring(resp_body)
        rows = []
        for tr in doc.xpath("//table//tr[./td]"):
            cells = [td.text_content().strip() for td in tr.xpath("./td")]
            if len(cells) != len(cols):
                continue
            rows.append({c: _coerce(c, v) for c, v in zip(cols, cells)})
        return rows
    if fmt == "text":
        out = []
        for line in resp_body.splitlines():
            if not line.strip():
                continue
            parts = [p.strip() for p in line.split("\t")]
            d = {c: _coerce(c, parts[i]) if i < len(parts) else _coerce(c, "")
                 for i, c in enumerate(cols)}
            out.append(d)
        return out
    raise ValueError("unknown fmt: %s" % fmt)


def _failure_kind(status_code):
    """Classify an HTTP status as a collection failure kind."""
    if status_code in (401, 403):
        return "blocked"
    if status_code == 429:
        return "rate_limited"
    if status_code in (204, 404, 410):
        return "missing_page"
    return "source_error"


def fetch_collect(http: Http):
    """Collect records, reporting partial failures instead of conflating them.

    Returns ``(records, attempts, info)`` where ``info`` contains:

    ``complete``   whether every page up to the end was read;
    ``pages``      number of pages successfully parsed;
    ``failures``   a list of failure descriptors (page, status, reason).

    A failed page stops the collection: the unseen tail is simply *unknown*.
    Callers must not treat an incomplete collection as evidence that a domain
    disappeared (see :func:`whoisdrift.kernel.failure_reason`).
    """
    records, attempts, page = [], 0, 1
    failures = []
    complete = True
    pages = 0
    while True:
        try:
            status, _hdr, body, att = http.get(
                "/" + SPEC.endpoint, params={"page": page})
        except Exception as exc:  # transport/offline failure
            failures.append({"page": page, "status": None, "kind": "offline",
                             "detail": "%s: %s" % (type(exc).__name__, exc)})
            complete = False
            break
        attempts += att + 1
        if status != 200:
            kind = _failure_kind(status)
            failures.append({"page": page, "status": status, "kind": kind,
                             "detail": "page %s returned HTTP %s" % (page, status)})
            complete = False
            break
        try:
            page_recs = parse(body, SPEC.fmt)
        except Exception as exc:  # unreadable/truncated response
            failures.append({"page": page, "status": status,
                             "kind": "unreadable",
                             "detail": "%s: %s" % (type(exc).__name__, exc)})
            complete = False
            break
        pages += 1
        records.extend(page_recs)
        if len(page_recs) < SPEC.page_size:
            # Short page.  Before trusting it as the end, confirm the next page
            # is genuinely empty: a dropped/omitted/short page must not silently
            # look like an end of data (otherwise missing rows look like
            # deletions).  A failure on that confirmation is itself a failure.
            try:
                status2, _h2, body2, att2 = http.get(
                    "/" + SPEC.endpoint, params={"page": page + 1})
            except Exception as exc:
                failures.append({"page": page + 1, "status": None,
                                 "kind": "offline",
                                 "detail": "%s: %s" % (type(exc).__name__, exc)})
                complete = False
                break
            attempts += att2 + 1
            if status2 != 200:
                failures.append({
                    "page": page + 1, "status": status2,
                    "kind": _failure_kind(status2),
                    "detail": "end-of-data confirmation for page %s returned "
                              "HTTP %s" % (page, status2)})
                complete = False
                break
            try:
                next_recs = parse(body2, SPEC.fmt)
            except Exception as exc:
                failures.append({"page": page + 1, "status": status2,
                                 "kind": "unreadable",
                                 "detail": "%s: %s" % (type(exc).__name__, exc)})
                complete = False
                break
            if not next_recs:
                break
            failures.append({"page": page, "status": status,
                             "kind": "short_page",
                             "detail": "page %s held fewer than %s rows but page "
                                       "%s still has data (next status %s)"
                                       % (page, SPEC.page_size, page + 1, status2),
                             "next_status": status2})
            complete = False
            break
        page += 1
        if page > 200:
            complete = False
            failures.append({"page": page, "status": None, "kind": "page_limit",
                             "detail": "pagination safety limit reached"})
            break
    return records, attempts, {"complete": complete, "pages": pages,
                               "failures": failures}


def fetch_all(http: Http):
    """Collect records from the local source (compatibility wrapper).

    Returns ``(records, attempts)`` exactly as before.  Use
    :func:`fetch_collect` when the completeness of the collection matters.
    """
    records, attempts, _info = fetch_collect(http)
    return records, attempts


# --------------------------------------------------------------------------- #
# Record identity helpers.
# --------------------------------------------------------------------------- #
def canonical(d):
    return json.dumps(d, sort_keys=True, ensure_ascii=False)


def content_hash(d):
    return hashlib.sha256(canonical(d).encode()).hexdigest()[:24]


def dedup(records):
    seen, out, dup = set(), [], 0
    for r in records:
        h = content_hash(r)
        if h in seen:
            dup += 1
            continue
        seen.add(h)
        out.append(r)
    return out, dup


def normalize(r):
    d = {k: (v.strip() if isinstance(v, str) else v) for k, v in r.items()}
    if isinstance(d.get("status"), str):
        d["status"] = d["status"].lower()
    return d


def _apply_kernel(records):
    k = importlib.import_module(".kernel", __package__)
    for r in records:
        if hasattr(k, "process"):
            r.update(k.process(r))
    if hasattr(k, "reduce"):
        records = k.reduce(records)
    return records


# --------------------------------------------------------------------------- #
# Collection pipeline.
# --------------------------------------------------------------------------- #
def kernel_module():
    return importlib.import_module(".kernel", __package__)


def run(mode="full", con=None, rules=None, run_id=None):
    own = con is None
    con = con or Store().connect()
    store = Store(con)
    sleeper = Sleeper()
    http = Http(Transport(), sleeper)
    k = kernel_module()
    rules = k.make_rules(rules)
    if run_id is None:
        run_id = "run-%06d" % store.next_counter("run_seq")
    run_seq_base = int(store.meta_get("observed_seq", "0") or 0)
    try:
        records, attempts, info = fetch_collect(http)
        norm = [normalize(dict(r)) for r in records]
        norm, dup_in_batch = dedup(norm)
        norm = _apply_kernel(norm)

        wm = store.watermark()
        candidates = norm
        if mode == "incremental" and wm:
            candidates = [r for r in norm if str(r.get("ts", "")) > wm]

        ingested = sum(1 for r in candidates if store.upsert(r))

        # The event journal records *every* observed report, not just rows the
        # incremental watermark admitted.  State resolution therefore converges
        # between full and incremental collection, while record-table ingestion
        # keeps its original watermark semantics.
        events = status.event_batch(norm, run_id, start_seq=run_seq_base)
        store.meta_set("observed_seq", run_seq_base + len(events))
        summary = status.ingest_events(store, events, rules=rules, run_id=run_id)
        failures = status.ingest_failures(
            store, info["failures"], rules=rules, run_id=run_id)
        decision_counts = store.decision_counts(run_id=run_id)

        total = store.count()
        watermark_candidates = [str(r.get("ts", "")) for r in norm] + [wm or ""]
        new_wm = max(watermark_candidates) if watermark_candidates else ""
        # Never move the freshness checkpoint backwards.
        if new_wm and (wm is None or str(new_wm) >= str(wm)):
            store.set_watermark(new_wm)

        current = store.all_current()
        return {"engine": "crawl", "mode": mode, "run_id": run_id,
                "fetched": len(records), "duplicates": dup_in_batch,
                "ingested": ingested, "count": total,
                "http_attempts": attempts, "slept": round(sleeper.slept, 4),
                "watermark": store.watermark(),
                "collection": {"complete": info["complete"],
                               "pages": info["pages"]},
                "failures": failures,
                "events": {"new": summary["new_events"],
                           "repeat": summary["repeat_events"]},
                "status": decision_counts,
                "domains": {d["id"]: {kk: d[kk] for kk in
                                       ("state", "state_ts", "reason",
                                        "terminal", "confirmed")}
                            for d in current},
                }
    finally:
        if own:
            con.close()


# --------------------------------------------------------------------------- #
# Session pipeline.
# --------------------------------------------------------------------------- #
class Rpa:
    def __init__(self):
        self.t = Transport()
        self.sleeper = Sleeper()
        self.log = []
        self.captcha_solved = False

    def login(self):
        status, _h, _b = self.t.request("POST", "/login",
                                        json_body={"u": "operator", "p": "local"})
        if status != 200:
            raise RuntimeError("login failed status=%s" % status)
        self.log.append("login")
        return True

    def _csrf(self):
        return {"x-csrf": self.t.cookies.get("csrf", "")}

    def _query_page(self, page):
        status, _h, body = self.t.request("GET", "/query", params={"page": page},
                                          headers=self._csrf())
        if status != 200:
            raise RuntimeError("query failed status=%s" % status)
        return json.loads(body)

    def collect(self):
        out, page = [], 1
        while True:
            self.sleeper.sleep(0.1)
            data = self._query_page(page)
            out.extend(data.get("items", []))
            if data.get("captcha_required") and not self.captcha_solved:
                ok = self._solve_captcha()
                if not ok:
                    raise RuntimeError("captcha solve rejected")
                self.captcha_solved = True
                page += 1
                continue
            if not data.get("has_more"):
                break
            page += 1
            if page > 200:
                break
        self.log.append("collect:%d" % len(out))
        return out

    def _solve_captcha(self):
        status, _h, body = self.t.request("POST", "/captcha",
                                          json_body={"code": self.t.cookies.get("challenge", "")},
                                          headers=self._csrf())
        ok = status == 200 and json.loads(body).get("ok") is True
        self.log.append("captcha:%s" % ("solved" if ok else "failed"))
        return ok

    def submit(self, record):
        token = content_hash(record)
        status, _h, body = self.t.request("POST", "/submit",
                                          json_body={"token": token, "record": record},
                                          headers=self._csrf())
        data = json.loads(body)
        self.log.append("submit:%s" % ("created" if data.get("created") else "dedup"))
        return bool(data.get("created"))


def run_rpa(con=None, rules=None, run_id=None):
    own = con is None
    con = con or Store().connect()
    store = Store(con)
    k = kernel_module()
    rules = k.make_rules(rules)
    if run_id is None:
        run_id = "run-%06d" % store.next_counter("run_seq")
    seq_base = int(store.meta_get("observed_seq", "0") or 0)
    r = Rpa()
    try:
        logged = r.login()
        items = r.collect()
        norm = [normalize(dict(it)) for it in items]
        norm, dup = dedup(norm)
        norm = _apply_kernel(norm)
        ingested = sum(1 for x in norm if store.upsert(x))
        total = store.count()
        created = dedup_submit = 0
        for x in norm[:2]:
            if r.submit(x):
                created += 1
            else:
                dedup_submit += 1
        for x in norm[:1]:
            if not r.submit(x):
                dedup_submit += 1
        events = status.event_batch(norm, run_id, start_seq=seq_base,
                                    source="rpa")
        store.meta_set("observed_seq", seq_base + len(events))
        summary = status.ingest_events(store, events, rules=rules, run_id=run_id)
        wm = max([str(x.get("ts", "")) for x in norm] + [store.watermark() or ""])
        if wm:
            store.set_watermark(wm)
        current = store.all_current()
        return {"engine": "rpa", "logged_in": logged, "captcha_solved": r.captcha_solved,
                "collected": len(items), "fetched": len(items), "duplicates": dup,
                "ingested": ingested, "count": total, "run_id": run_id,
                "submit_created": created, "submit_dedup": dedup_submit,
                "events": {"new": summary["new_events"],
                           "repeat": summary["repeat_events"]},
                "status": store.decision_counts(run_id=run_id),
                "domains": {d["id"]: {kk: d[kk] for kk in
                                       ("state", "state_ts", "reason",
                                        "terminal", "confirmed")}
                            for d in current},
                "log": r.log}
    finally:
        if own:
            con.close()
