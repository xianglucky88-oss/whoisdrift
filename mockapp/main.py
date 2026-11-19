
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
import hashlib
import json
from fastapi import FastAPI, Request, Response
from whoisdrift.config import SPEC
from whoisdrift.mockdata import build_rows, extra_row

app = FastAPI(title="Local source")
_ALL = build_rows()
_extra = []
_verified_sessions = set()
_accepted_tokens = set()
# Test controls (empty by default; the offline fixture behaves as before).
_faults = {}        # page -> HTTP status to return instead of the page
_offline = False    # raise, emulating an unreachable source
_omit_pages = set() # pages to return as empty (a "missing page" without an error)


def _data():
    return _ALL + _extra

def _page_items(page):
    lo, hi = (page - 1) * SPEC.page_size, page * SPEC.page_size
    return _data()[lo:hi]

def _reset_source():
    global _extra, _offline
    _ALL[:] = build_rows()
    _extra = []
    _faults.clear()
    _offline = False
    _omit_pages.clear()
    _verified_sessions.clear()
    _accepted_tokens.clear()

@app.get("/items")
def items(page: int = 1):
    if _offline:
        raise RuntimeError("source is offline (simulated)")
    if page in _faults:
        return Response(status_code=int(_faults[page]))
    data = [] if page in _omit_pages else _page_items(page)
    if SPEC.fmt == "json":
        return {"items": data, "page": page}
    if SPEC.fmt == "text":
        lines = ["\t".join(str(it.get(c, "")) for c in SPEC.columns) for it in data]
        return Response(content="\n".join(lines), media_type="text/plain")
    th = "".join("<th>%s</th>" % c for c in SPEC.columns)
    trs = "".join("<tr>" + "".join("<td>%s</td>" % it.get(c, "") for c in SPEC.columns)
                  + "</tr>" for it in data)
    return Response(content="<html><table><tr>%s</tr>%s</table></html>" % (th, trs),
                    media_type="text/html")

@app.get("/refresh")
def refresh():
    _extra.append(extra_row(len(_extra) + 1))
    return {"total": len(_data())}

@app.get("/grow")
def grow():
    """Add a newer source row (alias used by incremental scenarios)."""
    _extra.append(extra_row(len(_extra) + 1))
    return {"total": len(_data())}

@app.post("/reports")
async def add_report(request: Request):
    """Inject a raw status report (late, duplicate or conflicting by design).

    Missing columns default from the first base row; ``status``/``ts``/``id``
    are taken verbatim so out-of-order and conflicting reports can be built.
    """
    body = await request.json()
    row = dict(_ALL[0])
    for c in SPEC.columns:
        if c in body:
            row[c] = body[c]
    _extra.append(row)
    return {"total": len(_data()), "record": row}

@app.post("/control")
async def control(request: Request):
    """Test-only fault injection: ``faults``, ``offline``, ``omit_pages``."""
    global _offline
    body = await request.json()
    if body.get("reset"):
        _reset_source()
    if "offline" in body:
        _offline = bool(body["offline"])
    if "faults" in body:
        _faults.clear()
        _faults.update({int(k): int(v) for k, v in (body["faults"] or {}).items()})
    if "omit_pages" in body:
        _omit_pages.clear()
        _omit_pages.update(int(p) for p in (body["omit_pages"] or []))
    return {"faults": {str(k): v for k, v in _faults.items()},
            "offline": _offline, "omit_pages": sorted(_omit_pages),
            "rows": len(_data())}

@app.get("/probe")
def probe():
    app.__dict__["_flaky_n"] = app.__dict__.get("_flaky_n", 0) + 1
    if app.__dict__["_flaky_n"] <= 2:
        return Response(status_code=429)
    return {"ok": True, "attempts": app.__dict__["_flaky_n"]}

@app.get("/flaky")
def flaky():
    """Alias of /probe, kept for callers that used the recovered upstream name."""
    return probe()

@app.get("/health")
def health():
    return {"ok": True, "rows": len(_data())}

@app.post("/login")
def login(response: Response):
    sid = "SID-local"
    csrf = "CSRF-local"
    challenge = hashlib.sha256((sid + csrf).encode()).hexdigest()[:4]
    response.set_cookie("sid", sid)
    response.set_cookie("csrf", csrf)
    response.set_cookie("challenge", challenge)
    return {"role": "operator"}

@app.get("/query")
def query(request: Request, page: int = 1):
    if not request.cookies.get("sid"):
        return Response(status_code=401)
    if request.headers.get("x-csrf") != request.cookies.get("csrf"):
        return Response(status_code=403)
    items_page = _page_items(page)
    nxt = _page_items(page + 1)
    sid = request.cookies.get("sid")
    gate = (SPEC.engine == "rpa" and page == 1 and sid not in _verified_sessions
            and len(_data()) > SPEC.page_size)
    return {"items": items_page, "page": page, "has_more": bool(nxt),
            "captcha_required": bool(gate)}

@app.post("/captcha")
async def captcha(request: Request):
    body = await request.json()
    ok = body.get("code") == request.cookies.get("challenge")
    if ok:
        _verified_sessions.add(request.cookies.get("sid"))
    return {"ok": bool(ok)}

@app.post("/submit")
async def submit(request: Request):
    body = await request.json()
    token = body.get("token")
    created = token not in _accepted_tokens
    if created:
        _accepted_tokens.add(token)
    return {"created": bool(created), "token": token}
