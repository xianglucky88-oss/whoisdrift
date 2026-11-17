
TRANS = {"new": "pending", "draft": "pending", "active": "active",
         "pending": "pending", "sold": "closed", "withdrawn": "closed",
         "deleted": "archived"}
ORDER = {"unknown": 0, "pending": 1, "active": 2, "closed": 3, "archived": 4}
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
