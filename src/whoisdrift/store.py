
"""Real sqlite3 store: content-hash primary key, INSERT-OR-IGNORE dedup, watermark."""
import json, sqlite3
from .config import DB_PATH

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
        self.con.commit()

    @staticmethod
    def hash(record):
        blob = json.dumps(record, sort_keys=True, ensure_ascii=False)
        import hashlib
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
        self.con.commit()
