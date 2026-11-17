
from types import SimpleNamespace

ROOT = __import__("pathlib").Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "whoisdrift.db"

SPEC = SimpleNamespace(
    slug="whoisdrift",
    fmt="text",
    engine="crawl",
    endpoint="items",
    columns=["id", "name", "value", "status", "ts", "registrar"],
    page_size=4,
    int_fields=["value"],
    ts_field="ts",
)
