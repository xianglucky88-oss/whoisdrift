
"""Public orchestration entry point."""
from . import framework


def run(mode="full", con=None, rules=None, run_id=None):
    """Run the configured collection engine.

    ``rules`` optionally overrides the status rule tables (see
    :func:`whoisdrift.kernel.make_rules`); the current state is never moved
    backwards by a rule adjustment.
    """
    from .config import SPEC
    if SPEC.engine == "rpa":
        return framework.run_rpa(con=con, rules=rules, run_id=run_id)
    return framework.run(mode=mode, con=con, rules=rules, run_id=run_id)


def reconcile(con=None, rules=None):
    """Re-derive state from the stored journal (read-mostly, monotonic)."""
    from .status import reconcile as _reconcile
    from .store import Store
    own = con is None
    con = con or Store().connect()
    try:
        return _reconcile(Store(con), rules=rules)
    finally:
        if own:
            con.close()


def backfill(con=None, rules=None):
    """Seed the journal from an existing records table (idempotent)."""
    from .status import backfill as _backfill
    from .store import Store
    own = con is None
    con = con or Store().connect()
    try:
        return _backfill(Store(con), rules=rules)
    finally:
        if own:
            con.close()


def state_at(domain_id, as_of, basis="event", con=None, rules=None):
    """Read-only historical query; does not change the current state."""
    from .status import state_at as _state_at
    from .store import Store
    own = con is None
    con = con or Store().connect()
    try:
        return _state_at(Store(con), domain_id, as_of, basis=basis, rules=rules)
    finally:
        if own:
            con.close()


def explain(domain_id, con=None):
    """User-facing explanation of a domain's current state and event history."""
    from .status import explain as _explain
    from .store import Store
    own = con is None
    con = con or Store().connect()
    try:
        return _explain(Store(con), domain_id)
    finally:
        if own:
            con.close()
