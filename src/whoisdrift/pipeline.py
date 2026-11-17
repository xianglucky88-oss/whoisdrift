
"""Public orchestration entry point."""
from . import framework

def run(mode="full", con=None):
    from .config import SPEC
    if SPEC.engine == "rpa":
        return framework.run_rpa(con=con)
    return framework.run(mode=mode, con=con)
