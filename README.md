# whoisdrift

A small offline data-processing service with a local HTTP fixture and SQLite
storage. It is self-contained and does not require network access.

## Usage

```text
make run       # process the bundled source
make run_inc   # process using the incremental command-line mode
make test      # run the test suite
make mock      # start the local fixture service
```

The package exposes a Python entry point under `src/whoisdrift`. The fixture
service supports the source formats used by the package, and the command-line
entry point writes results to the local SQLite database. Re-running commands is
safe for an existing database.

## Status semantics

Domain status reports are resolved by an event-time state machine
(`src/whoisdrift/kernel.py`) and every decision is journaled
(`src/whoisdrift/status.py`, `src/whoisdrift/store.py`):

| Situation | Outcome | Reason |
| --- | --- | --- |
| Newer report changes the state | applied | `status_changed` |
| First confirmed report | applied | `initial_state` |
| Newer report repeats the state | applied (freshness advances) | `confirmed_same_state` |
| Report older than the confirmed state | rejected | `stale_event` |
| Same domain/time/status seen again | rejected | `duplicate_report` / `duplicate_event` |
| Report after a terminal (`archived`) state | rejected | `terminal_boundary` |
| Unknown status value | unconfirmed | `unknown_status` |
| Report without a usable timestamp | unconfirmed | `missing_timestamp` |
| RDAP request failed / blocked / offline | unconfirmed | `source_unavailable`, `access_blocked`, … |
| A page of the result set was missing | unconfirmed | `missing_page` |

Key guarantees:

* a late-delivered old report can never drag the confirmed state back in time
  (ordering is by observation batch, then event time within a snapshot);
* a failed request, a block, an offline source or a missing page is **never**
  treated as a domain deletion – it is recorded as an unconfirmed observation;
* a terminal state is a boundary: later contradictory reports are recorded but
  cannot resurrect the domain;
* historical (`as of`) queries are read-only and cannot change current state;
* restoring old data or adjusting the rule tables never moves the current state
  backwards (regressions are logged as `stale_recompute`).

Every record in `status_decisions` carries a machine-readable `reason` and a
human-readable `detail`, so effective changes, unconfirmed observations,
rejected events and termination boundaries can all be explained.

## Inspecting state

```text
python -m whoisdrift.cli --explain WHO-0002         # full explanation of a domain
python -m whoisdrift.cli --state-at 2030-01-04T00:00:00Z --domain WHO-0002
python -m whoisdrift.cli --reconcile                # re-derive state from the journal
```

Python API (`whoisdrift.pipeline`): `run`, `reconcile`, `backfill`, `state_at`,
`explain`.

Backfill seeds the journal from an existing (legacy) `records` table, so older
databases become explainable without changing their content.

## Verification

`tests/` covers the interleaved cases explicitly: out-of-order and duplicate
reports, late delivery across collection runs, page failures (offline/blocked/
missing/partial) and recovery, full-vs-incremental convergence, old-data
restoration, rule adjustment, historical queries, legacy database migration and
unchanged record content.
