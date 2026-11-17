# whoisdrift

A small offline data-processing service with a local HTTP fixture and SQLite storage. It is self-contained and does not require network access.

## Usage

```text
make run       # process the bundled source
make run_inc   # process using the incremental command-line mode
make test      # run the test suite
make mock      # start the local fixture service
```

The package exposes a Python entry point under `src/whoisdrift`. The fixture service supports the source formats used by the package, and the command-line entry point writes results to the local SQLite database. Re-running commands is safe for an existing database.
