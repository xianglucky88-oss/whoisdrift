PY=python3
run:
	PYTHONPATH=src:. $(PY) -m whoisdrift.cli
run_inc:
	PYTHONPATH=src:. $(PY) -m whoisdrift.cli --incremental
test:
	PYTHONPATH=src:. $(PY) -m pytest -q
mock:
	PYTHONPATH=src:. $(PY) -m uvicorn mockapp.main:app --port 8000
clean:
	rm -rf .pytest_cache **/__pycache__ *.db
