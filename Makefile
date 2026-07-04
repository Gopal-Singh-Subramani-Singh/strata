.PHONY: install infra-up infra-down start test demo clean

install:
	pip install -r requirements.txt

infra-up:
	docker compose up redis minio prometheus grafana -d

infra-down:
	docker compose down

start:
	uvicorn strata_core.main:app --port 8003 --reload

start-prod:
	uvicorn strata_core.main:app --port 8003 --workers 4

test:
	pytest tests/ -v

test-cov:
	pytest tests/ -v --cov=strata_core --cov-report=term-missing

demo:
	python demo/fraud_feature_store.py

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null; true
	find . -name "*.pyc" -delete 2>/dev/null; true
	rm -f strata_registry.db *.duckdb
