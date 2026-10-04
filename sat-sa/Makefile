# SAT-SA — Development & Execution Makefile

.PHONY: install generate run verify test ui benchmark clean

install:
	pip install -r requirements.txt

generate:
	python -m satsa generate --output-dir ./data/sample --num-cses 40 --seed 42

run:
	python -m satsa run --input-dir ./data/sample/CSE-101 --entity-id CSE-101

verify:
	python -m satsa verify --ledger-path ./trust_ledger.db

test:
	pytest tests -v

ui:
	streamlit run ui/app.py

benchmark:
	python validation/harness.py

clean:
	rm -rf ./data/validation_run ./trust_ledger.db .pytest_cache
