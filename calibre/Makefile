.PHONY: up down build api replay test lint clean topics

# ---------------------------------------------------------------------------
# Docker
# ---------------------------------------------------------------------------
up:
	docker compose up -d --build

down:
	docker compose down -v

build:
	docker compose build

# ---------------------------------------------------------------------------
# Local development (outside Docker)
# ---------------------------------------------------------------------------
api:
	uvicorn api.main:app --reload --host 0.0.0.0 --port 8000

replay:
	python -m replay.producer --speed 10

topics:
	python -m streaming.kafka_topics

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
db-init:
	python -m storage.database --init

# ---------------------------------------------------------------------------
# Testing
# ---------------------------------------------------------------------------
test:
	pytest tests/ -v

lint:
	ruff check .

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------
clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name '*.pyc' -delete 2>/dev/null || true
