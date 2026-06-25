BENCH_API_URL ?= http://127.0.0.1:8000
BENCH_API_AUTH ?= admin:admin123

.PHONY: help jupyter test redis-up redis-down redis-logs redis-reset dev-server server
.PHONY: minio-up minio-down minio-logs minio-reset storage-up storage-down
.PHONY: benchmark benchmark-multipage reset

.DEFAULT_GOAL := help

jupyter: ## Run Jupyter Lab
	uv run --with jupyter jupyter lab

test: ## Run tests
	uv run pytest $(ARGS)

redis-up: ## Start Redis via Docker Compose
	docker compose up -d redis

redis-down: ## Stop Redis
	docker compose down

redis-logs: ## View Redis logs
	docker compose logs -f redis

redis-reset: ## Wipe Redis data and stop
	docker compose down -v

dev-server: ## Run backend dev server
	uv run fastapi dev

server: ## Run backend prod server
	uv run fastapi run

minio-up: ## Start MinIO via Docker Compose
	docker compose up -d minio

minio-down: ## Stop MinIO
	docker compose down

minio-logs: ## View MinIO logs
	docker compose logs -f minio

minio-reset: ## Wipe MinIO data and stop
	docker compose down -v

storage-up: redis-up minio-up ## Start all storage services

storage-down: redis-down minio-down ## Stop all storage services

benchmark: ## Fase A: optimal (teks saja + screenshot ok + screenshot gagal)
	mkdir -p scripts/output
	BENCH_API_URL=$(BENCH_API_URL) BENCH_API_AUTH=$(BENCH_API_AUTH) \
		uv run python3 scripts/benchmark.py

benchmark-multipage: ## Fase B: multipage inference (bypass_text=false)
	mkdir -p scripts/output
	BENCH_API_URL=$(BENCH_API_URL) BENCH_API_AUTH=$(BENCH_API_AUTH) \
		uv run python3 scripts/benchmark.py --multipage

reset: ## Reset semua data (Redis, MinIO, SQLite)
	docker compose down -v
	rm -f app.db
	docker compose up -d

help: ## Show help
	@echo "Usage:"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | \
	awk 'BEGIN {FS = ":.*?## "} {printf "  %-12s %s\n", $$1, $$2}'
