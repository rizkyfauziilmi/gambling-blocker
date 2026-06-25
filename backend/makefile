BENCH_API_URL ?= http://127.0.0.1:8000
BENCH_CSV ?= model/dataset/dataset_crawl_070526.csv
BENCH_OUT ?= scripts/output

.PHONY: help jupyter reset test bench bench-text bench-screenshot bench-full

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

bench: ## Run benchmark with ARGS (e.g. make bench ARGS="--bypass-text --min-per-category 100")
	mkdir -p $(BENCH_OUT)
	BENCH_API_URL=$(BENCH_API_URL) uv run python3 scripts/benchmark_response.py \
		--urls $(BENCH_CSV) \
		--output $(BENCH_OUT)/$$(date +%Y%m%d_%H%M%S).csv \
		--reset $(ARGS)

bench-text: ## Benchmark: text-only (bypass_text=true, multipage=false)
	mkdir -p $(BENCH_OUT)
	BENCH_API_URL=$(BENCH_API_URL) uv run python3 scripts/benchmark_response.py \
		--urls $(BENCH_CSV) \
		--bypass-text --no-multipage \
		--min-per-category 500 \
		--output $(BENCH_OUT)/text_$$(date +%Y%m%d_%H%M%S).csv \
		--reset

bench-screenshot: ## Benchmark: text + screenshot (multipage=false)
	mkdir -p $(BENCH_OUT)
	BENCH_API_URL=$(BENCH_API_URL) uv run python3 scripts/benchmark_response.py \
		--urls $(BENCH_CSV) \
		--no-multipage \
		--min-per-category 500 \
		--output $(BENCH_OUT)/screenshot_$$(date +%Y%m%d_%H%M%S).csv \
		--reset

bench-full: ## Benchmark: full pipeline (multipage=true, bypass_text=false)
	mkdir -p $(BENCH_OUT)
	BENCH_API_URL=$(BENCH_API_URL) uv run python3 scripts/benchmark_response.py \
		--urls $(BENCH_CSV) \
		--min-per-category 500 \
		--output $(BENCH_OUT)/full_$$(date +%Y%m%d_%H%M%S).csv \
		--reset

reset: ## Reset semua data (Redis, MinIO, SQLite)
	docker compose down -v
	rm -f app.db
	docker compose up -d

help: ## Show help
	@echo "Usage:"
	@echo "  make <target>"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | \
	awk 'BEGIN {FS = ":.*?## "} {printf "  %-12s %s\n", $$1, $$2}'
