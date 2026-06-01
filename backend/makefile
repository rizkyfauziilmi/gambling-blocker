.PHONY: help jupyter crawl

.DEFAULT_GOAL := help

jupyter: ## Run Jupyter Lab
	uv run --with jupyter jupyter lab

screenshot: ## Run screenshot crawler
	uv run python -m crawler.screenshot_crawler

redis-up: ## Start Redis via Docker Compose
	docker compose up -d redis

redis-down: ## Stop Redis
	docker compose down

redis-logs: ## View Redis logs
	docker compose logs -f redis

redis-reset: ## Wipe Redis data and stop
	docker compose down -v

dev-server: ## Run backend dev server
	fastapi dev

server: ## Run backend prod server
	fastapi run

help: ## Show help
	@echo "Usage:"
	@echo "  make <target>"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | \
	awk 'BEGIN {FS = ":.*?## "} {printf "  %-10s %s\n", $$1, $$2}'
