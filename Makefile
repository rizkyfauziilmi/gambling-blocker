.DEFAULT_GOAL := help

.PHONY: format format-check lint dev dev-backend dev-dashboard dev-extension help

format:
	cd backend && uv run ruff format && uv run ruff check --fix
	cd client/dashboard && pnpm run format
	cd client/gambling-extension && pnpm format

format-check:
	cd backend && uv run ruff format --check && uv run ruff check
	cd client/dashboard && pnpm run format:check
	cd client/gambling-extension && pnpm format:check

dev-backend: ## Start backend dev server
	cd backend && $(MAKE) storage-up && $(MAKE) dev-server

dev-dashboard: ## Start dashboard dev server
	cd client/dashboard && pnpm run dev

dev-extension: ## Start extension dev server
	cd client/gambling-extension && pnpm run dev

dev: ## Start all development servers
	@echo "Starting all services (Redis, Backend, Dashboard, Extension)..."
	@trap 'echo "\nStopping all services..."; kill 0 2>/dev/null; exit' SIGINT SIGTERM EXIT; \
		( $(MAKE) dev-backend ) 2>&1 | sed 's/^/[backend] /' & \
		( $(MAKE) dev-dashboard ) 2>&1 | sed 's/^/[dashboard] /' & \
		( $(MAKE) dev-extension ) 2>&1 | sed 's/^/[extension] /' & \
		wait

help: ## Show help
	@echo "Usage:"
	@echo "  make <target>"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | \
	awk 'BEGIN {FS = ":.*?## "} {printf "  %-15s %s\n", $$1, $$2}'
