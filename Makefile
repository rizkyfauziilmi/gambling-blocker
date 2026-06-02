.PHONY: format format-check lint

format:
	cd backend && uv run ruff format && uv run ruff check --fix
	cd client/dashboard && pnpm run format
	cd client/gambling-extension && pnpm format

format-check:
	cd backend && uv run ruff format --check && uv run ruff check
	cd client/dashboard && pnpm run format:check
	cd client/gambling-extension && pnpm format:check
