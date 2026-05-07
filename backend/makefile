.PHONY: help jupyter crawl

.DEFAULT_GOAL := help

jupyter: ## Run Jupyter Lab
	uv run --with jupyter jupyter lab

crawl: ## Run crawler to generate dataset
	uv run python -m crawl

help: ## Show help
	@echo "Usage:"
	@echo "  make <target>"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | \
	awk 'BEGIN {FS = ":.*?## "} {printf "  %-10s %s\n", $$1, $$2}'