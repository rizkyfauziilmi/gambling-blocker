help:
	@echo "Available commands:"
	@echo "  make help - Show this help message"
	@echo "  make jupyter - Open Jupyter notebook"
	@echo "  make generate-dataset-domain - Generate dataset for domain"
	@echo "  make scrape - Run the web scraper"


jupyter:
	uv run --with jupyter jupyter lab

generate-dataset-domain:
	uv run --module datasets.generate_dataset_domain

scrape:
	uv run --module scraper.main