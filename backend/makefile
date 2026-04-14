help:
	@echo "Available commands:"
	@echo "  make jupyter - Open Jupyter notebook"
	@echo "  make generate-links - Generate links to scrape"
	@echo "  make scrape   - Run the web scraper"

jupyter:
	uv run --with jupyter jupyter lab

generate-dataset:
	uv run --module datasets.generate_dataset

scrape:
	uv run --module scraper.main
