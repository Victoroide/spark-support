.PHONY: help install test format lint type-check quality clean run

help:
	@echo "Available commands:"
	@echo "  make install       - Install dependencies"
	@echo "  make test          - Run tests with coverage"
	@echo "  make format        - Format code with black and isort"
	@echo "  make lint          - Lint code with pylint"
	@echo "  make type-check    - Type check with mypy"
	@echo "  make quality       - Run all quality checks"
	@echo "  make clean         - Remove generated files"
	@echo "  make run           - Run ETL pipeline"

install:
	pip install -r requirements.txt

test:
	pytest --cov=src --cov-report=term-missing --cov-report=html

format:
	black src/ tests/
	isort src/ tests/

lint:
	pylint src/

type-check:
	mypy src/

quality: format lint type-check test
	@echo "All quality checks passed!"

clean:
	rm -rf output/
	rm -rf .pytest_cache/
	rm -rf htmlcov/
	rm -rf .coverage
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

run:
	python -m src.main
