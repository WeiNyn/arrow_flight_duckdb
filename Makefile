.PHONY: lint fix test build clean

# Python source files
PY_FILES := $(shell find clients/ tests/clients/ -name "**.py")

# Required tools
VENV_DIR := .venv
PYTHON := python3
PIP := pip

# Install dev dependencies
$(VENV_DIR):
	$(PYTHON) -m venv $(VENV_DIR)
	$(VENV_DIR)/bin/$(PIP) install -U pip
	$(VENV_DIR)/bin/$(PIP) install isort ruff pylint pytest build mypy

# Lint checks
lint: $(VENV_DIR)
	$(VENV_DIR)/bin/isort --check-only $(PY_FILES) --color
	$(VENV_DIR)/bin/ruff check $(PY_FILES)
	$(VENV_DIR)/bin/pylint $(PY_FILES) --output-format=colorized
	$(VENV_DIR)/bin/mypy $(PY_FILES) --explicit-package-bases

# Fix and format code
fix: $(VENV_DIR)
	$(VENV_DIR)/bin/isort $(PY_FILES) --color
	$(VENV_DIR)/bin/ruff check --fix $(PY_FILES) 

# Run tests
test: $(VENV_DIR)
	$(VENV_DIR)/bin/pytest

# Build project
build: $(VENV_DIR)
	$(VENV_DIR)/bin/$(PYTHON) -m build

# Clean built distributions and cache
clean:
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf __pycache__/
	rm -rf .pytest_cache/
	rm -rf .ruff_cache/