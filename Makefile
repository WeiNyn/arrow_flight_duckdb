.PHONY: lint fix test build clean benchmark

# Python source files
PY_FILES := $(shell find arrow_flight_duckdb/ utils/ tests/ -name "**.py")

# Required tools
VENV_DIR := .venv
PYTHON := python3
PIP := pip

# Benchmark variables
EXECUTORS := duckdb pure_pa
FUNCTIONS := select_start select_start_limit groupby_count groupby_sum groupby_avg groupby_min_max groupby_complex
BENCHMARK_DIR := benchmark_results

# Install dev dependencies
$(VENV_DIR):
	$(PYTHON) -m venv $(VENV_DIR)
	$(VENV_DIR)/bin/$(PIP) install -U pip
	$(VENV_DIR)/bin/$(PIP) install isort ruff pylint pytest build mypy memory_profiler

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

# Create benchmark directory
$(BENCHMARK_DIR):
	mkdir -p $(BENCHMARK_DIR)

# Run benchmarks
benchmark: $(VENV_DIR) $(BENCHMARK_DIR)
	@for executor in $(EXECUTORS); do \
		for func in $(FUNCTIONS); do \
			echo "Benchmarking $$executor - $$func"; \
			$(VENV_DIR)/bin/mprof run -o $(BENCHMARK_DIR)/$${executor}_$${func} \
				$(VENV_DIR)/bin/python utils/benchmark.py $$executor $$func; \
		done; \
	done

# Clean built distributions and cache
clean:
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf __pycache__/
	rm -rf .pytest_cache/
	rm -rf .ruff_cache/
	rm -rf $(BENCHMARK_DIR)/