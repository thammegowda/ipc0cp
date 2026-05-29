# Makefile for ipc0cp
#
# Common targets for building, testing, and cleaning the C++ and Python
# components of ipc0cp. Run `make help` to see all available targets.

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BUILD_DIR       ?= build
BUILD_TYPE      ?= Release
JOBS            ?= $(shell nproc 2>/dev/null || echo 4)
PYTHON          ?= python3
PIP             ?= $(PYTHON) -m pip
CMAKE           ?= cmake
CTEST           ?= ctest

# Extra flags forwarded to cmake configure / pytest.
CMAKE_FLAGS     ?=
PYTEST_FLAGS    ?=

.DEFAULT_GOAL := help

# ---------------------------------------------------------------------------
# Help
# ---------------------------------------------------------------------------
.PHONY: help
help: ## Show this help message
	@echo "ipc0cp - available make targets:"
	@echo
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| sort \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------------------
# C++ build & test
# ---------------------------------------------------------------------------
.PHONY: configure
configure: ## Configure the CMake build directory
	$(CMAKE) -S . -B $(BUILD_DIR) -DCMAKE_BUILD_TYPE=$(BUILD_TYPE) -DIPC0CP_TESTS=ON $(CMAKE_FLAGS)

.PHONY: build
build: configure ## Build the C++ library and tests
	$(CMAKE) --build $(BUILD_DIR) -j $(JOBS)

.PHONY: test-cpp
test-cpp: build ## Build and run the C++ test suite (ctest)
	$(CTEST) --test-dir $(BUILD_DIR) --output-on-failure

# ---------------------------------------------------------------------------
# Python install & test
# ---------------------------------------------------------------------------
.PHONY: install
install: ## Install the Python package (editable)
	$(PIP) install -e .

.PHONY: install-dev
install-dev: ## Install the Python package with dev extras (pytest, etc.)
	$(PIP) install -e ".[dev]"

.PHONY: test-py
test-py: install-dev ## Run the Python test suite (pytest)
	$(PYTHON) -m pytest $(PYTEST_FLAGS)

# ---------------------------------------------------------------------------
# Combined
# ---------------------------------------------------------------------------
.PHONY: test
test: test-cpp test-py ## Run both C++ and Python test suites

.PHONY: all
all: build install-dev ## Build C++ and install Python (dev)

# ---------------------------------------------------------------------------
# Benchmarks
# ---------------------------------------------------------------------------
.PHONY: bench
bench: ## Run the benchmark harness (override ARGS for options)
	$(PYTHON) benchmarks/run_benchmark.py $(ARGS)

# ---------------------------------------------------------------------------
# Cleaning
# ---------------------------------------------------------------------------
.PHONY: clean
clean: ## Remove the CMake build directory
	rm -rf $(BUILD_DIR)

.PHONY: clean-py
clean-py: ## Remove Python build artifacts and caches
	rm -rf python/*.egg-info python/**/__pycache__ .pytest_cache build dist
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name '*.pyc' -delete

.PHONY: distclean
distclean: clean clean-py ## Remove all build and Python artifacts
