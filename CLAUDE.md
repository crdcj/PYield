# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

PYield is a Python library for Brazilian fixed income analysis. It fetches and processes data from ANBIMA, BCB (Central Bank), IBGE, and B3 (Brazilian stock exchange). All public functions return Polars DataFrames/Series (migration from Pandas completed in v0.40.0).

## Build & Development Commands

```bash
# Install dependencies (uses uv package manager)
uv sync

# Run all tests including doctests
pytest pyield --doctest-modules

# Test a single module
pytest pyield/bday.py --doctest-modules

# Run a single test file
pytest tests/test_bday.py

# Run a specific test
pytest tests/test_bday.py::test_count_with_strings1

# Linting
ruff check

# Type checking
pyright

# Build documentation (MkDocs)
mkdocs serve
```

## Architecture

### Module Structure

The library is organized into domain-specific namespaces, all exposed through `pyield/__init__.py`:

- **`bday`** - Business day calendar (Brazilian holidays built-in). Core functions: `count`, `offset`, `generate`, `is_business_day`
- **`anbima`** - ANBIMA data endpoints (treasury bond pricing, yield curves). Functions: `tpf_data`, `last_ettj`, `last_ima`
- **`bc`** - BCB indicators (SELIC, PTAX, repo rates, VNA). Functions: `selic_over`, `ptax`, `repos`, `vna_lft`
- **`b3`** - B3 market data (DI futures, price reports). Key function: `futures()`
- **`tn`** - Treasury bond modules: `ltn`, `ntnb`, `ntnf`, `ntnc`, `lft`, `pre` (each has `data()`, `quotation()`, pricing functions)
- **`ipca`** - Inflation data (historical and projected)

### Key Cross-Cutting Components

- **`types.py`** - Type aliases `DateLike` and `ArrayLike`; `any_is_empty()` for null/empty detection
- **`converters.py`** - `convert_dates()` normalizes various date inputs to `datetime.date` or `pl.Series[Date]`
- **`interpolator.py`** - `Interpolator` class for rate interpolation (linear or flat_forward method, 252 bday/year convention)
- **`data_cache.py`** - GitHub-hosted parquet data cache with daily TTL using `lru_cache`
- **`retry.py`** - Tenacity-based retry decorator for network requests (retries on 429, 5xx, timeouts)
- **`clock.py`** - `today()` and `now()` return Brazil timezone (America/Sao_Paulo) dates/times

### Date Handling Conventions

- Accepted string formats: `DD-MM-YYYY`, `DD/MM/YYYY`, `YYYY-MM-DD`
- Scalar dates normalize to `datetime.date`; collections become `pl.Series` with dtype `Date`
- First non-null string in a collection determines format for entire collection
- Nullable inputs (`None`, `NaN`, empty collections) short-circuit: scalar functions return `None` or `nan`, vectorized functions return empty DataFrame/Series

### Data Flow Pattern

Most data-fetching functions follow this pattern:
1. Accept `DateLike` reference date parameter
2. Convert dates using `convert_dates()`
3. Fetch from external API (with retry logic) or cached parquet
4. Return Polars DataFrame with standardized column names

## Docstring Conventions

- All docstrings must be written in **Portuguese** (both public and internal functions).
- Public functions use Google-style sections: `Args:`, `Returns:`, `Output Columns:`, `Notes:`, `Examples:`.
- `Output Columns:` lists every column with tipo Polars e descrição (ex: `* SettlementDate (Date): data de liquidação.`).
- Doctests (section `Examples:`) use real data and are validated by `pytest --doctest-modules`.

## Testing

Tests are in `tests/` and doctests are embedded in docstrings. Run `pytest pyield --doctest-modules` to execute both.

### Test Pattern for Data-Fetching Modules

Modules that fetch external data (e.g., `bc/repo.py`, `bc/trades_monthly.py`) use local reference data to test without network access:

1. **Reference data** — A pair of files in `tests/<module>/data/`: the raw input (CSV or ZIP) and the expected output (Parquet).
2. **Pipeline test** — Processes the local raw input through the internal processing functions and asserts `result.equals(expected_parquet)`.
3. **Public function test** — Patches the network fetch function to return local raw data, calls the public function, and asserts equality with the reference Parquet.
