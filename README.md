[![PyPI version](https://img.shields.io/pypi/v/pyield.svg)](https://pypi.python.org/pypi/pyield)
[![Made with Python](https://img.shields.io/badge/Python->=3.12-blue?logo=python&logoColor=white)](https://python.org "Go to Python homepage")
[![License](https://img.shields.io/badge/License-MIT-blue)](https://github.com/crdcj/PYield/blob/main/LICENSE)

# PYield: Brazilian Fixed Income Toolkit

PYield is a Python library designed for the analysis of Brazilian fixed income instruments. Leveraging the power of Polars and Requests, PYield simplifies the process of obtaining and processing data from key sources such as ANBIMA, BCB, IBGE and B3.

## Examples & Quickstart

A quickstart notebook is available in the examples/ directory and can be opened directly in Google Colab: 

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/crdcj/PYield/blob/main/examples/pyield_quickstart.ipynb)

It demonstrates:

- Package installation.
- Basic use of the business day calendar (bday.count, bday.offset).
- DI futures query (futures).

More examples may be added later (treasury bonds, curve interpolation, inflation). Contributions are welcome.

---
### ✅ Polars migration from version 0.40.0 onwards:

All public functions now return **Polars DataFrames or Series** as the canonical format. This provides stronger typing, faster execution and more reliable date/rate handling. The last version to return Pandas objects by default was **0.39.xx**.

Need Pandas? Convert explicitly:
```python
df_pandas = df.to_pandas(use_pyarrow_extension_array=True)
series_pandas = s.to_pandas(use_pyarrow_extension_array=True)
```
The internal typing relies on PyArrow-backed dtypes for consistency across numeric and date operations.

## Documentation

Visit the [full documentation for PYield](https://crdcj.github.io/PYield/).

## Key Features

- **Data Collection**: Automated fetching of data from ANBIMA and B3.
- **Data Processing**: Efficient processing and normalization of fixed income data.
- **Analysis Tools**: Built-in functions for common analysis tasks in fixed income markets.
- **Easy Integration**: Seamless integration with pandas data analysis workflows.
- **Type Hints**: Full support for static type checking, enhancing development experience and code quality.

## Overview & Navigation

PYield is split into focused namespaces. Each function and class has a rich docstring (the online site is generated from them via MkDocs). The quickest learning path is accessing the documentation and following the function/class examples verbatim.

### Top-Level Namespace (Cheat Sheet)

| Symbol | Kind | Purpose |
|--------|------|---------|
| `bday` | module | Business day calendar (count, offset, generate). |
| `anbima` | module | ANBIMA diffusion & curve endpoints. |
| `bc` | module | BCB indicators (SELIC Over, PTAX, repo rates). |
| `b3` | module | B3 domain (futures submodules). |
| `di1` | module | DI1 futures data & analytics. |
| `futures` | function | Fetch futures (see `help(futures)`). |
| `forward` | function | Single period forward rate from spot curve. |
| `forwards` | function | Vectorized forward rate construction. |
| `today` | function | Brazilian current date (America/Sao_Paulo). |
| `now` | function | Current local time in Brazil (time only). |
| `Interpolator` | class | Rate interpolation (linear / flat_forward). |
| `ltn` | module | LTN bond data & analytics. |
| `ntnb` | module | NTN-B pricing, spot, forward, BEI. |
| `ntnc` | module | NTN-C (if available in current dataset). |
| `ntnf` | module | NTN-F DI spreads & related analytics. |
| `lft` | module | LFT bond tools. |
| `pre` | module | Zero-coupon (pré-fixado) helpers. |
| `ipca` | module | Inflation historical & projections. |

### Minimal Quick Start
```python
from pyield import bday, ntnb, bc

# Calendar
business_days = bday.count("02-01-2025", "15-01-2025")

# Bond quotation (base 100)
q = ntnb.quotation("31-05-2024", "15-05-2035", 0.06149)

# SELIC Over single day
selic = bc.selic_over("31-05-2024")

print(business_days, q, selic)
```

### Brazilian Clock Helpers
```python
from pyield import today, now, now_datetime

# Date in Brazil timezone
d = today()            # -> datetime.date

# Time in Brazil (naive time, no tz info embedded)
t = now()              # -> datetime.time

# Timezone-aware datetime (America/Sao_Paulo)
dt = now_datetime()    # -> datetime.datetime (aware)
```

## Installation

You can install PYield using pip:
```sh
pip install pyield
```
## How to use PYield
### Brazilian Treasury Bonds Tools
```python
>>> from pyield import ltn, ntnb, ntnf

# Get ANBIMA LTN data for a given date
>>> ltn.data("23-08-2024")
shape: (13, 14)
┌──────────────┬─────────┬──────────┬──────────────┬───┬─────────┬─────────┬───────────────┬────────┐
│ ReferenceDate│ BondType│ SelicCode│ IssueBaseDate│ … │ BidRate │ AskRate │ IndicativeRate│ DIRate │
│ ---          │ ---     │ ---      │ ---          │   │ ---     │ ---     │ ---           │ ---    │
│ date         │ str     │ i64      │ date         │   │ f64     │ f64     │ f64           │ f64    │
├──────────────┼─────────┼──────────┼──────────────┼───┼─────────┼─────────┼───────────────┼────────┤
│ 2024-08-23   │ LTN     │ 100000   │ 2022-07-08   │ … │ 0.10459 │ 0.104252│ 0.104416      │ 0.10472│
│ 2024-08-23   │ LTN     │ 100000   │ 2018-02-01   │ … │ 0.107366│ 0.107016│ 0.107171      │ 0.10823│
│ 2024-08-23   │ LTN     │ 100000   │ 2023-01-06   │ … │ 0.110992│ 0.110746│ 0.110866      │ 0.11179│
│ …            │ …       │ …        │ …            │ … │ …       │ …       │ …             │ …      │
└──────────────┴─────────┴──────────┴──────────────┴───┴─────────┴─────────┴───────────────┴────────┘

# Calculate the quotation of an NTN-B bond (base 100, truncated to 4 decimals)
>>> ntnb.quotation("31-05-2024", "15-05-2035", 0.061490)
99.3651
>>> ntnb.quotation("31-05-2024", "15-08-2060", 0.061878)
99.5341

# DI Spreads: IndicativeRate - SettlementRate (bps=True multiplies by 10_000)
>>> ntnf.di_spreads("30-05-2025", bps=True)
shape: (5, 3)
┌─────────┬─────────────┬──────────┐
│ BondType│ MaturityDate│ DISpread │
│ ---     │ ---         │ ---      │
│ str     │ date        │ f64      │
├─────────┼─────────────┼──────────┤
│ NTN-F   │ 2027-01-01  │ -3.31    │
│ NTN-F   │ 2029-01-01  │ 14.21    │
│ NTN-F   │ 2031-01-01  │ 21.61    │
│ NTN-F   │ 2033-01-01  │ 11.51    │
│ NTN-F   │ 2035-01-01  │ 22.0     │
└─────────┴─────────────┴──────────┘
```

### Business Days Tools (Brazilian holidays automatically considered)
```python
>>> from pyield import bday
# Count business days (start inclusive, end exclusive)
>>> bday.count("29-12-2023", "02-01-2024")
1

# Next business day after given date (offset=1)
>>> bday.offset("29-12-2023", 1)
datetime.date(2024, 1, 2)

# Adjust to next business day when not a business day (offset=0)
>>> bday.offset("30-12-2023", 0)
datetime.date(2024, 1, 2)

# Returns same date if already business day (offset=0)
>>> bday.offset("29-12-2023", 0)
datetime.date(2023, 12, 29)
```

## Custom Types & Data Handling

This section documents the internal typing and data normalization rules adopted across PYield so users can rely on predictable inputs/outputs and null semantics.

### DateLike (scalar inputs)
`DateLike` is a union used anywhere a single date is accepted:

* `str` (accepted formats below)
* `datetime.date`
* `datetime.datetime`
* `pandas.Timestamp`
* `numpy.datetime64`

Returned scalar dates are normalized to `datetime.date` (no timezone).

### ArrayLike (collection inputs)
`ArrayLike` covers any homogeneous collection of values accepted by vectorized functions:

* `Sequence[Any]` (e.g. `list`, `tuple`)
* `pandas.Series`
* `polars.Series`
* `numpy.ndarray`

Internally collections of date-like values are converted to a `polars.Series` with dtype `Date`.

### Supported Date String Formats

* Brazilian day‑first (dash): `DD-MM-YYYY`  (e.g. `31-05-2024`)
* Brazilian day‑first (slash): `DD/MM/YYYY` (e.g. `31/05/2024`)
* ISO: `YYYY-MM-DD` (e.g. `2024-05-31`)

Rules:
1. No ambiguous inference: `2024-05-06` is always ISO (`YYYY-MM-DD`).
2. Collections: the first non-null/non-empty string determines the format for the entire collection.
3. Strings not matching that inferred format become `null` (instead of raising) to keep vectorization robust.
4. A collection containing only null/empty strings becomes a `Date` Series of all nulls.

Recommendation (defensive pipelines):
```python
import pandas as pd
dt_val = pd.to_datetime("31-05-2024", format="%d-%m-%Y")
iso_val = pd.to_datetime("2024-05-31", format="%Y-%m-%d")
```

### Conversion Summary

| Input                               | Output Normalization                 |
|-------------------------------------|--------------------------------------|
| "31-05-2024"                        | datetime.date(2024, 5, 31)           |
| ["31-05-2024", "01-06-2024"]        | polars.Series<Date>[2024-05-31,…]    |
| ["31-05-2024", None]                | polars.Series<Date>[2024-05-31,null] |
| ["31-05-2024", "2024-06-01"]        | second -> null (mismatched format)   |
| [] (where shape mandatory)          | ValueError (function-dependent)      |

### Nullability & Early Short-Circuit

The internal helper `has_nullable_args(*args)` returns `True` if any argument is considered "nullable". An argument is nullable when it is:

* `None`
* `NaN` (float)
* Empty string `""`
* Empty collection: `[]`, `()`, `{}`
* Empty pandas `DataFrame` / `Series` / `Index`
* Empty polars `DataFrame` / `Series`
* NumPy `ndarray` with `size == 0`

Policy:
* Scalar-style functions:
    * Pure date counters/adjusters may return `None` when a required scalar date is missing.
    * Valuation / rate functions (`quotation`, `price`, `duration`, `dv01`) return `float('nan')` when inputs are nullable or a cash-flow schedule cannot be built.
* Vectorized functions: an entirely missing/empty driving input yields an empty `DataFrame`/`Series` (or `None` if semantics are purely scalar); element-level nulls propagate.
* Arithmetic emits `NaN` only for truly undefined operations—most missing cases short-circuit sooner (or return `nan` as above for valuation functions).

### Practical Examples
```python
>>> from pyield import ntnb, bday

# Missing settlement -> nan (empty cash-flow schedule)
>>> ntnb.quotation(None, "15-05-2035", 0.06149)
nan

# Date output with null start date -> None
>>> bday.count(None, "01-01-2025")
None

# Element-level null propagation
>>> bday.count(["01-01-2024", None], "01-02-2024")
shape: (2,)
Series: 'bdays' [i64]
[
    22
    null
]
```

## Tests
To run the test suite, use the following command:
pytest pyield --doctest-modules