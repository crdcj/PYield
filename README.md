[![PyPI version](https://img.shields.io/pypi/v/pyield.svg)](https://pypi.python.org/pypi/pyield)
[![Made with Python](https://img.shields.io/badge/Python->=3.12-blue?logo=python&logoColor=white)](https://python.org "Go to Python homepage")
[![License](https://img.shields.io/badge/License-MIT-blue)](https://github.com/crdcj/PYield/blob/main/LICENSE)

# PYield: Brazilian Fixed Income Toolkit

PYield is a Python library designed for the analysis of Brazilian fixed income instruments. Leveraging the power of popular Python libraries like Polars, Pandas, Numpy and Requests, PYield simplifies the process of obtaining and processing data from key sources such as ANBIMA, BCB, IBGE and B3.

---
### ✅ Polars Migration Completed

All public functions now return **Polars DataFrames or Series** as the canonical format. This provides stronger typing, faster execution and more reliable date/rate handling.

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

## Installation

You can install PYield using pip:
```sh
pip install pyield
```
## Custom Types

### DateScalar
`DateScalar` and `DateArray` are internal type unions used across PYield to accept flexible date inputs. Supported scalar Python / library date types:

- `str` (formats: `DD-MM-YYYY`, `DD/MM/YYYY`, `YYYY-MM-DD`)
- `datetime.date`
- `datetime.datetime`
- `pandas.Timestamp`
- `numpy.datetime64`

### DateArray
Accepted collection types (homogeneous date-like values):

- `list[DateScalar]`
- `tuple[DateScalar, ...]`
- `pandas.Series`
- `pandas.DatetimeIndex`
- `numpy.ndarray`
- `polars.Series`

Other helper unions:

`FloatArray`:
- `list[float]` | `tuple[float, ...]` | `numpy.ndarray` | `pandas.Series` | `polars.Series`

`IntegerScalar`:
- `int` | `numpy.integer`

`IntegerArray`:
- `list[int]` | `tuple[int, ...]` | `numpy.ndarray` | `pandas.Series` | `polars.Series`

Referencing these unions in function docstrings means you can pass any of the listed types interchangeably; conversion is handled internally.

### Date String Formats
Accepted string date formats:

- Day-first (Brazilian): `DD-MM-YYYY` (e.g., `31-05-2024`)
- Day-first (slash): `DD/MM/YYYY` (e.g., `31/05/2024`)
- ISO: `YYYY-MM-DD` (e.g., `2024-05-31`)

Rules:
- No ambiguous autodetection: `2024-05-06` is always interpreted as ISO (`YYYY-MM-DD`).
- A collection of strings must not mix different styles; the first non-null value defines the format.
- Nulls are preserved; empty collections are not allowed.

Recommendation:
Always parse external inputs explicitly when constructing your own pipelines:
```python
import pandas as pd
dt_val = pd.to_datetime("31-05-2024", format="%d-%m-%Y")
iso_val = pd.to_datetime("2024-05-31", format="%Y-%m-%d")
```

### Null & Empty Input Handling

PYield uses an internal helper (`has_null_args`) for early detection of missing inputs. The default propagation policy is: return **`None`** for missing scalar inputs and preserve nulls inside collections. This avoids implicit imputation and makes failure modes explicit.

Summary:
- Scalar functions (dates, prices, quotations, spreads, durations): return `None` when any required argument is missing or empty.
- Collection (vectorized) functions: if the entire relevant input is missing/empty, return an empty Polars `DataFrame`/`Series` (or `None` for purely scalar semantics). Individual null elements propagate as `null` values in the resulting Polars Series/DataFrame.
- Empty collections where a shape is mandatory (e.g. an empty date array for conversion) raise `ValueError` rather than returning a silently empty result.
- Date string collections must share a single format; the first non-null defines it.
- Numeric computations only produce `NaN` when an internal arithmetic step yields an undefined value (rare — typical missing input short-circuits to `None`).

Examples:
```python
>>> from pyield import ntnb, bday
# Missing settlement -> None
>>> ntnb.quotation(None, "15-05-2035", 0.06149)
None

# Null start date in business day count -> None
>>> bday.count(None, "01-01-2025")
None

# Null in array input propagates element-wise
>>> bday.count(["01-01-2024", None], "01-02-2024")
shape: (2,)
Series: 'bdays' [i64]
[
    22
    null
]
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

# Generate business day series
>>> bday.generate(start="22-12-2023", end="02-01-2024")
shape: (6,)
Series: '' [date]
[
    2023-12-22
    2023-12-26
    2023-12-27
    2023-12-28
    2023-12-29
    2024-01-02
]
```

### Futures Data
```python
>>> from pyield.b3.futures import futures

# Fetch DI1 futures (historical)
>>> futures("DI1", "31-05-2024")
shape: (40, 20)
┌────────────┬──────────────┬───────────────┬────────────┬───┬──────────────┬──────────┬───────────────┬─────────────┐
│ TradeDate  ┆ TickerSymbol ┆ ExpirationDate┆ BDaysToExp ┆ … ┆ CloseBidRate ┆ CloseRate┆ SettlementRate┆ ForwardRate │
│ ---        ┆ ---          ┆ ---           ┆ ---        ┆   ┆ ---          ┆ ---      ┆ ---           ┆ ---         │
│ date       ┆ str          ┆ date          ┆ i64        ┆   ┆ f64          ┆ f64      ┆ f64           ┆ f64         │
├────────────┼──────────────┼───────────────┼────────────┼───┼──────────────┼──────────┼───────────────┼─────────────┤
│ 2024-05-31 ┆ DI1M24       ┆ 2024-06-03    ┆ 1          ┆ … ┆ 0.10404      ┆ 0.10404  ┆ 0.10399       ┆ 0.10399     │
│ 2024-05-31 ┆ DI1N24       ┆ 2024-07-01    ┆ 21         ┆ … ┆ 0.1039       ┆ 0.10386  ┆ 0.1039        ┆ 0.103896    │
│ 2024-05-31 ┆ DI1Q24       ┆ 2024-08-01    ┆ 44         ┆ … ┆ 0.10374      ┆ 0.10374  ┆ 0.1037        ┆ 0.103517    │
│ …          ┆ …            ┆ …             ┆ …          ┆ … ┆ …            ┆ …        ┆ …             ┆ …           │
└────────────┴──────────────┴───────────────┴────────────┴───┴──────────────┴──────────┴───────────────┴─────────────┘
```

### Indicators Data
```python
>>> from pyield import bc

# SELIC Over series (no data on Sunday)
>>> bc.selic_over_series("26-01-2025").head(5)
shape: (5, 2)
┌────────────┬────────┐
│ Date       ┆ Value  │
│ ---        ┆ ---    │
│ date       ┆ f64    │
├────────────┼────────┤
│ 2025-01-27 ┆ 0.1215 │
│ 2025-01-28 ┆ 0.1215 │
│ 2025-01-29 ┆ 0.1215 │
│ 2025-01-30 ┆ 0.1315 │
│ 2025-01-31 ┆ 0.1315 │
└────────────┴────────┘

# SELIC Over for a single date
>>> bc.selic_over("31-05-2024")
0.104  # 10.40%
```

### Projections Data
```python
>>> from pyield import ipca
# Fetch current month projection for IPCA
>>> proj = ipca.projected_rate()
>>> proj
IndicatorProjection(last_updated=..., reference_period=..., projected_value=...)
>>> proj.projected_value
0.0035  # 0.35%
```

### Interpolation Tools
Interpolate interest rates for specific business days using the Interpolator class.
```python
>>> from pyield import Interpolator
# Initialize the Interpolator with known business days and interest rates.
>>> known_bdays = [30, 60, 90]
>>> known_rates = [0.045, 0.05, 0.055]
>>> linear_interpolator = Interpolator("linear", known_bdays, known_rates)
>>> linear_interpolator(45)  # Interpolate the interest rate for a given number of business days.
0.0475

# Use the flat forward method for interpolation.
>>> ff_interpolator = Interpolator("flat_forward", known_bdays, known_rates)
>>> ff_interpolator(45)
0.04833068080970859
```
