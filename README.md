[![PyPI version](https://img.shields.io/pypi/v/pyield.svg)](https://pypi.python.org/pypi/pyield)
[![Made with Python](https://img.shields.io/badge/Python->=3.12-blue?logo=python&logoColor=white)](https://python.org "Go to Python homepage")
[![License](https://img.shields.io/badge/License-MIT-blue)](https://github.com/crdcj/PYield/blob/main/LICENSE)

# PYield: Brazilian Fixed Income Toolkit

PYield is a Python library for analyzing Brazilian fixed income instruments. It fetches and processes data from ANBIMA, BCB, IBGE and B3, returning Polars DataFrames for fast, type-safe data pipelines.

## Installation

```sh
pip install pyield
```

## Quick Start

```python
import pyield as yd

# Business days (foundation of all calculations)
yd.bday.count("02-01-2025", "15-01-2025")  # -> 9
yd.bday.offset("29-12-2023", 1)            # -> datetime.date(2024, 1, 2)

# DI Futures curve
df = yd.futures("31-05-2024", "DI1")
# Columns: TradeDate, TickerSymbol, ExpirationDate, BDaysToExp, SettlementRate, ...

# Rate interpolation (flat forward, 252 bdays/year convention)
interp = yd.Interpolator("flat_forward", df["BDaysToExp"], df["SettlementRate"])
interp(45)       # -> 0.04833...
interp([30, 60]) # -> pl.Series with interpolated rates

# Treasury bond pricing
yd.ntnb.quotation("31-05-2024", "15-05-2035", 0.061490)  # -> 99.3651

# BCB indicators
yd.bc.selic_over("31-05-2024")  # -> 0.000414...
```

A Colab notebook with more examples is available:

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/crdcj/PYield/blob/main/examples/pyield_quickstart.ipynb)

## Core Building Blocks

### Business Days (`bday`)

The `bday` module is the foundation of PYield. All date calculations (pricing, duration, forward rates) depend on accurate business day counting with Brazilian holidays.

```python
from pyield import bday

# Count business days (start inclusive, end exclusive)
bday.count("29-12-2023", "02-01-2024")  # -> 1

# Offset by N business days
bday.offset("29-12-2023", 1)  # -> datetime.date(2024, 1, 2)

# Adjust non-business day to next business day
bday.offset("30-12-2023", 0)  # -> datetime.date(2024, 1, 2)

# Generate business day range
bday.generate("22-12-2023", "02-01-2024")
# -> Series: [2023-12-22, 2023-12-26, 2023-12-27, 2023-12-28, 2023-12-29, 2024-01-02]

# Check if date is business day
bday.is_business_day("25-12-2023")  # -> False (Christmas)
```

All functions support vectorized operations with lists, Series, or arrays.

### Rate Interpolation (`Interpolator`)

The `Interpolator` class interpolates interest rates using the 252 business days/year convention standard in Brazil.

```python
from pyield import Interpolator

known_bdays = [30, 60, 90]
known_rates = [0.045, 0.05, 0.055]

# Flat forward interpolation (market standard)
interp = Interpolator("flat_forward", known_bdays, known_rates)
interp(45)  # -> 0.04833...

# Linear interpolation
linear = Interpolator("linear", known_bdays, known_rates)
linear(45)  # -> 0.0475

# Vectorized
interp([15, 45, 75])  # -> pl.Series with 3 rates

# Extrapolation (disabled by default, returns NaN)
interp(100)  # -> nan
Interpolator("flat_forward", known_bdays, known_rates, extrapolate=True)(100)  # -> 0.055
```

### Forward Rates (`forward`, `forwards`)

Calculate forward rates from spot curves:

```python
from pyield import forward, forwards

# Single forward rate between two points
forward(bday1=10, bday2=20, rate1=0.05, rate2=0.06)  # -> 0.0700952...

# Vectorized forward curve from spot rates
bdays = [10, 20, 30]
rates = [0.05, 0.06, 0.07]
forwards(bdays, rates)  # -> Series: [0.05, 0.070095, 0.090284]
```

## Modules Overview

| Module | Purpose |
|--------|---------|
| `bday` | Business day calendar with Brazilian holidays |
| `futures` | B3 futures data (DI1, DDI, FRC, DAP, DOL, WDO, IND, WIN) |
| `Interpolator` | Rate interpolation (flat_forward, linear) |
| `forward` / `forwards` | Forward rate calculation |
| `ltn`, `ntnb`, `ntnf`, `lft`, `ntnc` | Treasury bond pricing and analytics |
| `anbima` | ANBIMA data (TPF prices, yield curves, IMA indices) |
| `bc` | BCB indicators (SELIC, PTAX, repo rates, VNA) |
| `ipca` | Inflation data (historical and projections) |
| `today` / `now` | Current date/time in Brazil (America/Sao_Paulo) |

## Treasury Bonds

```python
from pyield import ltn, ntnb, ntnf

# Fetch ANBIMA indicative rates
ltn.data("23-08-2024")   # -> DataFrame with LTN bonds
ntnb.data("23-08-2024")  # -> DataFrame with NTN-B bonds

# Calculate bond quotation (base 100)
ntnb.quotation("31-05-2024", "15-05-2035", 0.061490)  # -> 99.3651
ntnb.quotation("31-05-2024", "15-08-2060", 0.061878)  # -> 99.5341

# DI spreads (bps=True multiplies by 10,000)
ntnf.di_spreads("30-05-2025", bps=True)
# -> DataFrame: BondType, MaturityDate, DISpread
```

## Futures Data

```python
from pyield import futures

# DI1 (Interbank Deposit Futures)
futures("31-05-2024", "DI1")

# Other contracts: DDI, FRC, DAP, DOL, WDO, IND, WIN
futures("31-05-2024", "DAP")

# Intraday data (when market is open)
futures("16-01-2025", "DI1")  # Returns live data if called during trading hours
```

## Date Handling

PYield accepts flexible date inputs (`DateLike`):
- Strings: `"31-05-2024"`, `"31/05/2024"`, `"2024-05-31"`
- `datetime.date`, `datetime.datetime`
- `pandas.Timestamp`, `numpy.datetime64`

Scalar functions return `datetime.date`. Vectorized functions return `polars.Series`.

Null handling: scalar functions return `float('nan')` for missing inputs (propagates through calculations). Vectorized functions propagate `null` element-wise.

```python
from pyield import ntnb, bday

ntnb.quotation(None, "15-05-2035", 0.06149)  # -> nan
bday.count(["01-01-2024", None], "01-02-2024")  # -> Series: [22, null]
```

## Polars Migration (v0.40.0+)

All functions return **Polars DataFrames/Series**. To convert to Pandas:

```python
df_pandas = df.to_pandas(use_pyarrow_extension_array=True)
```

## Documentation

Full documentation: [crdcj.github.io/PYield](https://crdcj.github.io/PYield/)

## Tests

```sh
pytest pyield --doctest-modules
```
