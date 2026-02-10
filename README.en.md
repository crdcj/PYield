[![PyPI version](https://img.shields.io/pypi/v/pyield.svg)](https://pypi.python.org/pypi/pyield)
[![Made with Python](https://img.shields.io/badge/Python->=3.12-blue?logo=python&logoColor=white)](https://python.org "Go to Python homepage")
[![License](https://img.shields.io/badge/License-MIT-blue)](https://github.com/crdcj/PYield/blob/main/LICENSE)

# PYield: Brazilian Fixed Income Toolkit

[Português](README.md) | [English](README.en.md)

PYield is a Python library focused on Brazilian treasury bond analysis. It fetches and processes data from ANBIMA, BCB, IBGE, B3, and **Tesouro Nacional**, returning Polars DataFrames/Series.

It also includes data and tools from related markets (such as DI1, DAP, and PTAX), but these components primarily support the core goal: treasury bond analysis, pricing, and monitoring.

For the complete documentation and examples in Portuguese, see:
- `README.md`
- https://crdcj.github.io/PYield/

## Installation

```sh
pip install pyield
```

## Quick Start

```python
import pyield as yd

# Business days
yd.bday.count("02-01-2025", "15-01-2025")  # -> 9

# DI Futures
df = yd.futures("31-05-2024", "DI1")

# Rate interpolation
interp = yd.Interpolator("flat_forward", df["BDaysToExp"], df["SettlementRate"])
interp(45)  # -> 0.04833...

# Treasury bond pricing
yd.ntnb.quotation("31-05-2024", "15-05-2035", 0.061490)  # -> 99.3651
```
