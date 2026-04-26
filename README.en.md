[![PyPI version](https://img.shields.io/pypi/v/pyield.svg)](https://pypi.python.org/pypi/pyield)
[![Made with Python](https://img.shields.io/badge/Python->=3.12-blue?logo=python&logoColor=white)](https://python.org "Go to Python homepage")
[![License](https://img.shields.io/badge/License-MIT-blue)](https://github.com/crdcj/PYield/blob/main/LICENSE)

# PYield: Brazilian Fixed Income Toolkit

[Português](README.md) | English

PYield is a Python library focused on Brazilian treasury bond analysis. It fetches and processes data from ANBIMA, BCB, IBGE, B3, and **Tesouro Nacional**, returning Polars DataFrames for fast, type-consistent pipelines.

Although it includes data and tools from other markets (such as DI1, DAP, and PTAX), these resources support the core goal: analysis, pricing, and monitoring of Brazilian treasury bonds.

## Quick Links

- Full documentation: https://crdcj.github.io/PYield/
- Colab notebook: https://colab.research.google.com/github/crdcj/PYield/blob/main/examples/pyield_quickstart.ipynb
- Package on PyPI: https://pypi.org/project/pyield/

## Installation

```sh
pip install pyield
```

## Quick Start

```python
import pyield as yd

# Business days (foundation for all calculations)
yd.du.contar("02-01-2025", "15-01-2025")  # -> 9
yd.du.deslocar("29-12-2023", 1)           # -> datetime.date(2024, 1, 2)

# DI future curve
df = yd.futuro.historico("31-05-2024", "DI1")
# Columns: data_referencia, codigo_negociacao, data_vencimento, dias_uteis, taxa_ajuste, ...

# Rate interpolation (flat forward, 252 business days/year convention)
interp = yd.Interpolador(df["dias_uteis"], df["taxa_ajuste"], metodo="flat_forward")
interp(45)       # -> 0.04833...
interp([30, 60]) # -> pl.Series with interpolated rates

# Treasury bond pricing
yd.ntnb.cotacao("31-05-2024", "15-05-2035", 0.061490)  # -> 99.3651

# BCB indicators
yd.selic_over("31-05-2024")  # -> 0.000414...
```

A Colab notebook with more examples:

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/crdcj/PYield/blob/main/examples/pyield_quickstart.ipynb)

## Core Building Blocks

### Business Days (`du`)

The `du` module is the foundation of PYield. All date-based calculations (price, duration, forward rates) depend on correct business-day counting with Brazilian holidays.

```python
from pyield import du

# Count business days (start inclusive, end exclusive)
du.contar("29-12-2023", "02-01-2024")  # -> 1

# Move by N business days
du.deslocar("29-12-2023", 1)  # -> datetime.date(2024, 1, 2)

# Adjust non-business day to next business day
du.deslocar("30-12-2023", 0)  # -> datetime.date(2024, 1, 2)

# Generate business day range
du.gerar("22-12-2023", "02-01-2024")
# -> Series: [2023-12-22, 2023-12-26, 2023-12-27, 2023-12-28, 2023-12-29, 2024-01-02]

# Check whether a date is a business day
du.eh_dia_util("25-12-2023")  # -> False (Christmas)
```

All functions support vectorized operations with lists, Series, or arrays.

### Rate Interpolation (`Interpolador`)

The `Interpolador` class interpolates rates using the 252 business days/year convention, standard in the Brazilian market.

```python
from pyield import Interpolador

dias_uteis = [30, 60, 90]
taxas = [0.045, 0.05, 0.055]

# Flat-forward interpolation (market standard)
interp = Interpolador(dias_uteis, taxas, metodo="flat_forward")
interp(45)  # -> 0.04833...

# Linear interpolation
linear = Interpolador(dias_uteis, taxas, metodo="linear")
linear(45)  # -> 0.0475

# Vectorized
interp([15, 45, 75])  # -> pl.Series with 3 rates

# Extrapolation (disabled by default, returns NaN)
interp(100)  # -> nan
Interpolador(dias_uteis, taxas, metodo="flat_forward", extrapolar=True)(100)  # -> 0.055
```

### Forward Rates (`forward`, `forwards`)

Compute forward rates from spot curves:

Convention used:

- `fwd_k = fwd_{j->k}` (forward from vertex `j` to `k`)
- `f_k = 1 + tx_k` (capitalization factor at `k`)
- `fwd_k = (f_k^au_k / f_j^au_j)^(1 / (au_k - au_j)) - 1`, with `au = du / 252`

```python
from pyield import forward, forwards

# Single forward rate between two points
forward(10, 20, 0.05, 0.06)  # -> 0.0700952...

# Vectorized forward curve from spot rates
dias_uteis = [10, 20, 30]
taxas = [0.05, 0.06, 0.07]
forwards(dias_uteis, taxas)  # -> Series: [0.05, 0.070095, 0.090284]
```

## Module Overview

| Module | Purpose |
|--------|---------|
| `du` | Business day calendar with Brazilian holidays |
| `futuro` | Futures data (DI1, DDI, DAP, DOL, WDO, IND, WIN and others) |
| `tpf` | Rates, maturities, outstanding stock, and trades for federal government bonds |
| `di1` | Interpolated DI1 curve and available trade dates |
| `Interpolador` | Rate interpolation (flat_forward, linear) |
| `forward` / `forwards` | Forward-rate calculations |
| `ltn`, `ntnb`, `ntnf`, `lft`, `ntnc` | Pricing and analysis of main treasury bonds |
| `ntnb1`, `ntnbprinc`, `pre` | NTN-B, variants and PRE curve |
| `tpf.leilao` / `tpf.benchmarks` | Treasury bond auctions and benchmarks |

| `bc` | Technical BCB data (repos, auctions, trades) |
| `b3` | Technical B3 data (price reports, intradia derivatives) |
| `ipca` | Inflation data (historical and projections) |
| `selic` | COPOM digital options and implied probabilities |
| `tn.rmd` | Monthly Debt Report (RMD) from Tesouro Nacional |
| `hoje` / `agora` | Current date/time in Brazil (America/Sao_Paulo) |

## Treasury Bonds

```python
from pyield import ltn, ntnb, ntnf

# Fetch ANBIMA indicative rates
ltn.dados("23-08-2024")  # -> DataFrame with LTN bonds
ntnb.dados("23-08-2024")  # -> DataFrame with NTN-B bonds

# Compute bond quotation (base 100)
ntnb.cotacao("31-05-2024", "15-05-2035", 0.061490)  # -> 99.3651
ntnb.cotacao("31-05-2024", "15-08-2060", 0.061878)  # -> 99.5341

# DI premium (pontos_base=True multiplies by 10,000)
ntnf.premio("30-05-2025", pontos_base=True)
# -> DataFrame: titulo, data_vencimento, premio
```

## Futures Data

```python
import pyield as yd

# DI1 (Interbank Deposit Futures)
yd.futuro.historico("31-05-2024", "DI1")

# Other available contracts in the historical cache:
# - Rates: DI1, DDI, FRC, FRO, DAP
# - Currencies: DOL, WDO
# - Indexes: IND, WIN
yd.futuro.historico("31-05-2024", "DAP")

# Multiple dates at once
yd.futuro.historico(["29-05-2024", "31-05-2024"], "DI1")

# Intraday data (when the market is open)
yd.futuro.intradia("DI1")  # Returns live data during trading hours
```

## Date Handling

PYield accepts flexible date inputs (`DateLike`):
- Strings: `"31-05-2024"`, `"31/05/2024"`, `"2024-05-31"`
- `datetime.date`, `datetime.datetime`
- `pandas.Timestamp`, `numpy.datetime64`

Scalar functions return `datetime.date`. Vectorized functions return `polars.Series`.

String parsing is performed element-by-element across supported formats. Invalid strings are converted to null values (`None` in scalar outputs and `null` in vectorized outputs).

Null handling: scalar functions return `float('nan')` for missing inputs (which propagates in calculations). Vectorized functions propagate `null` element-by-element.

```python
from pyield import ntnb, du

ntnb.cotacao(None, "15-05-2035", 0.06149)  # -> nan
du.contar(["01-01-2024", None], "01-02-2024")  # -> Series: [22, null]
```

Unavailable-data queries (future dates, holidays, weekends, or unavailable
sources) return an empty DataFrame or `nan`, without raising exceptions:

```python
import pyield as yd

yd.futuro.historico("01-01-2030", "DI1").is_empty()  # -> True
yd.tpf.secundario_mensal("01-01-2030").is_empty()    # -> True
yd.ptax("25-12-2025")                                # -> nan
```

## Object-Oriented API Migration (v0.49.0)

Version 0.49.0 reorganizes the main public API around the user's financial
object rather than the original data source. Canonical queries moved away from
`b3`, `bc`, or `anbima` when the source is only an operational detail.

Migration map:

| Before | After |
|---|---|
| `yd.b3.futuro(data, contrato)` | `yd.futuro.historico(data, contrato)` |
| `yd.b3.futuro_intradia(contrato)` | `yd.futuro.intradia(contrato)` |
| `yd.b3.futuro_datas_disponiveis(contrato)` | `yd.futuro.datas_disponiveis(contrato)` |
| `yd.b3.futuro_enriquecer(df, contrato)` | `yd.futuro.enriquecer(df, contrato)` |
| `yd.b3.di_over(data)` | `yd.di_over(data)` |
| `yd.b3.di1.dados(data)` | `yd.di1.dados(data)` |
| `yd.bc.ptax(data)` | `yd.ptax(data)` |
| `yd.bc.selic_over(data)` | `yd.selic_over(data)` |
| `yd.anbima.tpf(data, titulo)` | `yd.tpf.taxas(data, titulo)` |
| `yd.anbima.tpf_vencimentos(data, titulo)` | `yd.tpf.vencimentos(data, titulo)` |
| `yd.anbima.imaq(data)` | `yd.tpf.estoque(data)` |
| `yd.tn.leilao(data)` | `yd.tpf.leilao(data)` |
| `yd.bc.tpf_intradia()` | `yd.tpf.secundario_intradia()` |
| `yd.bc.tpf_mensal(data, extragrupo=...)` | `yd.tpf.secundario_mensal(data, extragrupo=...)` |
| `yd.bc.vna_lft(data)` | `yd.lft.vna(data)` |
| `yd.tn.benchmarks(...)` | `yd.tpf.benchmarks(...)` |

The old high-level source aliases listed above were removed from the public API.
Source modules remain available for technical, raw, or source-specific data.

## Migration to Polars (v0.40.0+)

All functions return **Polars DataFrames/Series**. To convert to Pandas:

```python
df_pandas = df.to_pandas(use_pyarrow_extension_array=True)
```

## Tests

```sh
pytest
```
