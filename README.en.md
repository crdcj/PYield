[![PyPI version](https://img.shields.io/pypi/v/pyield.svg)](https://pypi.python.org/pypi/pyield)
[![Made with Python](https://img.shields.io/badge/Python->=3.12-blue?logo=python&logoColor=white)](https://python.org "Go to Python homepage")
[![Powered by Polars](https://img.shields.io/badge/Powered%20by-Polars-blue)](https://pola.rs/)
[![License](https://img.shields.io/badge/License-MIT-blue)](https://github.com/crdcj/PYield/blob/main/LICENSE)

# PYield: Brazilian Fixed Income Toolkit

[Português](README.md) | English

PYield is a Polars-powered Python toolkit for Brazilian fixed income analysis,
with a primary focus on Brazilian treasury bonds. It fetches and processes data
from ANBIMA, BCB, IBGE, B3, and **Tesouro Nacional**.

Scalar outputs return native Python types, while non-scalar outputs return
`polars.Series` or `polars.DataFrame`, depending on the function.

Although it includes data and tools from other markets (such as DI1, DAP, and PTAX), these resources support the core goal: analysis, pricing, and monitoring of Brazilian treasury bonds.

## Quick Links

- Full documentation: https://crdcj.github.io/PYield/
- Colab notebook: [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/crdcj/PYield/blob/main/examples/pyield_quickstart.ipynb)
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
interp(45)  # -> 0.04833...

# Treasury bond pricing
yd.ntnb.cotacao("31-05-2024", "15-05-2035", 0.061490)  # -> 0.993651

# BCB indicators
yd.selic.over("31-05-2024")  # -> 0.000414...
```

Scalar dates accept `DD-MM-YYYY`, `DD/MM/YYYY`, and `YYYY-MM-DD`. Malformed
scalar dates raise `ValueError`; in vectorized operations, malformed elements
become `null` so the Polars pipeline can continue.

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

# Extrapolation on the long end: disabled by default (NaN). The short end
# always returns the first known rate.
interp(100)  # -> nan
Interpolador(dias_uteis, taxas, metodo="flat_forward", extrapolar=True)(100)  # -> 0.055
```

To interpolate a full column inside a Polars pipeline, use `interpolar_expr`:

```python
import polars as pl

df = pl.DataFrame({"du": [15, 45, 75]})
df.with_columns(taxa=interp.interpolar_expr("du"))
```

When target points and the curve come from different DataFrames (including
multiple reference dates), use the top-level `yd.interpolar` function:

```python
import pyield as yd

rates = yd.interpolar(
    dus_alvo=df_alvo["dias_uteis"],
    dus_curva=df_curva["dias_uteis"],
    taxas_curva=df_curva["taxa"],
    datas_alvo=df_alvo["data_referencia"],   # optional (multi-curve)
    datas_curva=df_curva["data_referencia"], # optional (multi-curve)
)
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
| `tpf` | Rates, maturities, outstanding stock, auctions, benchmarks, RMD, and trades for TPFs |
| `di1` | Interpolated DI1 curve and available trade dates |
| `Interpolador` | Scalar rate interpolation and Polars-expression interpolation (flat_forward, linear) |
| `interpolar` | Vectorized flat-forward interpolation, single curve or multi-curve |
| `forward` / `forwards` | Forward-rate calculations |
| `ltn`, `ntnb`, `ntnf`, `lft`, `ntnc` | Pricing and analysis of main treasury bonds |
| `ntnb1`, `ntnbprinc` | Additional bonds (NTN-B1, NTN-B Principal) |
| `selic` | Selic rate data, COPOM calendar, BCB repos, CPM options and implied probabilities |
| `ipca` | Inflation data (historical and projections) |
| `hoje` / `agora` | Current date/time in Brazil (America/Sao_Paulo) |

## Treasury Bonds

```python
from pyield import ltn, ntnb, ntnf

# Fetch ANBIMA indicative rates
ltn.dados("23-08-2024")  # -> DataFrame with LTN bonds
ntnb.dados("23-08-2024")  # -> DataFrame with NTN-B bonds

# Compute bond quotation (base 1)
ntnb.cotacao("31-05-2024", "15-05-2035", 0.061490)  # -> 0.993651
ntnb.cotacao("31-05-2024", "15-08-2060", 0.061878)  # -> 0.995341

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

Scalar dates are converted to `datetime.date`. Where a function permits a
missing date, `None` or an empty string represents an omitted value. A malformed
scalar date raises `ValueError`.

In vectorized operations, missing or malformed elements become `null` so the
Polars pipeline can continue. Domain functions that require a fully valid
collection may reject those nulls after conversion.

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
object rather than the original data source. Migration map:

| Before | After |
|---|---|
| `yd.b3.futuro(data, contrato)` | `yd.futuro.historico(data, contrato)` |
| `yd.b3.futuro_intradia(contrato)` | `yd.futuro.intradia(contrato)` |
| `yd.b3.futuro_datas_disponiveis(contrato)` | `yd.futuro.datas_disponiveis(contrato)` |
| `yd.b3.futuro_enriquecer(df, contrato)` | `yd.futuro.enriquecer(df, contrato)` |
| `yd.b3.di_over(data)` | `yd.di_over(data)` |
| `yd.b3.di1.dados(data)` | `yd.di1.dados(data)` |
| `yd.bc.ptax(data)` | `yd.ptax(data)` |
| `yd.bc.selic_over(data)` | `yd.selic.over(data)` |
| `yd.selic_over(data)` | `yd.selic.over(data)` |
| `yd.selic_over_serie(...)` | `yd.selic.over_serie(...)` |
| `yd.selic_meta(data)` | `yd.selic.meta(data)` |
| `yd.selic_meta_serie(...)` | `yd.selic.meta_serie(...)` |
| `yd.copom` | `yd.selic.copom` |
| `yd.copom_options(data)` | `yd.selic.cpm.data(data)` |
| `yd.compromissadas(...)` | `yd.selic.compromissadas(...)` |
| `yd.anbima.tpf(data, titulo)` | `yd.tpf.taxas(data, titulo)` |
| `yd.anbima.tpf_vencimentos(data, titulo)` | `yd.tpf.vencimentos(data, titulo)` |
| `yd.anbima.imaq(data)` | `yd.tpf.estoque(data)` |
| `yd.tn.leilao(data)` | `yd.tpf.leiloes(data=...)` |
| `yd.bc.tpf_intradia()` | `yd.tpf.secundario_intradia()` |
| `yd.bc.tpf_mensal(data, extragrupo=...)` | `yd.tpf.secundario_mensal(data, extragrupo=...)` |
| `yd.bc.vna_lft(data)` | `yd.lft.vna(data)` |
| `yd.tn.benchmarks(...)` | `yd.tpf.benchmarks(...)` |
| `yd.pre.taxas_zero(data)` | `yd.tpf.curva_pre(data)` |

The old high-level source aliases listed above were removed from the public API.

## Tests

```sh
pytest
```
