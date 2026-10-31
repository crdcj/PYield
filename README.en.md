[![PyPI version](https://img.shields.io/pypi/v/pyield.svg)](https://pypi.python.org/pypi/pyield)
[![Made with Python](https://img.shields.io/badge/Python->=3.12-blue?logo=python&logoColor=white)](https://python.org "Go to Python homepage")
[![Powered by Polars](https://img.shields.io/badge/Powered%20by-Polars-blue)](https://pola.rs/)
[![License](https://img.shields.io/badge/License-MIT-blue)](https://github.com/crdcj/PYield/blob/main/LICENSE)
[![Docs](https://img.shields.io/badge/docs-GitHub%20Pages-blue?logo=readthedocs&logoColor=white)](https://crdcj.github.io/PYield/)

# PYield: Brazilian Fixed Income Toolkit

[Português](README.md) | English

PYield is a Python library for Brazilian fixed-income analysis. It fetches and
processes data from ANBIMA, BCB, IBGE, B3, and Tesouro Nacional, returning
native Python types, `polars.Series`, or `polars.DataFrame` depending on the
operation.

Although it includes data and tools from other markets, such as DI1, DAP, and
PTAX, these resources support the library's central purpose: analyzing, pricing,
and monitoring Brazilian treasury bonds.

## Installation

With `pip`:

```sh
pip install pyield
```

In a project managed with `uv`:

```sh
uv add pyield
```

## Next steps

- [Quickstart](https://crdcj.github.io/PYield/quickstart/): first steps with the library.
- [Full documentation](https://crdcj.github.io/PYield/): concepts and module reference.
- [API map](https://crdcj.github.io/PYield/api-map/): public namespaces and functions.
- [Development and publishing](https://crdcj.github.io/PYield/desenvolvimento/): environment, checks, builds, and PyPI.
- [Build and publish the documentation](docs/desenvolvimento.md#documentação): generation, local preview, and GitHub Pages.
- [Colab notebook](https://colab.research.google.com/github/crdcj/PYield/blob/main/examples/pyield_quickstart.ipynb): interactive exploration.
- [Package on PyPI](https://pypi.org/project/pyield/).

## API overview

| Component | Type | Purpose |
|---|---|---|
| `yd.du` | module | Business days and Brazilian calendar |
| `yd.Interpolador` | class | Scalar and Polars-pipeline rate interpolation |
| `yd.interpolar`, `yd.forward`, `yd.forwards` | functions | Curve interpolation and forward rates |
| `yd.futuro` | module | B3 futures contracts and historical/intraday data |
| `yd.di1` | module | DI1 curve and interpolation |
| `yd.tpf` | module | Treasury rates, maturities, auctions, benchmarks, and trades |
| `yd.rmd(aba)` | function | Tesouro Nacional monthly debt report |
| `yd.lft`, `yd.ltn`, `yd.ntnb`, `yd.ntnb1`, `yd.ntnbp`, `yd.ntnc`, `yd.ntnf` | modules | Treasury-bond pricing and analytics |
| `yd.vna` | module | Updated nominal values for treasury bonds |
| `yd.copom`, `yd.selic`, `yd.cpm` | modules | COPOM calendar, Selic, and digital options |
| `yd.compromissadas` | function | BCB repo-operation auctions |
| `yd.ipca` | module | Historical and projected inflation data |
| `yd.igpm` | module | Monthly IGP-M rates and index numbers |
| `yd.ptax`, `yd.ptax_serie`, `yd.di_over` | functions | Exchange-rate and DI indicators |
| `yd.b3` | module | B3 bulletins and trading-session calendar |
| `yd.hoje`, `yd.agora` | functions | Current date and time in Brazil |

See the [complete API map](https://crdcj.github.io/PYield/api-map/) for detailed
documentation and public signatures. Public namespaces follow the semantic
autonomy of the concept users need to understand, rather than only its data
source or thematic relationship to another domain. Concepts such as `yd.ltn`,
`yd.ntnb`, `yd.vna`, and `yd.rmd` therefore live at the root, while
implementations may remain grouped internally by domain.

## What's new in version 0.61.0

This section covers only changes from `0.60.0` to `0.61.0`, which neither
remove nor change existing APIs.

- B3 trading-session calendar in `yd.b3`: `eh_pregao(data)`,
  `pregao_anterior(data)`, `proximo_pregao(data)`, and
  `ultimo_pregao(data=None)`. There is a session on every Brazilian business
  day except December 24 and 31. Non-recurring exceptional closures are not
  modeled.
- `yd.futuro.dv01_expr(coluna_dias_uteis, coluna_taxa, coluna_preco)` is now
  public and returns a `pl.Expr` with the DV01 of futures contracts.

When upgrading from earlier versions, see the
[0.60.0 migration notes (Portuguese)](https://crdcj.github.io/PYield/release-0.60.0/)
and the [GitHub releases](https://github.com/crdcj/PYield/releases).

## Project

- [Source code](https://github.com/crdcj/PYield)
- [Issues](https://github.com/crdcj/PYield/issues)
- [MIT License](LICENSE)
