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
| `yd.lft`, `yd.ltn`, `yd.ntnb`, `yd.ntnb1`, `yd.ntnbp`, `yd.ntnc`, `yd.ntnf` | modules | Treasury-bond pricing and analytics |
| `yd.vna` | module | Updated nominal values for treasury bonds |
| `yd.copom`, `yd.selic`, `yd.cpm` | modules | COPOM calendar, Selic, and digital options |
| `yd.compromissadas` | function | BCB repo-operation auctions |
| `yd.ipca` | module | Historical and projected inflation data |
| `yd.ptax`, `yd.ptax_serie`, `yd.di_over` | functions | Exchange-rate and DI indicators |
| `yd.hoje`, `yd.agora` | functions | Current date and time in Brazil |

See the [complete API map](https://crdcj.github.io/PYield/api-map/) for detailed
documentation and public signatures.

## API compatibility

See the [GitHub releases](https://github.com/crdcj/PYield/releases) for the
complete version history and migration notes. The current public organization
places treasury-bond modules at the package root, while implementations remain
organized internally under `pyield/tpf/titulos/`.

| Before | Now |
|---|---|
| `yd.tpf.ntnb` | `yd.ntnb` or `from pyield import ntnb` |
| `yd.tpf.rmd(aba)` | `yd.rmd(aba)` or `from pyield import rmd` |
| `pyield.tpf.vna.calcular_vna(...)` | `yd.vna.calcular_vna(...)` |
| `yd.ntnb.vna(data)` | `yd.vna.valor("NTN-B", data)` |
| `yd.ntnb.vnas()` | `yd.vna.historico("NTN-B")` |
| `vna_projetado(...)` in bond modules | `yd.vna.projetado(titulo, data, vna_base, inflacao)` |
| `vigencia(data)` in bond modules | `yd.vna.vigencia(titulo, data)` |

## Project

- [Source code](https://github.com/crdcj/PYield)
- [Issues](https://github.com/crdcj/PYield/issues)
- [MIT License](LICENSE)
