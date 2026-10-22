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
| `yd.hoje`, `yd.agora` | functions | Current date and time in Brazil |

See the [complete API map](https://crdcj.github.io/PYield/api-map/) for detailed
documentation and public signatures. Public namespaces follow the semantic
autonomy of the concept users need to understand, rather than only its data
source or thematic relationship to another domain. Concepts such as `yd.ltn`,
`yd.ntnb`, `yd.vna`, and `yd.rmd` therefore live at the root, while
implementations may remain grouped internally by domain.

## What's new in version 0.60.0

Summary of the main changes since `0.59.0`. See also the
[migration notes (Portuguese)](https://crdcj.github.io/PYield/release-0.60.0/).

### Decimal contracts

VNA histories and calculations and inflation indicators now preserve decimal
precision. Review operations that mix these results with `float`: use
`Decimal("0.0045")` for exact constants and `.is_nan()` to check a scalar
`Decimal("NaN")` result. Convert explicitly to `float` only when the application
can accept the loss of precision.

| Operation | Return type in 0.60.0 |
|---|---|
| `vna.valor`, `vna.projetado`, `vna.calcular_vna` | `Decimal` |
| `vna` column in `vna.historico` and `vna.ultimo` | `Decimal(18, 6)` |
| `ipca.indice` / `indice` column in `ipca.indice_serie` | `Decimal` / `Decimal(18, 2)` |
| `ipca.taxa` / `taxa` column in `ipca.taxa_serie` | `Decimal` / `Decimal(18, 4)` |
| `ipca.taxa_projetada().valor_projetado` | `Decimal` with four decimal places |
| `igpm.taxa` / `taxa` column in `igpm.taxa_serie` | `Decimal` / `Decimal(18, 4)` |
| `igpm.indice` / `indice` column in `igpm.indice_serie` | `Decimal` / `Decimal(18, 10)` |

`vna.calcular_vna` also returns `Decimal("NaN")` instead of `float("nan")`
when data is unavailable. VNA calculations use 28 digits of precision and apply
the methodology's truncation and rounding rules; intermediate results may
differ from those previously obtained with `float`.

Bond analytical rates and DV01 still return `float`, as introduced in `0.59.0`;
`pu` and `cotacao` continue to use `Decimal`.

### IPCA series

Historical IPCA queries follow the same pattern as Selic and PTAX series:
`taxa(data)` and `indice(data)` return a scalar; `taxa_serie` and `indice_serie`
return series over a date range or for the most recent months.

To migrate, replace `ipca.taxas(inicio, fim)` with
`ipca.taxa_serie(inicio, fim)` and `ipca.indices(inicio, fim)` with
`ipca.indice_serie(inicio, fim)`. Replace `ipca.taxas_ultimas(n)` with
`ipca.taxa_serie(ultimos=n)` and `ipca.indices_ultimos(n)` with
`ipca.indice_serie(ultimos=n)`. The old names have been removed. Columns and units are preserved;
numeric types change as described above.

The index number now uses `Decimal`: `ipca.indice(data)` returns `Decimal`
(or `Decimal("NaN")` when unavailable), and the `indice` column returned by
`ipca.indice_serie(...)` uses `Decimal(18, 2)`. Values are converted directly
from IBGE text without passing through `float`.

Rates also use `Decimal`: `ipca.taxa(data)` returns `Decimal` (or
`Decimal("NaN")`), and the `taxa` column returned by `ipca.taxa_serie(...)`
uses `Decimal(18, 4)`. The `valor_projetado` field returned by
`ipca.taxa_projetada()` now uses `Decimal` with four decimal places. The unit
remains decimal: 0.45% corresponds to `Decimal("0.0045")`.

For series, `fim=None` uses the current date in Brazil. Provide `inicio` or
`ultimos`; when supplied, `ultimos` takes precedence over dates.

### VNA context and inflation calendars

- `yd.vna.contexto(...)` gathers the parameters for an NTN-B or NTN-C validity
  period: published VNAs, reference month, inflation, and missing inputs. It
  requires `vencimento` for NTN-C and accepts an explicit projection when
  realized inflation is unavailable. Inflation in `contexto` uses decimal
  units; multiply it by 100 before passing it to the percentage parameter
  `inflacao` in `vna.projetado`.
- `yd.ipca.calendario(ano, atualizar=False)` queries IBGE's scheduled release
  dates by reference year, including December released in the following year.
- `yd.igpm.calendario(ano, atualizar=False)` queries FGV IBRE for monthly
  releases, excluding preliminary ten-day releases. Calendars do not confirm
  that an index has already been published.

### Other changes

- New `yd.igpm` module with `taxa` and `taxa_serie` for monthly IGP-M rates
  from SGS/BCB. `taxa` returns `Decimal` (or `Decimal("NaN")` when unavailable),
  and the `taxa` column returned by `taxa_serie` uses `Decimal(18, 4)`.
  The unit is decimal: 0.27% corresponds to `Decimal("0.0027")`.
  `indice` and `indice_serie` provide FGV index numbers via Ipeadata
  (series `IGP12_IGPM12`), as `Decimal` and a `Decimal(18, 10)` column.
  Ten decimal places preserve the history before 1994. Series queries accept
  a date range or `ultimos`, with a `periodo` column in `YYYYMM` format.
- CPM now exposes `yd.cpm.contratos(data)` and
  `yd.cpm.probabilidades(data, tipo_opcao="call")`. The latter returns all
  available meetings; filter the DataFrame to select a meeting.
- BCB auctions move from `yd.bc.leiloes` to `yd.tpf.leiloes_bcb`, with column
  names aligned with `yd.tpf.leiloes`.
- For secondary-market data, `yd.tpf.secundario.ler(fonte)` replaces `ler_zip`
  and `zip_para_silver`: it accepts a ZIP path or bytes and includes
  `financeiro` in the output. `nome_arquivo_mensal` is no longer public.
- `yd.ntnf.premio` has been removed. Use `yd.tpf.premios_pre(data)` and filter
  `titulo == "NTN-F"` for the gross spread.
- `yd.tpf.taxas` preserves columns and types when no data is available. In
  Tesouro auctions, a minimum PU reported as zero becomes null, and the
  offered financial amount uses the calculated average PU when needed.
- `yd.rmd` now propagates download and processing errors instead of returning
  an empty DataFrame in those cases.

See the [GitHub releases](https://github.com/crdcj/PYield/releases) for the
complete history and migration notes for earlier versions.

## Project

- [Source code](https://github.com/crdcj/PYield)
- [Issues](https://github.com/crdcj/PYield/issues)
- [MIT License](LICENSE)
