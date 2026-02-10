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

- **`_internal/types.py`** - Type aliases `DateLike` and `ArrayLike`; `any_is_empty()` for null/empty detection
- **`_internal/converters.py`** - `converter_datas()` normalizes various date inputs to `datetime.date` or `pl.Series[Date]`
- **`interpolator.py`** - `Interpolator` class for rate interpolation (linear or flat_forward method, 252 bday/year convention)
- **`_internal/data_cache.py`** - GitHub-hosted parquet data cache with daily TTL using `lru_cache`
- **`_internal/retry.py`** - Tenacity-based retry decorator for network requests (retries on 429, 5xx, timeouts)
- **`clock.py`** - `today()` and `now()` return Brazil timezone (America/Sao_Paulo) dates/times

### Date Handling Conventions

- Accepted string formats: `DD-MM-YYYY`, `DD/MM/YYYY`, `YYYY-MM-DD`
- Scalar dates normalize to `datetime.date`; collections become `pl.Series` with dtype `Date`
- String parsing is element-wise (row-wise) with fallback across the accepted formats
- Invalid strings are converted to `null` (or `None` for scalar outputs)
- Nullable inputs (`None`, `NaN`, empty collections) short-circuit: scalar functions return `None` or `nan`, vectorized functions return empty DataFrame/Series

### Data Flow Pattern

Most data-fetching functions follow this pattern:
1. Accept `DateLike` reference date parameter
2. Convert dates using `converter_datas()`
3. Fetch from external API (with retry logic) or cached parquet
4. Return Polars DataFrame with standardized column names

## Naming Conventions

- **Fronteira da API pública:** Considere público o que está documentado e/ou exportado no namespace de topo (`pyield/__init__.py`). Módulos não exportados no topo são internos, mesmo que importáveis por caminho direto.
- **API pública (inglês):** Nomes de funções públicas, parâmetros, nomes de colunas em DataFrames e classes exportadas permanecem em inglês.
- **Código interno (português):** Variáveis locais, constantes de módulo, mensagens de log e mensagens de exceção devem ser em português.
- **Nomes em módulos internos:** Para módulos de uso interno compartilhado, nomes de função podem permanecer sem prefixo `_` quando isso melhora legibilidade. Use prefixo `_` para helpers locais/privados dentro do módulo.
- **Exceção para módulos utilitários base:** Módulos transversais e fundacionais de uso interno (ex.: `_internal/types.py`, `_internal/retry.py`) podem manter identificadores técnicos em inglês (`is_collection`, `retry_state`, etc.) para preservar legibilidade, reduzir churn e evitar renomeações sem ganho funcional.
- **Exceção para módulos internos com classe de serviço:** Se um módulo não é exposto pela API e é usado apenas internamente por outro módulo, é comum manter **um método principal sem `_`** dentro da classe para sinalizar o ponto de entrada interno. Os demais helpers seguem com `_`. Esse método principal deve permanecer em português e o módulo **não deve** ser exportado em `__init__.py`.
- **Exceção para módulos internos com função principal:** Se um módulo não é exposto pela API e é usado apenas internamente por outro módulo, pode manter **uma função principal sem `_`** como ponto de entrada interno. As demais funções seguem com `_`. A função principal deve ser em português e o módulo **não deve** ser exportado em `__init__.py`.

## Docstring Conventions

- All docstrings must be written in **Portuguese** (both public and internal functions).
- Public functions use Google-style sections: `Args:`, `Returns:`, `Output Columns:`, `Notes:`, `Examples:`.
- `Output Columns:` lists every column with tipo Polars e descrição (ex: `* SettlementDate (Date): data de liquidação.`).
- Doctests (section `Examples:`) use real data and are validated by `pytest --doctest-modules`.

## Complexity

- Always prioritize reducing code complexity. Do not increase complexity unless explicitly requested.

## Testing

Tests are in `tests/` and doctests are embedded in docstrings. Run `pytest pyield --doctest-modules` to execute both.

### Test Pattern for Data-Fetching Modules

Modules that fetch external data (e.g., `bc/repo.py`, `bc/trades_monthly.py`) use local reference data to test without network access:

1. **Reference data** — A pair of files in `tests/<module>/data/`: the raw input (CSV or ZIP) and the expected output (Parquet).
2. **Pipeline test** — Processes the local raw input through the internal processing functions and asserts `result.equals(expected_parquet)`.
3. **Public function test** — Patches the network fetch function to return local raw data, calls the public function, and asserts equality with the reference Parquet.

#### Padrão recomendado para ETL mais complexos (HTML/CSV/JSON)

Para pipelines ETL com múltiplas etapas e dependência de rede:

1. **Substituir apenas a camada de rede** — Use `pytest` com `monkeypatch` para sobrescrever a função interna de fetch (ex.: `_obter_csv`, `_buscar_conteudo_url`) e retornar o arquivo bruto local.
2. **Executar o fluxo público completo** — Chame a função pública com a opção de buscar na fonte (`fetch_from_source=True`) para exercitar o mesmo pipeline real.
3. **Comparar com Parquet de referência** — Valide o resultado final com `DataFrame.equals`, sem repetir etapas do pipeline no teste.

Isso reduz complexidade do teste e mantém a cobertura do fluxo real sem rede.

### Quando doctests são suficientes

Módulos com pipeline trivial que retornam valores escalares simples (float, str) sem transformações complexas de DataFrame. Os doctests já validam o comportamento real e servem como documentação. Exemplos: `di_over.py`, `tn/ltn.py`, `fwd.py`, `interpolator.py`.

### Quando criar test files separados

Módulos com pipelines ETL multi-etapa, transformações complexas de DataFrame (5+ colunas), processamento de arquivos binários (ZIP), ou quando é necessário testar múltiplos cenários de edge case que não cabem em doctests.
