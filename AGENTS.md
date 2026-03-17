# AGENTS.md

This file provides guidance to AI coding agents when working with code in this repository.

## Project Overview

PYield is a Python library for Brazilian fixed income analysis (requires Python ≥ 3.12). It fetches and processes data from ANBIMA, BCB (Central Bank), IBGE, and B3 (Brazilian stock exchange). All public functions return Polars DataFrames/Series.

## Princípio Fundamental: Redução de Complexidade

O objetivo principal ao trabalhar neste repositório é **reduzir a complexidade do código**. Toda alteração deve, por padrão, simplificar — nunca adicionar ramificações, abstrações ou lógica desnecessária. Só aumente a complexidade quando for estritamente necessário e com justificativa clara do solicitante.

## Build & Development Commands

```bash
# Install dependencies (uses uv package manager)
uv sync

# Run all tests (tests/ + doctests in pyield/)
# pyproject.toml already configures testpaths and --doctest-modules
pytest

# Run only doctests in a single module
pytest pyield/bday/core.py --doctest-modules

# Run a single test file
pytest tests/bday/test_bday.py

# Run a specific test
pytest tests/bday/test_bday.py::test_count_with_strings1

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

- **`bday`** — Business day calendar (Brazilian holidays built-in). Core functions: `count`, `offset`, `generate`, `is_business_day`, `last_business_day`. Polars expression variants: `count_expr`, `offset_expr`, `is_business_day_expr`.
- **`anbima`** — ANBIMA data endpoints (treasury bond pricing, yield curves). Functions: `tpf`, `tpf_maturities`, `fetch_tpf`, `last_ettj`, `intraday_ettj`, `last_ima`, `imaq`, `tpf_difusao`.
- **`bc`** — BCB indicators. Functions: `selic_over`, `selic_over_series`, `selic_target`, `selic_target_series`, `di_over`, `di_over_series`, `ptax`, `ptax_series`, `repos`, `vna_lft`, `auctions`, `tpf_monthly_trades`, `tpf_intraday_trades`. Submodule: `copom`.
- **`b3`** — B3 market data. Functions: `futures`, `di_over`, `fetch_price_report`, `read_price_report`, `fetch_intraday_derivatives`. Submodule: `di1`.
- **`tn`** — Treasury bond modules: `ltn`, `ntnb`, `ntnf`, `ntnc`, `lft`, `pre`, `ntnbprinc`, `ntnb1`. Most have `data()`, `maturities()`, `price()`; `ntnb`, `ntnc`, `lft`, `ntnb1` also have `quotation()`. `pre` only has `spot_rates()` and `di_spreads()`. Also exposes: `tn.auction`, `tn.benchmarks`, `tn.di_spreads`.
- **`ipca`** — Inflation data. Functions: `indexes`, `last_indexes`, `rates`, `last_rates`, `projected_rate`.
- **`selic`** — COPOM-related analytics. Submodules: `cpm` (raw B3 COPOM Digital Option data), `probabilities` (implied COPOM meeting probabilities).

Top-level functions also exported from `pyield`:
- `forwards`, `forward` — Forward rate calculations from `fwd.py`.
- `rmd` — Treasury monthly debt report (Relatório Mensal da Dívida) from `rmd.py`.
- `Interpolator` — Rate interpolation class from `interpolator.py`.
- `today`, `now` — Brazil timezone date/time from `clock.py`.
- `copom_options` — Alias for `selic.cpm.data`.

### Key Cross-Cutting Components

- **`_internal/types.py`** — Type aliases `DateLike` and `ArrayLike`; `any_is_empty()` for null/empty detection; `any_is_collection()` for array-like detection.
- **`_internal/converters.py`** — `converter_datas()` normalizes various date inputs to `datetime.date` or `pl.Series[Date]`. `converter_datas_expr()` for Polars expression pipelines.
- **`interpolator.py`** — `Interpolator` class for rate interpolation (linear or flat_forward method, 252 bday/year convention).
- **`_internal/data_cache.py`** — GitHub-hosted parquet data cache with daily TTL using `lru_cache` (date-key trick for auto-invalidation).
- **`_internal/retry.py`** — Tenacity-based retry decorator (`retry_padrao`) for network requests (retries on 429, 5xx, timeouts).
- **`clock.py`** — `today()` and `now()` return Brazil timezone (America/Sao_Paulo) dates/times.

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
- Docstring line width must respect the project's `line-length = 88` (configured in `pyproject.toml`). This includes the indentation — e.g., a docstring inside a function has 4 spaces of indent, leaving 84 usable characters.
- Public functions use Google-style sections: `Args:`, `Returns:`, `Output Columns:`, `Notes:`, `Examples:`.
- `Output Columns:` lists every column with tipo Polars e descrição (ex: `* SettlementDate (Date): data de liquidação.`).
- Doctests (section `Examples:`) use real data and are validated by `pytest --doctest-modules`.
- Renderização Markdown (MkDocs/mkdocstrings): em listas livres dentro do texto (ex.: `Onde:`), evitar linha em branco entre o título e os itens e iniciar os itens imediatamente abaixo.
  Exemplo recomendado:
  `Onde:`
  `- item 1`
  `- item 2`
- Após mudanças em docstrings com listas/fórmulas, validar visualmente com `mkdocs serve` e confirmar o HTML gerado (evitar renderização como bloco de código).

## Polars Conventions

- Em `with_columns`, preferir sintaxe de keyword `col=expr` em vez de `expr.alias("col")`.
- Retornos com encadeamento Polars: usar um único `return (...)` com quebras de linha entre métodos, sem variável intermediária.

## Logging Conventions

- Não usar `warning` para validações de entrada esperadas (ex.: `None`, vazio, combinação inválida já prevista pelo contrato).
- Nesses casos, retornar o valor de contrato (`None`, `NaN`, `Series/DataFrame` vazio) ou lançar `ValueError` quando apropriado.
- Reservar `warning`/`error` para anomalias operacionais reais (falha de rede, fonte indisponível, schema inesperado, erro de parsing fora do contrato, etc.).

## Testing

Tests are in `tests/` and doctests are embedded in docstrings. Run `pytest` to execute both (configured in `pyproject.toml` via `testpaths` and `addopts = "--doctest-modules"`).

### Doctest Configuration (conftest.py)

The root `conftest.py` configures the doctest environment:
- **Namespace injection:** `yd` (pyield) and `pl` (polars) are available in all doctests via `doctest_namespace` fixture.
- **Polars display:** `pl.Config.set_tbl_width_chars(150)` ensures consistent table output across environments.
- **Option flags:** `ELLIPSIS` and `NORMALIZE_WHITESPACE` are enabled globally.

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
