# Futuro

Contratos futuros negociados na B3.

## `datas_disponiveis(contrato)`

Retorna as datas disponíveis no dataset histórico cacheado.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `contrato` | `str` | Contrato futuro (ex.: `DI1`, `DOL`). | *required* |

Returns:

| Type | Description |
| --- | --- |
| `Series` | Series ordenada de datas para as quais há dados históricos. |

Examples:

```pycon
>>> yd.futuro.datas_disponiveis("DI1").head(3)
shape: (3,)
Series: 'data_referencia' [date]
[
    2018-01-02
    2018-01-03
    2018-01-04
]
```

## `enriquecer(df, contrato)`

Enriquece DataFrame bruto do Price Report (PR) da B3.

Aceita um DataFrame com colunas no schema original da B3 (ex.: `TradDt`, `TckrSymb`). Filtra pelo contrato informado, adiciona data de vencimento, dias úteis/corridos e colunas derivadas (dv01, taxa\_forward) conforme o contrato.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `df` | `DataFrame` | DataFrame com dados brutos do PR da B3. | *required* |
| `contrato` | `str` | Contrato futuro (ex.: `DI1`, `DOL`). | *required* |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | DataFrame Polars enriquecido e ordenado. |

## `vencimento(codigo, contrato)`

Calcula a data de vencimento de contratos futuros da B3.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `codigo` | `str \| ArrayLike \| None` | Código de negociação ou coleção de códigos. | *required* |
| `contrato` | `str` | Contrato futuro na B3 (ex.: `DI1` ou `DAP`). | *required* |

Returns:

| Type | Description |
| --- | --- |
| `date \| Series \| None` | Data de vencimento para código escalar, ou `pl.Series` para coleção. |

Examples:

```pycon
>>> yd.futuro.vencimento("DI1F25", "DI1")
datetime.date(2025, 1, 2)
```

```pycon
>>> yd.futuro.vencimento(["DI1F25", "di1g25", "DI1E27"], "DI1")
shape: (3,)
Series: 'vencimento' [date]
[
    2025-01-02
    2025-02-03
    null
]
```

## `vencimento_expr(coluna_codigo, contrato)`

Cria expressão Polars para a data de vencimento de futuros da B3.

Assume códigos de negociação no formato padrão de futuros da B3 (ex.: `DI1F25`). A caixa dos caracteres é ignorada e o vencimento é ajustado para dia útil.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `coluna_codigo` | `str` | Nome da coluna com o código de negociação. | *required* |
| `contrato` | `str` | Contrato futuro na B3 (ex.: `DI1` ou `DAP`). | *required* |

Returns:

| Type | Description |
| --- | --- |
| `Expr` | Uma `pl.Expr` que resulta em Date. |

Examples:

```pycon
>>> df = pl.DataFrame({"codigo_negociacao": ["DI1F25", "di1g25", "DI1E27"]})
>>> df.select(
...     yd.futuro.vencimento_expr("codigo_negociacao", "DI1").alias(
...         "vencimento"
...     )
... )
shape: (3, 1)
┌────────────┐
│ vencimento │
│ ---        │
│ date       │
╞════════════╡
│ 2025-01-02 │
│ 2025-02-03 │
│ null       │
└────────────┘
```

Contratos DAP vencem no dia 15:

```pycon
>>> df = pl.DataFrame({"codigo_negociacao": ["DAPF25"]})
>>> df.select(
...     yd.futuro.vencimento_expr("codigo_negociacao", "DAP").alias(
...         "vencimento"
...     )
... )
shape: (1, 1)
┌────────────┐
│ vencimento │
│ ---        │
│ date       │
╞════════════╡
│ 2025-01-15 │
└────────────┘
```
