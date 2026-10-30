# Selic

Este namespace reúne os indicadores diretamente relacionados à taxa Selic. Os contratos CPM e suas probabilidades implícitas estão documentados separadamente em [CPM](<https://crdcj.github.io/PYield/cpm/index.md>).

selic — Dados e análises relacionados à taxa Selic e política monetária.

### Submódulos

Este namespace reúne os indicadores diretamente relacionados à taxa Selic. O produto CPM e suas análises ficam disponíveis em `pyield.cpm`.

## `over(data)`

Taxa SELIC Over para uma data específica.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data` | `DateLike` | Data da consulta. | *required* |

Returns:

| Type | Description |
| --- | --- |
| `float` | Taxa SELIC Over em decimal ou `nan` se não disponível. |

Examples:

```pycon
>>> import pyield as yd
>>> taxa = yd.selic.over("31-05-2024")  # decimal (0.104 = 10,4% a.a.)
>>> f"{taxa:.1%}"
'10.4%'
```

## `over_serie(inicio=None, fim=None, *, ultimos=None)`

Taxa SELIC Over (série SGS 1178).

Taxa de juros média diária praticada no mercado interbancário, com títulos públicos como garantia.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `inicio` | `DateLike \| None` | Data inicial. | `None` |
| `fim` | `DateLike \| None` | Data final. Se `None`, usa a data mais recente. | `None` |
| `ultimos` | `int \| None` | Número de registros mais recentes a retornar. Mutuamente exclusivo com `inicio`/`fim`. | `None` |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | DataFrame com colunas data e taxa (decimal), ou DataFrame vazio. |

Examples:

```pycon
>>> import pyield as yd
>>> # Copom de 29-01-2025: over subiu de 12,15% para 13,15% a.a.
>>> yd.selic.over_serie("28-01-2025", "31-01-2025")
shape: (4, 2)
┌────────────┬────────┐
│ data       ┆ taxa   │
│ ---        ┆ ---    │
│ date       ┆ f64    │
╞════════════╪════════╡
│ 2025-01-28 ┆ 0.1215 │
│ 2025-01-29 ┆ 0.1215 │
│ 2025-01-30 ┆ 0.1315 │
│ 2025-01-31 ┆ 0.1315 │
└────────────┴────────┘
```

## `meta(data)`

Taxa SELIC Meta para uma data específica.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data` | `DateLike` | Data da consulta. | *required* |

Returns:

| Type | Description |
| --- | --- |
| `float` | Taxa SELIC Meta em decimal ou `nan` se não disponível, inclusive para |
| `float` | datas anteriores a 05/03/1999. |

Examples:

```pycon
>>> import pyield as yd
>>> taxa = yd.selic.meta("31-05-2024")  # decimal (0.105 = 10,5% a.a.)
>>> f"{taxa:.1%}"
'10.5%'
```

## `meta_serie(inicio=None, fim=None, *, ultimos=None)`

Taxa SELIC Meta (série SGS 432).

Taxa de juros oficial definida pelo COPOM. Fonte: Banco Central do Brasil (BCB), série SGS 432, disponível desde 05/03/1999.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `inicio` | `DateLike \| None` | Data inicial. | `None` |
| `fim` | `DateLike \| None` | Data final. Se `None`, usa a data mais recente. | `None` |
| `ultimos` | `int \| None` | Número de registros mais recentes a retornar. Mutuamente exclusivo com `inicio`/`fim`. | `None` |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | DataFrame com colunas data e taxa (decimal), ou DataFrame vazio. |

**Output Columns**

- data (Date): data de referência da taxa.
- taxa (Float64): taxa Selic Meta anual em formato decimal.

**Notes**

Consultas iniciadas antes de 05/03/1999 consideram automaticamente essa data como início da série. Intervalos inteiramente anteriores retornam um DataFrame vazio.

Examples:

```pycon
>>> import pyield as yd
>>> # Copom de 29-01-2025: meta subiu de 12,25% para 13,25% a.a.
>>> yd.selic.meta_serie("28-01-2025", "31-01-2025")
shape: (4, 2)
┌────────────┬────────┐
│ data       ┆ taxa   │
│ ---        ┆ ---    │
│ date       ┆ f64    │
╞════════════╪════════╡
│ 2025-01-28 ┆ 0.1225 │
│ 2025-01-29 ┆ 0.1225 │
│ 2025-01-30 ┆ 0.1325 │
│ 2025-01-31 ┆ 0.1325 │
└────────────┴────────┘
```
