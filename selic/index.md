# Selic

Este namespace reúne os indicadores diretamente relacionados à taxa Selic. Os contratos CPM e suas probabilidades implícitas estão documentados separadamente em [CPM](<https://crdcj.github.io/PYield/cpm/index.md>).

`over` e `over_serie` retornam a taxa anualizada em base 252 por padrão. Com `anualizada=False`, consultam a taxa ao dia útil publicada pelo Banco Central. A frequência das observações é diária nas duas modalidades; `meta` e `meta_serie` retornam a meta anual definida pelo Copom.

As consultas escalares retornam `Decimal`; as séries têm colunas `data` (`pl.Date`) e `taxa` (`pl.Decimal(18, 10)`). Valores percentuais são convertidos diretamente do texto para unidade decimal, sem passagem por `float`. Escalares sem dados retornam `Decimal("NaN")`; séries sem dados retornam DataFrame vazio com o mesmo esquema.

```python
import pyield as yd

taxa_anual = yd.selic.over("28-02-2025")
taxa_diaria = yd.selic.over("28-02-2025", anualizada=False)
serie_diaria = yd.selic.over_serie(ultimos=5, anualizada=False)
meta = yd.selic.meta("28-02-2025")
```

As docstrings abaixo identificam as séries de origem. Veja as [notas de migração da 0.62.0](<https://crdcj.github.io/PYield/release-0.62.0/#indicadores-sgs-com-decimal>) para converter explicitamente os resultados para ponto flutuante.

selic — Dados e análises relacionados à taxa Selic e política monetária.

### Submódulos

Este namespace reúne os indicadores diretamente relacionados à taxa Selic. O produto CPM e suas análises ficam disponíveis em `pyield.cpm`.

## `over(data, *, anualizada=True)`

Taxa SELIC Over para uma data específica.

Fonte: SGS do Banco Central, série 1178 para a taxa anualizada em base 252 ou série 11 para a taxa diária publicada pela fonte.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data` | `DateLike` | Data da consulta. | *required* |
| `anualizada` | `bool` | Se `True`, retorna a taxa ao ano, em base 252 dias úteis. Se `False`, retorna a taxa ao dia útil publicada pela fonte. | `True` |

Returns:

| Name | Type | Description |
| --- | --- | --- |
| `Decimal` | `Decimal` | Taxa SELIC Over com dez casas decimais, ao ano ou ao dia útil |
|  | `Decimal` | conforme `anualizada`, ou `Decimal('NaN')` se não disponível. |

Examples:

```pycon
>>> import pyield as yd
>>> taxa = yd.selic.over("31-05-2024")  # decimal (0.104 = 10,4% a.a.)
>>> f"{taxa:.1%}"
'10.4%'
```

## `over_serie(inicio=None, fim=None, *, ultimos=None, anualizada=True)`

Taxa SELIC Over anualizada ou diária via SGS do Banco Central.

Taxa de juros média diária praticada no mercado interbancário, com títulos públicos como garantia. Consulta a série SGS 1178 para a taxa anualizada em base 252 ou a série SGS 11 para a taxa diária.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `inicio` | `DateLike \| None` | Data inicial. | `None` |
| `fim` | `DateLike \| None` | Data final. Se `None`, usa a data mais recente. | `None` |
| `ultimos` | `int \| None` | Número de registros mais recentes a retornar. Mutuamente exclusivo com `inicio`/`fim`. | `None` |
| `anualizada` | `bool` | Se `True`, retorna a taxa ao ano, em base 252 dias úteis. Se `False`, retorna a taxa ao dia útil publicada pela fonte. | `True` |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | DataFrame com colunas data e taxa (decimal), ou DataFrame vazio. |

**Output Columns**

data (pl.Date): Data de referência da observação. taxa (pl.Decimal(18, 10)): Taxa em decimal, ao ano se `anualizada=True` ou ao dia útil se `anualizada=False`.

**Notes**

A frequência das observações é diária nas duas modalidades. Os valores percentuais da série escolhida são convertidos diretamente do texto para Decimal e divididos por 100, sem passagem por float e sem recalcular uma modalidade a partir da outra. A escala de dez casas preserva até oito casas decimais do percentual publicado.

Examples:

```pycon
>>> import pyield as yd
>>> # Copom de 29-01-2025: over subiu de 12,15% para 13,15% a.a.
>>> yd.selic.over_serie("28-01-2025", "31-01-2025")
shape: (4, 2)
┌────────────┬────────────────┐
│ data       ┆ taxa           │
│ ---        ┆ ---            │
│ date       ┆ decimal[18,10] │
╞════════════╪════════════════╡
│ 2025-01-28 ┆ 0.1215000000   │
│ 2025-01-29 ┆ 0.1215000000   │
│ 2025-01-30 ┆ 0.1315000000   │
│ 2025-01-31 ┆ 0.1315000000   │
└────────────┴────────────────┘
```

## `meta(data)`

Taxa SELIC Meta para uma data específica.

Fonte: SGS do Banco Central, série 432.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data` | `DateLike` | Data da consulta. | *required* |

Returns:

| Name | Type | Description |
| --- | --- | --- |
| `Decimal` | `Decimal` | Taxa SELIC Meta em formato decimal com dez casas decimais ou |
|  | `Decimal` | `Decimal('NaN')` se não disponível, inclusive para datas anteriores |
|  | `Decimal` | a 05/03/1999. |

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
- taxa (pl.Decimal(18, 10)): taxa Selic Meta anual em formato decimal.

**Notes**

Consultas iniciadas antes de 05/03/1999 consideram automaticamente essa data como início da série. Intervalos inteiramente anteriores retornam um DataFrame vazio. Os percentuais são convertidos diretamente do texto para Decimal e divididos por 100, sem passagem por float, com dez casas decimais.

Examples:

```pycon
>>> import pyield as yd
>>> # Copom de 29-01-2025: meta subiu de 12,25% para 13,25% a.a.
>>> yd.selic.meta_serie("28-01-2025", "31-01-2025")
shape: (4, 2)
┌────────────┬────────────────┐
│ data       ┆ taxa           │
│ ---        ┆ ---            │
│ date       ┆ decimal[18,10] │
╞════════════╪════════════════╡
│ 2025-01-28 ┆ 0.1225000000   │
│ 2025-01-29 ┆ 0.1225000000   │
│ 2025-01-30 ┆ 0.1325000000   │
│ 2025-01-31 ┆ 0.1325000000   │
└────────────┴────────────────┘
```
