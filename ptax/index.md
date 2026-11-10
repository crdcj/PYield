# PTAX

`yd.ptax` e `yd.ptax_serie` consultam a cotação de venda do dólar no Banco Central, em R\\(/US\\). Os valores são convertidos diretamente do texto da fonte para `Decimal`, com quatro casas decimais.

A consulta escalar retorna `Decimal("NaN")` se não houver cotação. A série retorna um DataFrame com `data` (`pl.Date`) e `cotacao` (`pl.Decimal(18, 4)`), preservando o esquema quando vazio.

```python
import pyield as yd

cotacao = yd.ptax("22-04-2025")
historico = yd.ptax_serie("22-04-2025", "25-04-2025")
```

Veja as [notas de migração da 0.62.0](<https://crdcj.github.io/PYield/release-0.62.0/#indicadores-sgs-com-decimal>) para converter explicitamente os resultados para ponto flutuante.

Cotação PTAX de venda para uma data específica.

Retorna a PTAX de venda, taxa de câmbio oficial do dólar no Brasil, via SGS do Banco Central (série 1).

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data` | `DateLike` | Data desejada. | *required* |

Returns:

| Name | Type | Description |
| --- | --- | --- |
| `Decimal` | `Decimal` | Cotação PTAX de venda em R\\(/US\\) com quatro casas decimais, ou |
|  | `Decimal` | `Decimal('NaN')` se não houver cotação (feriado, fim de semana ou |
|  | `Decimal` | data futura). |

Examples:

```pycon
>>> import pyield as yd
>>> yd.ptax("22-04-2025")
Decimal('5.7496')
```

```pycon
>>> yd.ptax("20-04-2025")
Decimal('NaN')
```

Cotação PTAX de venda do dólar (série SGS 1).

Fonte: Banco Central do Brasil (BCB). Frequência diária.

A cotação retornada é a PTAX de **venda**, a taxa de câmbio oficial do dólar no Brasil, usada como referência para liquidação de contratos, marcação a mercado de derivativos, conversão de DV01 e índices de fundos cambiais.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `inicio` | `DateLike \| None` | Data inicial. | `None` |
| `fim` | `DateLike \| None` | Data final. Se `None`, usa a data de hoje. | `None` |
| `ultimos` | `int \| None` | Número de registros mais recentes a retornar. Se informado, tem prioridade sobre `inicio` e `fim`. | `None` |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | DataFrame com colunas data e cotacao, ou DataFrame vazio |
| `DataFrame` | se não houver dados. |

**Output Columns**

- data (Date): data da cotação.
- cotacao (pl.Decimal(18, 4)): cotação PTAX de venda em R\\(/US\\).

**Notes**

Disponível desde 28.11.1984. Refere-se às taxas administradas até março de 1990 e às taxas livres a partir de então (Resolução 1690, de 18.3.1990). A partir de março de 1992, essa taxa recebeu a denominação PTAX. Desde 1 de julho de 2011 (Circular 3506), a PTAX corresponde à média aritmética das taxas obtidas em quatro consultas diárias aos dealers de câmbio. Os valores são convertidos diretamente do texto para Decimal, sem passagem por float, com quatro casas decimais.

Examples:

```pycon
>>> import pyield as yd
>>> yd.ptax_serie("20-04-2025", "25-04-2025")
shape: (4, 2)
┌────────────┬───────────────┐
│ data       ┆ cotacao       │
│ ---        ┆ ---           │
│ date       ┆ decimal[18,4] │
╞════════════╪═══════════════╡
│ 2025-04-22 ┆ 5.7496        │
│ 2025-04-23 ┆ 5.6880        │
│ 2025-04-24 ┆ 5.6738        │
│ 2025-04-25 ┆ 5.6846        │
└────────────┴───────────────┘
```
