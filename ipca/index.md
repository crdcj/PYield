# IPCA

O número-índice é retornado como `Decimal` em `yd.ipca.indice()` e como `Decimal(18, 2)` na coluna `indice` de `yd.ipca.indice_serie()`. Os valores preservam as duas casas publicadas pelo IBGE, sem conversão intermediária para `float`. Quando indisponível, a consulta escalar retorna `Decimal("NaN")`; use `.is_nan()` para verificar.

As taxas históricas são retornadas como `Decimal` em `yd.ipca.taxa()` e como `Decimal(18, 4)` na coluna `taxa` de `yd.ipca.taxa_serie()`. A projeção da ANBIMA, em `yd.ipca.taxa_projetada().valor_projetado`, também retorna `Decimal` com quatro casas. A unidade é decimal: `Decimal("0.0045")` representa 0,45%. A taxa histórica indisponível retorna `Decimal("NaN")` na consulta escalar.

## Calendário de divulgação

`yd.ipca.calendario(ano, atualizar=False)` consulta o calendário oficial do IBGE e retorna as colunas `competencia` e `data_divulgacao`, ambas `Date`. O ano seleciona a competência: dezembro pode ter divulgação no ano seguinte. A data prevista não comprova publicação do índice e datas ausentes não são estimadas. `atualizar=True` renova o cache de 60 segundos da consulta.

```python
calendario = yd.ipca.calendario(2026)
```

## `calendario(ano, *, atualizar=False)`

Consulta o calendário oficial de divulgação do IPCA no IBGE.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `ano` | `int` | Ano da competência, inclusive dezembro divulgado no ano seguinte. | *required* |
| `atualizar` | `bool` | Ignora e renova o cache de 60 segundos da consulta completa. | `False` |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | DataFrame ordenado por competência. Sem registros para o ano, retorna |
| `DataFrame` | vazio com o mesmo esquema. |

**Output Columns**

competencia (Date): Primeiro dia do mês de referência do índice. data\_divulgacao (Date): Data de divulgação prevista pelo IBGE.

**Notes**

Fonte: API de calendário do IBGE, produto 9256. Percorre todas as páginas e seleciona divulgações de indicadores. A data prevista não comprova que o índice já foi publicado. Não estima datas ausentes. Erros de rede e payload inválido são propagados.

Raises:

| Type | Description |
| --- | --- |
| `ValueError` | Ano inválido, paginação inválida ou datas conflitantes. |

Examples:

Seleciona a divulgação do IPCA de dezembro do ano atual, prevista para o ano seguinte. As datas e a disponibilidade dependem do calendário publicado pelo IBGE:

```pycon
>>> agenda = yd.ipca.calendario(yd.hoje().year)
>>> dezembro = agenda.filter(pl.col("competencia").dt.month() == 12)
```

## `indice(data)`

Número-índice do IPCA do IBGE para um mês específico.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data` | `DateLike` | Qualquer data dentro do mês desejado. | *required* |

Returns:

| Name | Type | Description |
| --- | --- | --- |
| `Decimal` | `Decimal` | Número-índice do IPCA com duas casas decimais ou |
|  | `Decimal` | `Decimal('NaN')` se não disponível. |

Examples:

```pycon
>>> from pyield import ipca
>>> ipca.indice("01-01-2025")
Decimal('7111.86')
```

## `indice_serie(inicio=None, fim=None, *, ultimos=None)`

Obtém os valores do número-índice do IPCA por intervalo ou últimos meses.

Realiza chamada à API do portal de dados do IBGE no formato: https://servicodados.ibge.gov.br/api/v3/agregados/1737/periodos/YYYYMM-YYYYMM/variaveis/2266?localidades=N1\[all\] Para os últimos meses, usa o formato: https://servicodados.ibge.gov.br/api/v3/agregados/1737/periodos/-N/variaveis/2266?localidades=N1\[all\]

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `inicio` | `DateLike \| None` | Data de início do intervalo. | `None` |
| `fim` | `DateLike \| None` | Data de fim do intervalo. Se `None`, usa a data atual no Brasil. | `None` |
| `ultimos` | `int \| None` | Número de meses mais recentes a recuperar. Se informado, tem prioridade sobre `inicio` e `fim`. | `None` |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | pl.DataFrame com colunas 'periodo' e 'indice'. |

**Output Columns**

- periodo (Int64): período no formato YYYYMM.
- indice (Decimal(18, 2)): número-índice do IPCA.

**Notes**

Os valores são convertidos diretamente do texto do IBGE para Decimal, preservando as duas casas decimais publicadas, sem passagem por float.

Raises:

| Type | Description |
| --- | --- |
| `ValueError` | Se não informar `inicio` nem `ultimos`, ou se `ultimos` for menor ou igual a 0. |

Examples:

```pycon
>>> from pyield import ipca
>>> # Obter os números-índice do IPCA para o primeiro trimestre
>>> ipca.indice_serie(inicio="01-01-2025", fim="01-03-2025")
shape: (3, 2)
┌─────────┬───────────────┐
│ periodo ┆ indice        │
│ ---     ┆ ---           │
│ i64     ┆ decimal[18,2] │
╞═════════╪═══════════════╡
│ 202501  ┆ 7111.86       │
│ 202502  ┆ 7205.03       │
│ 202503  ┆ 7245.38       │
└─────────┴───────────────┘
>>> # Obter o número-índice do IPCA do último mês
>>> df = ipca.indice_serie(ultimos=1)
>>> # Obter os números-índice do IPCA dos últimos 3 meses
>>> df = ipca.indice_serie(ultimos=3)
```

## `taxa(data)`

Taxa mensal do IPCA do IBGE para um mês específico.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data` | `DateLike` | Qualquer data dentro do mês desejado. | *required* |

Returns:

| Name | Type | Description |
| --- | --- | --- |
| `Decimal` | `Decimal` | Taxa mensal do IPCA em decimal, com quatro casas decimais, |
|  | `Decimal` | ou `Decimal('NaN')` se não disponível. |

Examples:

```pycon
>>> from pyield import ipca
>>> taxa = ipca.taxa("01-01-2025")  # decimal (0.0016 = 0,16%)
>>> taxa
Decimal('0.0016')
>>> f"{taxa:.2%}"
'0.16%'
```

## `taxa_projetada()`

Obtém a projeção atual do IPCA no site da ANBIMA.

A função baixa a planilha XLS de indicadores da ANBIMA e extrai os dados da projeção do IPCA.

Returns:

| Name | Type | Description |
| --- | --- | --- |
| `ProjecaoIndicador` | `ProjecaoIndicador` | Objeto contendo: - ultima\_atualizacao (dt.datetime): Data e hora da última atualização. - periodo\_referencia (str): Período de referência no formato "MMM/YY" (ex.: "mar/26"). - valor\_projetado (Decimal): Valor projetado do IPCA em unidade decimal, com quatro casas decimais (0.0045 representa 0,45%). |

Raises:

| Type | Description |
| --- | --- |
| `ConnectionError` | Se houver erro de conexão com o site da ANBIMA. |
| `ValueError` | Se os dados esperados não forem encontrados na planilha. |

**Notes**

Requer conexão com a internet para acessar o site da ANBIMA. O valor é convertido da célula da planilha para Decimal, dividido por 100 e arredondado em quatro casas decimais, com empate para o par.

Examples:

```pycon
>>> from pyield import ipca
>>> # Obter a projeção atual do IPCA na ANBIMA
>>> ipca.taxa_projetada()
ProjecaoIndicador(ultima_atualizacao=..., periodo_referencia=..., valor_projetado=...)
```

## `taxa_serie(inicio=None, fim=None, *, ultimos=None)`

Obtém as taxas mensais do IPCA por intervalo ou últimos meses.

Realiza chamada à API do portal de dados do IBGE no formato: https://servicodados.ibge.gov.br/api/v3/agregados/1737/periodos/YYYYMM-YYYYMM/variaveis/63?localidades=N1\[all\] Para os últimos meses, usa o formato: https://servicodados.ibge.gov.br/api/v3/agregados/1737/periodos/-N/variaveis/63?localidades=N1\[all\]

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `inicio` | `DateLike \| None` | Data de início do intervalo. | `None` |
| `fim` | `DateLike \| None` | Data de fim do intervalo. Se `None`, usa a data atual no Brasil. | `None` |
| `ultimos` | `int \| None` | Número de meses mais recentes a recuperar. Se informado, tem prioridade sobre `inicio` e `fim`. | `None` |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | pl.DataFrame com colunas 'periodo' e 'taxa' (decimal). |

**Output Columns**

- periodo (Int64): período no formato YYYYMM.
- taxa (Decimal(18, 4)): taxa mensal em decimal (ex: 0.0016 = 0,16%).

**Notes**

Os valores percentuais são convertidos diretamente do texto do IBGE para Decimal e divididos por 100, sem passagem por float.

Raises:

| Type | Description |
| --- | --- |
| `ValueError` | Se não informar `inicio` nem `ultimos`, ou se `ultimos` for menor ou igual a 0. |

Examples:

```pycon
>>> from pyield import ipca
>>> ipca.taxa_serie("01-01-2025", "01-03-2025")  # decimal (0.0016 = 0,16%)
shape: (3, 2)
┌─────────┬───────────────┐
│ periodo ┆ taxa          │
│ ---     ┆ ---           │
│ i64     ┆ decimal[18,4] │
╞═════════╪═══════════════╡
│ 202501  ┆ 0.0016        │
│ 202502  ┆ 0.0131        │
│ 202503  ┆ 0.0056        │
└─────────┴───────────────┘
>>> # Obter a taxa do IPCA do último mês
>>> df = ipca.taxa_serie(ultimos=1)
>>> # Obter as taxas do IPCA dos últimos 3 meses
>>> df = ipca.taxa_serie(ultimos=3)
```
