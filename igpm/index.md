# IGP-M

As taxas mensais são retornadas como `Decimal` em `yd.igpm.taxa()` e como `Decimal(18, 4)` na coluna `taxa` de `yd.igpm.taxa_serie()`. A unidade é decimal: `Decimal("0.0027")` representa 0,27%. Os valores são convertidos diretamente do texto do SGS/BCB, sem passagem por `float`.

Quando indisponível, a consulta escalar retorna `Decimal("NaN")`; use `.is_nan()` para verificar. A consulta de série retorna um DataFrame vazio com o mesmo esquema. A série de taxas representa a variação mensal.

Os números-índice são obtidos com `yd.igpm.indice(data)` e `yd.igpm.indice_serie(inicio, fim)` ou `yd.igpm.indice_serie(ultimos=n)`. A fonte é a FGV/Conjuntura Econômica, disponibilizada pelo [Ipeadata](<https://www.ipeadata.gov.br/api/>) na série `IGP12_IGPM12`. O retorno escalar usa `Decimal` e a coluna `indice` usa `Decimal(18, 10)`: as dez casas preservam os valores antigos, anteriores a 1994. A coluna `periodo` identifica a competência no formato `YYYYMM`. O índice indisponível retorna `Decimal("NaN")` na consulta escalar.

Nas séries de índices, qualquer dia identifica o mês inteiro; os dois meses extremos são incluídos. `fim=None` usa a data atual no Brasil. Informe `inicio` ou `ultimos`; quando informado, `ultimos` tem prioridade sobre as datas.

## Calendário de divulgação

`yd.igpm.calendario(ano, atualizar=False)` consulta as divulgações mensais na FGV IBRE, excluindo as prévias decendiais e os demais índices. Retorna `competencia` e `data_divulgacao`, ambas `Date`. A competência corresponde ao mês da divulgação mensal. Datas ausentes não são estimadas, e o calendário não comprova publicação. `atualizar=True` renova o cache de 60 segundos do ano.

```python
calendario = yd.igpm.calendario(2026)
```

Taxas e números-índice do IGP-M por competência ou série histórica.

## `calendario(ano, *, atualizar=False)`

Consulta o calendário de divulgação mensal do IGP-M na FGV IBRE.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `ano` | `int` | Ano das divulgações e competências. | *required* |
| `atualizar` | `bool` | Ignora e renova o cache de 60 segundos desse ano. | `False` |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | DataFrame ordenado por competência. Sem registros, retorna vazio com |
| `DataFrame` | o mesmo esquema. |

**Output Columns**

competencia (Date): Primeiro dia do mês da divulgação mensal do IGP-M. data\_divulgacao (Date): Data prevista pela FGV IBRE.

**Notes**

Fonte: calendário de divulgação da FGV IBRE. Seleciona o IGP-M mensal com seus componentes, excluindo prévias decendiais e outros índices. A competência corresponde ao mês da divulgação mensal. A data prevista não comprova publicação. Não cria janelas estimadas quando faltam dados. Erros de rede e payload inválido são propagados.

Raises:

| Type | Description |
| --- | --- |
| `ValueError` | Ano inválido, data inválida ou datas conflitantes. |

Examples:

Seleciona as divulgações previstas a partir de hoje, sem incluir prévias decendiais. O resultado depende do calendário da FGV IBRE e não comprova que os índices já tenham sido publicados:

```pycon
>>> hoje = yd.hoje()
>>> agenda = yd.igpm.calendario(hoje.year)
>>> proximas = agenda.filter(pl.col("data_divulgacao") >= hoje)
```

## `indice(data)`

Obtém o número-índice do IGP-M para um mês específico.

Fonte: FGV/Conjuntura Econômica, disponibilizada pelo Ipeadata na série `IGP12_IGPM12`, com base agosto de 1994 = 100.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data` | `DateLike \| None` | Qualquer data dentro do mês desejado, nos formatos da biblioteca. | *required* |

Returns:

| Name | Type | Description |
| --- | --- | --- |
| `Decimal` | `Decimal` | Número-índice com dez casas decimais ou `Decimal('NaN')` |
|  | `Decimal` | se a data for nula, vazia ou o índice não estiver disponível. |

Examples:

```pycon
>>> yd.igpm.indice("01-06-2000")
Decimal('183.7450000000')
>>> yd.igpm.indice("01-06-1989")
Decimal('0.0000308619')
```

## `indice_serie(inicio=None, fim=None, *, ultimos=None)`

Obtém os números-índice mensais do IGP-M por intervalo ou últimos meses.

Fonte: FGV/Conjuntura Econômica, disponibilizada pelo Ipeadata na série `IGP12_IGPM12`, com base agosto de 1994 = 100.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `inicio` | `DateLike \| None` | Qualquer data no primeiro mês do intervalo. | `None` |
| `fim` | `DateLike \| None` | Qualquer data no último mês do intervalo. Se `None`, usa a data atual no Brasil. | `None` |
| `ultimos` | `int \| None` | Número de meses mais recentes a recuperar. Se informado, tem prioridade sobre `inicio` e `fim`. | `None` |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | DataFrame com os números-índice em ordem crescente de competência. |
| `DataFrame` | Retorna vazio, preservando o esquema, se não houver dados no intervalo. |

**Output Columns**

- periodo (Int64): competência mensal no formato YYYYMM.
- indice (Decimal(18, 10)): número-índice do IGP-M.

**Notes**

Os números do JSON são lidos diretamente como Decimal, sem passagem por float. As dez casas preservam também os valores anteriores a 1994, que podem ser menores que 0.001. Valores ausentes são omitidos.

Raises:

| Type | Description |
| --- | --- |
| `ValueError` | Se não informar `inicio` nem `ultimos`, se `ultimos` for menor ou igual a zero ou se alguma data for inválida. |

Examples:

```pycon
>>> yd.igpm.indice_serie("01-06-2000", "31-07-2000").to_dicts()
[{'periodo': 200006, 'indice': Decimal('183.7450000000')},
 {'periodo': 200007, 'indice': Decimal('186.6340000000')}]
```

## `taxa(data)`

Taxa mensal do IGP-M via SGS do Banco Central (série 189, fonte FGV).

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data` | `DateLike` | Qualquer data dentro do mês desejado. | *required* |

Returns:

| Name | Type | Description |
| --- | --- | --- |
| `Decimal` | `Decimal` | Variação mensal em decimal com quatro casas decimais, ou |
|  | `Decimal` | `Decimal('NaN')` se não disponível. |

Examples:

```pycon
>>> taxa = yd.igpm.taxa("15-01-2025")
>>> taxa
Decimal('0.0027')
```

Qualquer dia seleciona a competência mensal. Uma taxa negativa representa deflação, como os -0,34% de março de 2025:

```pycon
>>> yd.igpm.taxa("31-03-2025")
Decimal('-0.0034')
```

## `taxa_serie(inicio=None, fim=None, *, ultimos=None)`

Obtém as taxas mensais do IGP-M via SGS do Banco Central (série 189).

O índice é produzido pela FGV. A série representa a variação mensal, não o número-índice nem a variação acumulada em 12 meses.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `inicio` | `DateLike \| None` | Qualquer data no primeiro mês do intervalo. | `None` |
| `fim` | `DateLike \| None` | Qualquer data no último mês do intervalo. Se `None`, usa a data atual no Brasil. | `None` |
| `ultimos` | `int \| None` | Número de meses mais recentes a recuperar. Se informado, tem prioridade sobre `inicio` e `fim`. | `None` |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | DataFrame com as taxas mensais em decimal, ou vazio se não houver dados. |

**Output Columns**

- periodo (Int64): competência mensal no formato YYYYMM.
- taxa (Decimal(18, 4)): variação mensal em decimal (0.005 = 0,50%).

**Notes**

Os valores percentuais são convertidos diretamente do texto do SGS para Decimal e divididos por 100, sem passagem por float.

Raises:

| Type | Description |
| --- | --- |
| `ValueError` | Se não informar `inicio` nem `ultimos`, ou se `ultimos` for menor ou igual a 0. |

Examples:

```pycon
>>> df = yd.igpm.taxa_serie("01-01-2025", "31-03-2025")
>>> df.to_dicts()
[{'periodo': 202501, 'taxa': Decimal('0.0027')},
 {'periodo': 202502, 'taxa': Decimal('0.0106')},
 {'periodo': 202503, 'taxa': Decimal('-0.0034')}]
```

A taxa é mensal e decimal: março registra deflação de 0,34%. Para acompanhar as últimas competências, sem fixar os valores:

```pycon
>>> df = yd.igpm.taxa_serie(ultimos=12)
```
