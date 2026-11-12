# IGP-M

As escalas e a cobertura dos números-índice foram ajustadas na [0.63.0](<https://crdcj.github.io/PYield/release-0.63.0/index.md#cobertura-e-organizacao-do-igp-m>).

As taxas mensais são retornadas como `Decimal` em `yd.igpm.taxa()` e como `Decimal(18, 4)` na coluna `taxa` de `yd.igpm.taxa_serie()`. A unidade é decimal: `Decimal("0.0027")` representa 0,27%. Os valores são convertidos diretamente do texto do SGS/BCB, sem passagem por `float`.

Quando indisponível, a consulta escalar retorna `Decimal("NaN")`; use `.is_nan()` para verificar. A consulta de série retorna um DataFrame vazio com o mesmo esquema. A série de taxas representa a variação mensal.

Os números-índice são obtidos com `yd.igpm.indice(data)` e `yd.igpm.indice_serie(inicio, fim)` ou `yd.igpm.indice_serie(ultimos=n)`. A fonte é a FGV/Conjuntura Econômica, disponibilizada pelo [Ipeadata](<https://www.ipeadata.gov.br/api/>) na série `IGP12_IGPM12`. O retorno escalar usa `Decimal` com três casas e a coluna `indice` usa `Decimal(18, 3)`. A cobertura começa em **agosto de 1994**, base 100 da série. A coluna `competencia` (`Date`) identifica o mês pelo seu primeiro dia, como nas demais séries mensais da biblioteca. O índice indisponível retorna `Decimal("NaN")` na consulta escalar.

O corte acompanha a base do índice: julho de 1994 ainda tem quatro casas decimais na fonte (`92.9684`); de agosto de 1994 em diante, três casas preservam os valores. A biblioteca cobre esse período para manter um esquema simples e coerente com seu uso em cálculos financeiros em reais. Competências anteriores ao corte são omitidas nas séries e retornam `Decimal("NaN")` na consulta escalar. Um intervalo que atravesse o corte retorna somente agosto de 1994 em diante; um intervalo inteiramente anterior retorna DataFrame vazio, com o mesmo esquema. A seleção por `ultimos` também respeita esse limite.

Para consultar os valores anteriores com sua precisão original, acesse a [série completa na API do Ipeadata](<https://www.ipeadata.gov.br/api/odata4/ValoresSerie(SERCODIGO=> "IGP12_IGPM12")). Essa consulta é feita diretamente na fonte, fora da API de índices do PYield. A biblioteca omite esses índices, em vez de retorná-los com precisão reduzida. A cobertura das taxas mensais segue a série SGS 189. A projeção da ANBIMA, em `yd.igpm.taxa_projetada().valor_projetado`, também retorna `Decimal` com quatro casas.

Nas séries de índices, qualquer dia identifica o mês inteiro; os dois meses extremos são incluídos. `fim=None` usa a data atual no Brasil. Informe `inicio` ou `ultimos`; quando informado, `ultimos` tem prioridade sobre as datas.

## Projeção vigente

`yd.igpm.taxa_projetada()` retorna a projeção do IGP-M que vale hoje para a atualização do VNA, segundo o Grupo Consultivo Permanente Macroeconômico da ANBIMA. O objeto informa `competencia` (primeiro dia do mês de referência, como em `calendario`), `valor_projetado`, `data_coleta` e `data_validade`. A projeção divulgada no fim da tarde só é retornada a partir da sua data de validade. Entre a divulgação do índice fechado e a validade da primeira projeção do mês seguinte, nenhuma projeção está vigente e a função retorna `None`.

```python
projecao = yd.igpm.taxa_projetada()
```

## Calendário de divulgação

`yd.igpm.calendario(ano)` consulta as divulgações mensais na FGV IBRE, excluindo as prévias decendiais e os demais índices. Retorna `competencia` e `data_divulgacao`, ambas `Date`. A competência corresponde ao mês da divulgação mensal. Datas ausentes não são estimadas, e o calendário não comprova publicação.

```python
calendario = yd.igpm.calendario(2026)
```

Taxas, números-índice, projeção e calendário de divulgação do IGP-M.

Disponibiliza `taxa`, `taxa_serie`, `indice`, `indice_serie`, `taxa_projetada` e `calendario`. As taxas vêm do SGS/BCB e a projeção, da ANBIMA; os índices e o calendário têm implementações próprias neste pacote.

## `calendario(ano)`

Consulta o calendário de divulgação mensal do IGP-M na FGV IBRE.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `ano` | `int` | Ano das divulgações e competências. | *required* |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | DataFrame ordenado por competência. Sem registros, retorna vazio com |
| `DataFrame` | o mesmo esquema. |

**Output Columns**

- competencia (Date): Primeiro dia do mês da divulgação mensal do IGP-M.
- data\_divulgacao (Date): Data prevista pela FGV IBRE.

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
| `Decimal` | `Decimal` | Número-índice com três casas decimais ou `Decimal('NaN')` |
|  | `Decimal` | se a data for nula, vazia, anterior a agosto de 1994 ou o índice não |
|  | `Decimal` | estiver disponível. |

**Notes**

A cobertura começa em agosto de 1994, base 100 da série. Os dados anteriores permanecem disponíveis diretamente no Ipeadata, fora do escopo desta consulta.

Examples:

```pycon
>>> yd.igpm.indice("01-06-2000")
Decimal('183.745')
>>> yd.igpm.indice("01-06-1989")
Decimal('NaN')
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

- competencia (Date): primeiro dia do mês de referência.
- indice (Decimal(18, 3)): número-índice do IGP-M.

**Notes**

Os números do JSON são lidos diretamente como Decimal, sem passagem por float. A cobertura começa em agosto de 1994, base 100 da série, com três casas decimais. Os valores anteriores, que exigem mais casas, ficam fora do escopo da biblioteca e podem ser consultados diretamente na série original do Ipeadata. Valores ausentes e competências anteriores ao corte são omitidos; intervalos inteiramente anteriores retornam vazios. O limite também se aplica à seleção por ultimos.

Raises:

| Type | Description |
| --- | --- |
| `ValueError` | Se não informar `inicio` nem `ultimos`, se `ultimos` for menor ou igual a zero ou se alguma data for inválida. |

Examples:

```pycon
>>> yd.igpm.indice_serie("01-06-2000", "31-07-2000")
shape: (2, 2)
┌─────────────┬───────────────┐
│ competencia ┆ indice        │
│ ---         ┆ ---           │
│ date        ┆ decimal[18,3] │
╞═════════════╪═══════════════╡
│ 2000-06-01  ┆ 183.745       │
│ 2000-07-01  ┆ 186.634       │
└─────────────┴───────────────┘
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

## `taxa_projetada()`

Obtém a projeção do IGP-M vigente hoje, segundo a ANBIMA.

A função consulta a página de projeções do Grupo Consultivo Permanente Macroeconômico da ANBIMA e seleciona, entre as projeções do mês corrente, a de maior data de validade que não seja posterior à data de hoje no Brasil.

Returns:

| Type | Description |
| --- | --- |
| `ProjecaoIndicador \| None` | ProjecaoIndicador \| None: Objeto contendo: - competencia (dt.date): Primeiro dia do mês de referência da projeção (ex.: `dt.date(2026, 11, 1)`). - valor\_projetado (Decimal): Valor projetado do IGP-M em unidade decimal, com quatro casas decimais (0.0078 representa 0,78%). - data\_coleta (dt.date): Dia em que a ANBIMA coletou as projeções. - data\_validade (dt.date): Data a partir da qual a projeção vale para a atualização do Valor Nominal dos títulos indexados ao IGP-M. |
| `ProjecaoIndicador \| None` | Retorna `None` quando nenhuma projeção do mês corrente está |
| `ProjecaoIndicador \| None` | vigente, o que ocorre entre a divulgação do IGP-M fechado e a data de |
| `ProjecaoIndicador \| None` | validade da primeira projeção do mês seguinte. Nesse intervalo, a |
| `ProjecaoIndicador \| None` | atualização do VNA usa o índice oficial. |

Raises:

| Type | Description |
| --- | --- |
| `ConnectionError` | Se houver erro de conexão com o site da ANBIMA. |
| `ValueError` | Se a página não tiver a estrutura esperada. |

**Notes**

Fonte: ANBIMA, página "Projeções IPCA e IGP-M" (https://www.anbima.com.br/pt\_br/informar/estatisticas/precos-e-indices/projecao-de-inflacao-gp-m.htm), lida do conteúdo que o portal carrega dinamicamente.

A coleta ocorre no dia da divulgação de cada prévia e do IGP-M fechado; o resultado é publicado por volta das 17h30 e passa a valer na data de validade informada pela ANBIMA. Uma projeção recém-divulgada só é retornada a partir da sua data de validade.

O valor é convertido do texto da página para Decimal, dividido por 100 e arredondado em quatro casas decimais, com empate para o par.

Examples:

```pycon
>>> projecao = yd.igpm.taxa_projetada()
>>> projecao is None or projecao.data_validade <= yd.hoje()
True
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

- competencia (Date): primeiro dia do mês de referência.
- taxa (Decimal(18, 4)): variação mensal em decimal (0.005 = 0,50%).

**Notes**

Os valores percentuais são convertidos diretamente do texto do SGS para Decimal e divididos por 100, sem passagem por float.

Raises:

| Type | Description |
| --- | --- |
| `ValueError` | Se não informar `inicio` nem `ultimos`, ou se `ultimos` for menor ou igual a 0. |

Examples:

```pycon
>>> yd.igpm.taxa_serie("01-01-2025", "31-03-2025")
shape: (3, 2)
┌─────────────┬───────────────┐
│ competencia ┆ taxa          │
│ ---         ┆ ---           │
│ date        ┆ decimal[18,4] │
╞═════════════╪═══════════════╡
│ 2025-01-01  ┆ 0.0027        │
│ 2025-02-01  ┆ 0.0106        │
│ 2025-03-01  ┆ -0.0034       │
└─────────────┴───────────────┘
```

A taxa é mensal e decimal: março registra deflação de 0,34%. Para acompanhar as últimas competências, sem fixar os valores:

```pycon
>>> df = yd.igpm.taxa_serie(ultimos=12)
```
