Cálculos e dados de NTN-B.

Convenções de precificação (STN, tabela 3): - Cotação em base 100, truncada a 4 casas. - VNA recebido no cálculo do PU: truncado a 6 casas. - Prazo de desconto: dias úteis / 252, truncado a 14 casas. - PU: truncado a 6 casas. - Cada fluxo descontado: arredondado a 10 casas.

Valores de referência derivados conforme as regras da STN, exibidos em base 100: TAXA\_CUPOM = ((0.06 + 1) \*\* 0.5 - 1) \* 100 \# 6% a.a. com capitalização semestral VALOR\_CUPOM = round(TAXA\_CUPOM, 6) -\> 2.956301 VALOR\_FINAL = 102.956301 DIA\_CUPOM = 15 MESES\_CUPOM = {2, 5, 8, 11}

## `cotacao(data_liquidacao, data_vencimento, taxa)`

Calcula a cotação da NTN-B pela metodologia da STN para leilões primários.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data_liquidacao` | `DateLike` | Data de liquidação da operação. | *required* |
| `data_vencimento` | `DateLike` | Data de vencimento da NTN-B. | *required* |
| `taxa` | `float \| Decimal \| str` | TIR real ao ano, em decimal, na base de 252 dias úteis. Aceita também percentual explícito: "5.75%" ou "5,75%". Antes do cálculo, é truncada em oito casas decimais (seis na forma percentual), descartando as casas excedentes sem arredondar. | *required* |

Returns:

| Name | Type | Description |
| --- | --- | --- |
| `Decimal` | `Decimal` | Cotação em base 100, truncada em 4 casas. Retorna Decimal('NaN') em erro. |

**Notes**

A cotação teórica em base 100 é:

\\\[ \\mathrm{COT} = \\sum\_{i=1}^{n} \\frac{\\mathrm{FC}\_i}{(1+y)^{t\_i}} \\\]

onde:

- \\(\\mathrm{FC}\_i\\): valor do pagamento \\(i\\) em base 100, incluindo cupom e amortização quando aplicável.
- \\(\\mathrm{DU}\_i\\): dias úteis entre liquidação e pagamento \\(i\\).
- \\(t\_i = \\mathrm{DU}\_i/252\\): prazo até o pagamento \\(i\\), em anos de 252 dias úteis.
- \\(n\\): número de pagamentos restantes.
- \\(y\\): TIR real informada em `taxa`, em decimal, expressa ao ano na base de 252 dias úteis, relativa aos fluxos em base 100.

Os fluxos em base 100 representam percentuais do VNA, e não valores monetários futuros. O VNA incorpora a atualização pelo IPCA. No cálculo oficial, além do truncamento da TIR descrito em `Args`, cada prazo é truncado em 14 casas decimais.

A cotação é calculada e retornada na escala percentual (base 100), truncada em 4 casas conforme a STN. Por exemplo, `99.3651` representa 99,3651% do VNA; o PU é calculado como `VNA * cotacao / 100`.

O cupom semestral definido pela STN como 2,956301% é armazenado como 2,956301 em base 100, com 6 casas decimais. Cada fluxo descontado é arredondado em 10 casas antes da soma.

**References**

- Secretaria do Tesouro Nacional. Metodologia de Cálculo dos Títulos Públicos Federais Ofertados nos Leilões Primários. https://crdcj.github.io/PYield/referencias/metodologia-calculo-tpf-stn/

Examples:

```pycon
>>> from pyield import ntnb
>>> cotacao = ntnb.cotacao("31-05-2024", "15-05-2035", "6.149%")
>>> cotacao
Decimal('99.3651')
>>> ntnb.cotacao("31-05-2024", "15-08-2060", "6.1878%")
Decimal('99.5341')
>>> ntnb.cotacao("15-08-2024", "15-08-2032", "5.929%")
Decimal('100.6409')
>>> ntnb.cotacao("15-05-2024", "15-05-2025", "10%")
Decimal('96.4454')
>>> ntnb.cotacao("21-05-2008", "15-08-2010", "8.29%")
Decimal('97.0813')
```

## `dados(data)`

Busca as taxas indicativas de NTN-B para a data de referência.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data` | `DateLike` | Data da consulta. | *required* |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | pl.DataFrame: DataFrame Polars com os dados de NTN-B. Na ausência de dados, retorna vazio com as mesmas colunas e tipos. |

**Output Columns**

- data\_referencia (Date): Data de referência dos dados.
- titulo (String): Tipo do título (ex.: "NTN-B").
- codigo\_selic (Int64): Código do título no SELIC.
- data\_base (Date): Data base de emissão do título.
- data\_vencimento (Date): Data de vencimento do título.
- dias\_uteis (Int64): Dias úteis entre referência e vencimento.
- duration (Float64): Macaulay Duration do título (anos).
- prazo\_medio (Float64): Prazo médio do título (anos).
- dv01 (Float64): Variação no preço para 1bp de taxa.
- pu (Float64): Preço unitário (PU).
- taxa\_compra (Float64): Taxa de compra (decimal).
- taxa\_venda (Float64): Taxa de venda (decimal).
- taxa\_indicativa (Float64): Taxa indicativa (decimal).
- taxa\_di (Float64): Taxa de ajuste do DI Futuro interpolada pelo método flat forward.
- taxa\_zero (Float64): Taxa zero real (via bootstrap das taxas indicativas).
- taxa\_forward (Float64): Taxa forward real (a partir das taxas zero).
- inflacao\_implicita (Float64): Inflação implícita (breakeven) calculada a partir de taxas nominais do DI Futuro e taxas zero das NTN-B.

Examples:

```pycon
>>> from pyield import ntnb
>>> df_ntnb = ntnb.dados("23-08-2024")
```

## `datas_pagamento(data_liquidacao, data_vencimento)`

Gera todas as datas de pagamento entre liquidação e vencimento.

Os pagamentos são semestrais e seguem a série do título, com datas em 15/02, 15/05, 15/08 ou 15/11. No vencimento, o fluxo inclui o último cupom e a amortização do principal. As datas são contratuais e não são ajustadas para dias úteis.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data_liquidacao` | `DateLike` | Data de liquidação (exclusiva). | *required* |
| `data_vencimento` | `DateLike` | Data de vencimento. | *required* |

Returns:

| Type | Description |
| --- | --- |
| `Series` | pl.Series: Série de datas de pagamento entre a liquidação (exclusiva) e o vencimento (inclusivo). Retorna série vazia se o vencimento for menor ou igual à liquidação. |

**Notes**

Para obter as datas efetivas de processamento, use `yd.du.deslocar(..., 0)`.

Examples:

```pycon
>>> from pyield import ntnb
>>> ntnb.datas_pagamento("10-05-2024", "15-05-2025")
shape: (3,)
Series: 'datas_pagamento' [date]
[
    2024-05-15
    2024-11-15
    2025-05-15
]
```

A data de liquidação coincidente com um pagamento é exclusiva:

```pycon
>>> ntnb.datas_pagamento("15-05-2024", "15-05-2025")
shape: (2,)
Series: 'datas_pagamento' [date]
[
    2024-11-15
    2025-05-15
]
```

## `duration(data_liquidacao, data_vencimento, taxa)`

Calcula a Macaulay duration da NTN-B em anos úteis.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data_liquidacao` | `DateLike` | Data de liquidação. | *required* |
| `data_vencimento` | `DateLike` | Data de vencimento. | *required* |
| `taxa` | `float \| str` | TIR real ao ano, em decimal, na base de 252 dias úteis. Aceita também percentual explícito: "5.75%" ou "5,75%". | *required* |

Returns:

| Name | Type | Description |
| --- | --- | --- |
| `float` | `float` | Macaulay duration em anos úteis. |

**Notes**

A Macaulay duration \\(D\\), em anos de 252 dias úteis, é:

\\\[ D = \\frac{\\sum\_{i=1}^{n}t\_i\\,\\mathrm{FC}\_i/(1+y)^{t\_i}} {\\sum\_{i=1}^{n}\\mathrm{FC}\_i/(1+y)^{t\_i}} \\\]

onde:

- \\(\\mathrm{FC}\_i\\): valor do pagamento \\(i\\) em base 100, incluindo cupom e amortização quando aplicável.
- \\(\\mathrm{DU}\_i\\): dias úteis entre liquidação e pagamento \\(i\\).
- \\(t\_i = \\mathrm{DU}\_i/252\\): prazo até o pagamento \\(i\\), em anos de 252 dias úteis.
- \\(n\\): número de pagamentos restantes.
- \\(y\\): TIR real informada em `taxa`, em decimal, expressa ao ano na base de 252 dias úteis, relativa aos fluxos em base 100.

O desconto dos fluxos não aplica os arredondamentos e truncamentos da cotação oficial. O resultado é truncado em 14 casas decimais. O VNA, comum a todos os fluxos, cancela na razão.

Examples:

```pycon
>>> from pyield import ntnb
>>> ntnb.duration("23-08-2024", "15-08-2060", "6.1005%")
15.08305431313046
```

## `duration_expr(data_liquidacao, data_vencimento, taxa)`

Cria expressão Polars para a duration da NTN-B.

O cálculo é aplicado linha a linha porque a duration depende dos fluxos de caixa do título.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data_liquidacao` | `Expr \| str` | Nome de coluna ou expressão Polars com a data de liquidação. | *required* |
| `data_vencimento` | `Expr \| str` | Nome de coluna ou expressão Polars com a data de vencimento. | *required* |
| `taxa` | `Expr \| str` | Nome de coluna ou expressão Polars com a TIR real ao ano, em decimal, na base de 252 dias úteis. | *required* |

Returns:

| Type | Description |
| --- | --- |
| `Expr` | pl.Expr: Expressão sem alias com a Macaulay duration em anos úteis. |

## `dv01(data_liquidacao, data_vencimento, taxa, pu)`

Calcula o DV01 (Dollar Value of 01) da NTN-B em R$.

Representa a variação do PU informado para um aumento de 1 bp (0,01 ponto percentual) na taxa.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data_liquidacao` | `DateLike` | Data de liquidação. | *required* |
| `data_vencimento` | `DateLike` | Data de vencimento. | *required* |
| `taxa` | `float \| Decimal \| str` | TIR real ao ano, em decimal, na base de 252 dias úteis. Aceita também percentual explícito: "5.75%" ou "5,75%". | *required* |
| `pu` | `float \| Decimal` | PU usado como base para o cálculo. | *required* |

Returns:

| Name | Type | Description |
| --- | --- | --- |
| `float` | `float` | DV01, variação de preço para 1 bp. |

**Notes**

Mantendo liquidação e vencimento fixos, os fluxos e seus prazos também ficam fixos. Nesse contexto, a cotação teórica em função da TIR é:

\\\[ \\mathrm{COT}(y) = \\sum\_{i=1}^{n} \\frac{\\mathrm{FC}\_i}{(1+y)^{t\_i}} \\\]

O \\(\\mathrm{DV01}\\) é a redução do preço, em R$, para um aumento de 1 bp na TIR, usando o PU informado como base:

\\\[ \\mathrm{DV01} = \\mathrm{PU}\_{\\mathrm{informado}} \\left(1-\\frac{\\mathrm{COT}(y+0.0001)}{\\mathrm{COT}(y)}\\right) \\\]

onde:

- \\(\\mathrm{FC}\_i\\): valor do pagamento \\(i\\) em base 100, incluindo cupom e amortização quando aplicável.
- \\(\\mathrm{DU}\_i\\): dias úteis entre liquidação e pagamento \\(i\\).
- \\(t\_i = \\mathrm{DU}\_i/252\\): prazo até o pagamento \\(i\\), em anos de 252 dias úteis.
- \\(n\\): número de pagamentos restantes.
- \\(y\\): TIR real informada em `taxa`, em decimal, expressa ao ano na base de 252 dias úteis, relativa aos fluxos em base 100.
- \\(\\mathrm{PU}\_{\\mathrm{informado}}\\): preço recebido em `pu`.
- \\(0.0001\\): aumento de 1 bp na TIR em decimal.

As cotações teóricas não aplicam os arredondamentos e truncamentos da cotação oficial. A razão entre elas é aplicada ao PU informado.

Examples:

```pycon
>>> from pyield import ntnb
>>> cot = ntnb.cotacao("26-03-2025", "15-08-2060", "7.4358%")
>>> pu = ntnb.pu(4470.979474, cot)
>>> ntnb.dv01("26-03-2025", "15-08-2060", "7.4358%", pu)
4.638861271718656
```

## `dv01_expr(data_liquidacao, data_vencimento, taxa, pu)`

Cria expressão Polars para o DV01 da NTN-B.

O cálculo é aplicado linha a linha e reprifica o PU informado para um aumento de 1 bp na taxa.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data_liquidacao` | `Expr \| str` | Nome de coluna ou expressão Polars com a data de liquidação. | *required* |
| `data_vencimento` | `Expr \| str` | Nome de coluna ou expressão Polars com a data de vencimento. | *required* |
| `taxa` | `Expr \| str` | Nome de coluna ou expressão Polars com a TIR real ao ano, em decimal, na base de 252 dias úteis. | *required* |
| `pu` | `Expr \| str` | Nome de coluna ou expressão Polars com o PU usado como base. | *required* |

Returns:

| Type | Description |
| --- | --- |
| `Expr` | pl.Expr: Expressão sem alias com o DV01. |

## `fluxos_caixa(data_liquidacao, data_vencimento)`

Gera os fluxos de caixa da NTN-B entre liquidação e vencimento.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data_liquidacao` | `DateLike` | Data de liquidação (exclusiva). | *required* |
| `data_vencimento` | `DateLike` | Data de vencimento. | *required* |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | pl.DataFrame: DataFrame com as colunas de fluxo. |

**Output Columns**

- data\_pagamento (Date): Data contratual do pagamento, sem ajuste para dia útil.
- valor\_pagamento (Float64): Valor do pagamento em base 100.

**Notes**

Para obter as datas efetivas de processamento, use `yd.du.deslocar(..., 0)`.

Examples:

```pycon
>>> from pyield import ntnb
>>> with pl.Config(float_precision=6):
...     ntnb.fluxos_caixa("10-05-2024", "15-05-2025")
shape: (3, 2)
┌────────────────┬─────────────────┐
│ data_pagamento ┆ valor_pagamento │
│ ---            ┆ ---             │
│ date           ┆ f64             │
╞════════════════╪═════════════════╡
│ 2024-05-15     ┆ 2.956301        │
│ 2024-11-15     ┆ 2.956301        │
│ 2025-05-15     ┆ 102.956301      │
└────────────────┴─────────────────┘
```

## `implicitas(data_liquidacao, vencimentos_tir, taxas_tir, vencimentos_nominais, taxas_nominais, *, extrapolar=False)`

Calcula a inflação implícita para NTN-B contra uma curva nominal de referência.

A inflação implícita (breakeven) relaciona taxas zero reais e nominais, baseada nas taxas zero das NTN-B e na curva nominal informada.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data_liquidacao` | `DateLike` | Data de liquidação da operação. | *required* |
| `vencimentos_tir` | `DatesLike` | Vencimentos das NTN-B usadas como vértices da curva de TIR real. | *required* |
| `taxas_tir` | `ArrayLike` | TIRs reais observadas das NTN-B correspondentes aos vencimentos informados. A função calcula a curva zero real a partir dessas taxas. São expressas ao ano, em decimal, na base de 252 dias úteis. | *required* |
| `vencimentos_nominais` | `DatesLike` | Vencimentos da curva nominal de referência. | *required* |
| `taxas_nominais` | `ArrayLike` | Taxas zero nominais ao ano, em decimal, na base de 252 dias úteis. A curva de referência pode representar DI Futuro, curva soberana prefixada ou outra curva nominal escolhida pelo usuário. | *required* |
| `extrapolar` | `bool` | Se `True`, extrapola a curva nominal fora dos vencimentos informados. Se `False`, vencimentos fora do intervalo da curva nominal retornam valores nulos nas colunas dependentes dessa curva. O padrão é `False`. | `False` |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | pl.DataFrame: DataFrame com as taxas calculadas. |

**Output Columns**

- data\_vencimento (Date): Data de vencimento.
- dias\_uteis (Int64): Dias úteis entre liquidação e vencimento.
- taxa\_tir\_real (Float64): TIR real da NTN-B recebida na entrada, em formato decimal.
- taxa\_zero\_real (Float64): Taxa real zero via bootstrap, em formato decimal.
- taxa\_nominal (Float64): Taxa nominal interpolada, em formato decimal.
- inflacao\_implicita (Float64): Inflação implícita (breakeven), em formato decimal.

**Notes**

A inflação implícita \\(\\pi(t)\\), expressa ao ano e em decimal, é:

\\\[ \\pi(t) = \\frac{1+r\_{\\mathrm{nominal}}(t)} {1+r\_{\\mathrm{real}}(t)}-1 \\\]

onde:

- \\(\\mathrm{DU}\\): dias úteis entre liquidação e vencimento.
- \\(t = \\mathrm{DU}/252\\): prazo até o vencimento, em anos de 252 dias úteis.
- \\(r\_{\\mathrm{nominal}}(t)\\): taxa zero nominal da curva fornecida, interpolada por `flat_forward` para o prazo \\(t\\).
- \\(r\_{\\mathrm{real}}(t)\\): taxa zero real obtida pelo bootstrap das TIRs das NTN-B, para o mesmo prazo \\(t\\).

As taxas são expressas ao ano, em decimal, na base de 252 dias úteis. A comparação usa a taxa zero real, e não diretamente a TIR do título.

A inflação implícita é calculada contra a curva nominal informada. Se essa curva for DI Futuro, o resultado representa a implícita contra DI, não uma inflação soberana prefixada pura.

Examples:

Busca as taxas de NTN-B para uma data de referência. Estas são TIRs e as taxas zero são calculadas a partir delas.

```pycon
>>> df_ntnb = yd.ntnb.dados("19-06-2026")
```

Busca as taxas de ajuste do DI Futuro para a mesma data de referência:

```pycon
>>> df_di = yd.di1.dados("19-06-2026")
```

Calcula a inflação implícita na data de referência:

```pycon
>>> curva = yd.ntnb.implicitas(
...     data_liquidacao="19-06-2026",
...     vencimentos_tir=df_ntnb["data_vencimento"],
...     taxas_tir=df_ntnb["taxa_indicativa"],
...     vencimentos_nominais=df_di["data_vencimento"],
...     taxas_nominais=df_di["taxa_ajuste"],
... )
>>> colunas_percentual = [
...     "taxa_tir_real",
...     "taxa_zero_real",
...     "taxa_nominal",
...     "inflacao_implicita",
... ]
>>> _ = pl.Config.set_tbl_width_chars(150)
>>> curva.with_columns(pl.col(colunas_percentual) * 100)
shape: (15, 6)
┌─────────────────┬────────────┬───────────────┬────────────────┬──────────────┬────────────────────┐
│ data_vencimento ┆ dias_uteis ┆ taxa_tir_real ┆ taxa_zero_real ┆ taxa_nominal ┆ inflacao_implicita │
│ ---             ┆ ---        ┆ ---           ┆ ---            ┆ ---          ┆ ---                │
│ date            ┆ i64        ┆ f64           ┆ f64            ┆ f64          ┆ f64                │
╞═════════════════╪════════════╪═══════════════╪════════════════╪══════════════╪════════════════════╡
│ 2026-08-15      ┆ 41         ┆ 11.15         ┆ 11.15          ┆ 14.133881    ┆ 2.684553           │
│ 2027-05-15      ┆ 226        ┆ 8.5733        ┆ 8.564222       ┆ 14.579493    ┆ 5.540749           │
│ 2028-08-15      ┆ 541        ┆ 8.9683        ┆ 8.970714       ┆ 14.914875    ┆ 5.454824           │
│ 2029-05-15      ┆ 725        ┆ 8.8171        ┆ 8.813162       ┆ 14.953463    ┆ 5.642977           │
│ 2030-08-15      ┆ 1039       ┆ 8.8766        ┆ 8.875555       ┆ 14.916618    ┆ 5.548594           │
│ …               ┆ …          ┆ …             ┆ …              ┆ …            ┆ …                  │
│ 2040-08-15      ┆ 3548       ┆ 7.8262        ┆ 7.608037       ┆ 14.591       ┆ 6.489258           │
│ 2045-05-15      ┆ 4735       ┆ 7.6656        ┆ 7.394303       ┆ null         ┆ null               │
│ 2050-08-15      ┆ 6050       ┆ 7.5659        ┆ 7.243255       ┆ null         ┆ null               │
│ 2055-05-15      ┆ 7239       ┆ 7.4658        ┆ 7.05134        ┆ null         ┆ null               │
│ 2060-08-15      ┆ 8556       ┆ 7.464         ┆ 7.082527       ┆ null         ┆ null               │
└─────────────────┴────────────┴───────────────┴────────────────┴──────────────┴────────────────────┘
```

## `pu(vna, cotacao)`

Calcula o PU da NTN-B pela metodologia da STN para leilões primários.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `vna` | `float \| Decimal` | Valor nominal atualizado (VNA), truncado em seis casas decimais antes do cálculo, sem arredondar. | *required* |
| `cotacao` | `float \| Decimal` | Cotação da NTN-B em base 100. Truncada em quatro casas decimais antes do cálculo, sem arredondar. | *required* |

Returns:

| Name | Type | Description |
| --- | --- | --- |
| `Decimal` | `Decimal` | Preço da NTN-B truncado em 6 casas decimais. |

**Notes**

O preço unitário é:

\\\[ \\mathrm{PU} = \\mathrm{VNA}\\,\\frac{\\mathrm{COT}}{100} \\\]

onde:

- \\(\\mathrm{VNA}\\): valor nominal atualizado pelo IPCA, recebido em `vna`.
- \\(\\mathrm{COT}\\): cotação recebida em `cotacao`, em base 100.

O cálculo aplica os truncamentos de VNA e cotação descritos em `Args` e trunca o PU em seis casas decimais.

**References**

- Secretaria do Tesouro Nacional. Metodologia de Cálculo dos Títulos Públicos Federais Ofertados nos Leilões Primários. https://crdcj.github.io/PYield/referencias/metodologia-calculo-tpf-stn/

Examples:

```pycon
>>> from pyield import ntnb
>>> ntnb.pu(4299.160173, 99.3651)
Decimal('4271.864805')
>>> ntnb.pu(4315.498383, 100.6409)
Decimal('4343.156412')
```

## `taxa(data_liquidacao, data_vencimento, vna, pu)`

Calcula a TIR implícita de uma NTN-B a partir do preço (PU).

A função resolve numericamente a taxa que aproxima o PU informado por `VNA * cotação / 100`, com a cotação calculada pelo valor presente bruto dos fluxos. Como não há arredondamento nem truncamento intermediário, o resultado não pretende reproduzir exatamente a taxa usada para gerar um PU canônico por `pu(vna, cotacao(...))`.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data_liquidacao` | `DateLike` | Data de liquidação. | *required* |
| `data_vencimento` | `DateLike` | Data de vencimento. | *required* |
| `vna` | `float \| Decimal` | Valor nominal atualizado (VNA). | *required* |
| `pu` | `float \| Decimal` | Preço unitário (PU) do título. | *required* |

Returns:

| Name | Type | Description |
| --- | --- | --- |
| `float` | `float` | TIR implícita em formato decimal, sem arredondamento. Retorna NaN para entradas ausentes, PU não positivo ou falha na resolução numérica. |

**Notes**

A TIR real implícita \\(y\\), expressa ao ano, em decimal e na base de 252 dias úteis, é encontrada resolvendo:

\\\[ \\frac{\\mathrm{VNA}}{100} \\sum\_{i=1}^{n}\\frac{\\mathrm{FC}\_i}{(1+y)^{t\_i}} = \\mathrm{PU}\_{\\mathrm{informado}} \\\]

onde:

- \\(\\mathrm{FC}\_i\\): valor do pagamento \\(i\\) em base 100, incluindo cupom e amortização quando aplicável.
- \\(\\mathrm{DU}\_i\\): dias úteis entre liquidação e pagamento \\(i\\).
- \\(t\_i = \\mathrm{DU}\_i/252\\): prazo até o pagamento \\(i\\), em anos de 252 dias úteis.
- \\(n\\): número de pagamentos restantes.
- \\(\\mathrm{VNA}\\): valor nominal atualizado pelo IPCA, recebido em `vna`.
- \\(\\mathrm{PU}\_{\\mathrm{informado}}\\): preço recebido em `pu`.

O lado esquerdo representa o preço teórico, sem os arredondamentos e truncamentos do PU oficial. A TIR se refere aos fluxos em base 100; a atualização pelo IPCA é representada pelo VNA.

Examples:

Exibe as taxas em formato decimal:

```pycon
>>> from pyield import ntnb
>>> ntnb.taxa("31-05-2024", "15-05-2035", 4299.160173, 4271.864805)
0.06149006550833583
>>> ntnb.taxa("15-08-2024", "15-08-2032", 4315.498383, 4343.156412) * 100
5.929006687775254
>>> ntnb.taxa("21-05-2008", "15-08-2010", 1728.461136, 1781.867128) * 100
5.234584951922298
```

## `taxas_zero(data_liquidacao, vencimentos, taxas)`

Calcula a curva zero de NTN-B pelo bootstrap de forwards.

Retorna apenas os vencimentos informados, com taxas em base 252 dias úteis. As entradas podem ser as taxas indicativas da ANBIMA obtidas por `yd.ntnb.dados` ou TIRs fornecidas pelo usuário.

O método calcula as taxas zero a partir das TIRs observadas das NTN-B, calibrando uma taxa forward por trecho para que os fluxos descontados pela curva zero reproduzam a cotação obtida pela TIR de cada título. A calibração é iterativa e tem convergência condicional: depende da existência de um intervalo válido para cada raiz.

**Notes**

**Racional**

Uma TIR desconta todos os fluxos de um título por uma única taxa. Uma curva zero, por outro lado, precisa de uma taxa para cada data de fluxo. O bootstrap transforma as TIRs em uma estrutura de taxas forward e, a partir dela, em taxas zero.

**Curva de forwards e taxa zero**

A curva usa como vértices as datas de pagamento dos títulos informados. Para cada NTN-B, a taxa forward calibrada é mantida constante desde o vencimento anterior até o seu vencimento. A primeira taxa forward começa na TIR do título mais curto.

A taxa zero é obtida acumulando os fatores dos trechos calibrados:

\\\[ r\_0 = f\_0 \\\]

\\\[ (1 + r\_i)^{t\_i} = (1 + r\_{i-1})^{t\_{i-1}} (1 + f\_i)^{t\_i - t\_{i-1}} \\\]

onde:

- \\(i\\): índice dos títulos em ordem de vencimento, começando em zero.
- \\(\\mathrm{DU}\_i\\): dias úteis entre liquidação e vencimento do título \\(i\\).
- \\(t\_i = \\mathrm{DU}\_i/252\\): prazo até o vencimento do título \\(i\\), em anos de 252 dias úteis.
- \\(f\_i\\): taxa forward real do trecho que termina no vencimento do título \\(i\\). O primeiro trecho começa na liquidação.
- \\(r\_i\\): taxa zero real para o prazo \\(t\_i\\).

As taxas são expressas ao ano, em decimal, na base de 252 dias úteis.

**Calibração sequencial**

Para cada título, do menor para o maior vencimento, a função calcula a cotação-alvo pela TIR observada:

\\\[ \\mathrm{COT}\_i^{\\mathrm{TIR}} = \\sum\_{k=1}^{n\_i} \\frac{\\mathrm{FC}\_{i,k}}{(1+y\_i)^{t\_{i,k}}} \\\]

Em seguida, busca por bisseção o forward \\(f\_i\\) que zera o erro de cotação \\(E\_i(f\_i)\\):

\\\[ E\_i(f\_i) = \\sum\_{k=1}^{n\_i} \\frac{\\mathrm{FC}\_{i,k}}{(1 + r(t\_{i,k}; f\_i))^{t\_{i,k}}} - \\mathrm{COT}\_i^{\\mathrm{TIR}} \\\]

onde:

- \\(n\_i\\): número de pagamentos restantes do título \\(i\\).
- \\(\\mathrm{FC}\_{i,k}\\): valor do pagamento \\(k\\) do título \\(i\\), em base 100, incluindo cupom e amortização quando aplicável.
- \\(\\mathrm{DU}\_{i,k}\\): dias úteis entre liquidação e pagamento \\(k\\) do título \\(i\\).
- \\(t\_{i,k} = \\mathrm{DU}\_{i,k}/252\\): prazo até esse pagamento, em anos de 252 dias úteis.
- \\(y\_i\\): TIR real observada do título \\(i\\).
- \\(r(t\_{i,k}; f\_i)\\): taxa zero real para o prazo do pagamento, obtida pela curva que mantém os trechos anteriores fixos e usa \\(f\_i\\) no trecho em calibração.

Os forwards e taxas zero já calibrados nos títulos curtos permanecem fixos durante a calibração dos títulos longos. Por isso, cada etapa tem apenas uma incógnita e é resolvida por bisseção quando há mudança de sinal no intervalo pesquisado. A coluna `taxa_forward` retorna essa incógnita calibrada: em cada linha, ela é constante no trecho que termina no respectivo vencimento. O primeiro trecho começa na liquidação.

**Convergência condicional**

A bisseção é determinística quando encontra um intervalo que contém uma raiz. A função tenta expandir o limite superior para encontrar esse intervalo, mas pode não encontrá-lo para entradas incompatíveis ou extremos. Nesse caso, a calibração não produz uma curva e lança `RuntimeError`. A mesma exceção é lançada se a avaliação numérica produzir valor não finito ou a bisseção não convergir em 100 iterações.

**Precisão do método**

A calibração usa \\(\\mathrm{DU} / 252\\) sem truncamento e soma os valores presentes sem arredondamento. Isso difere de :func:`cotacao`, que aplica as regras da STN de arredondamento dos fluxos e truncamento da cotação.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data_liquidacao` | `DateLike` | Data de liquidação. | *required* |
| `vencimentos` | `DatesLike` | Datas de vencimento das NTN-B. | *required* |
| `taxas` | `ArrayLike` | TIRs reais ao ano, em decimal, na base de 252 dias úteis (ex.: 0.10 para 10%). | *required* |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | pl.DataFrame: Curva zero calibrada pelo bootstrap de forwards. Retorna vazio quando não restarem vencimentos posteriores à liquidação. |

Raises:

| Type | Description |
| --- | --- |
| `RuntimeError` | Se não for possível encontrar um intervalo válido para alguma taxa forward, ocorrer valor não finito na avaliação ou a bisseção não convergir. |

**Output Columns**

- data\_vencimento (Date): Data do vértice da curva.
- dias\_uteis (Int64): Dias úteis entre liquidação e vértice.
- taxa\_tir (Float64): TIR recebida na entrada, em formato decimal.
- taxa\_forward (Float64): Parâmetro calibrado por bisseção para o trecho que termina no vencimento, em formato decimal.
- taxa\_zero (Float64): Taxa zero real anualizada, em formato decimal.

Examples:

```pycon
>>> from pyield import ntnb
>>> import polars.selectors as cs
>>> # Taxas indicativas da ANBIMA na data de referência.
>>> df = ntnb.dados("16-08-2024")
>>> curva = ntnb.taxas_zero(
...     data_liquidacao="16-08-2024",
...     vencimentos=df["data_vencimento"],
...     taxas=df["taxa_indicativa"],
... )
>>> # Taxas em percentual.
>>> curva.with_columns(cs.starts_with("taxa_") * 100)
shape: (14, 5)
┌─────────────────┬────────────┬──────────┬──────────────┬───────────┐
│ data_vencimento ┆ dias_uteis ┆ taxa_tir ┆ taxa_forward ┆ taxa_zero │
│ ---             ┆ ---        ┆ ---      ┆ ---          ┆ ---       │
│ date            ┆ i64        ┆ f64      ┆ f64          ┆ f64       │
╞═════════════════╪════════════╪══════════╪══════════════╪═══════════╡
│ 2025-05-15      ┆ 185        ┆ 6.3893   ┆ 6.3893       ┆ 6.3893    │
│ 2026-08-15      ┆ 502        ┆ 6.6095   ┆ 6.745466     ┆ 6.614071  │
│ 2027-05-15      ┆ 687        ┆ 6.4164   ┆ 5.853671     ┆ 6.40877   │
│ 2028-08-15      ┆ 1002       ┆ 6.3199   ┆ 6.080954     ┆ 6.305605  │
│ 2029-05-15      ┆ 1186       ┆ 6.1753   ┆ 5.279866     ┆ 6.145816  │
│ …               ┆ …          ┆ …        ┆ …            ┆ …         │
│ 2040-08-15      ┆ 4009       ┆ 5.8873   ┆ 5.6644       ┆ 5.832614  │
│ 2045-05-15      ┆ 5196       ┆ 6.0013   ┆ 6.729968     ┆ 6.036943  │
│ 2050-08-15      ┆ 6511       ┆ 6.0247   ┆ 6.234593     ┆ 6.076832  │
│ 2055-05-15      ┆ 7700       ┆ 5.9926   ┆ 5.521283     ┆ 5.990856  │
│ 2060-08-15      ┆ 9017       ┆ 6.0179   ┆ 6.498313     ┆ 6.064823  │
└─────────────────┴────────────┴──────────┴──────────────┴───────────┘
```

O bootstrap considera apenas os fluxos posteriores à liquidação:

```pycon
>>> df = ntnb.dados("15-05-2026")
>>> curva = ntnb.taxas_zero(
...     data_liquidacao="18-05-2026",
...     vencimentos=df["data_vencimento"],
...     taxas=df["taxa_indicativa"],
... )
>>> curva_percentual = curva.with_columns(cs.starts_with("taxa_") * 100)
>>> curva_percentual
shape: (15, 5)
┌─────────────────┬────────────┬──────────┬──────────────┬───────────┐
│ data_vencimento ┆ dias_uteis ┆ taxa_tir ┆ taxa_forward ┆ taxa_zero │
│ ---             ┆ ---        ┆ ---      ┆ ---          ┆ ---       │
│ date            ┆ i64        ┆ f64      ┆ f64          ┆ f64       │
╞═════════════════╪════════════╪══════════╪══════════════╪═══════════╡
│ 2026-08-15      ┆ 64         ┆ 10.2013  ┆ 10.2013      ┆ 10.2013   │
│ 2027-05-15      ┆ 249        ┆ 8.12     ┆ 7.395284     ┆ 8.109613  │
│ 2028-08-15      ┆ 564        ┆ 8.1131   ┆ 8.097792     ┆ 8.103011  │
│ 2029-05-15      ┆ 748        ┆ 8.0391   ┆ 7.808764     ┆ 8.030555  │
│ 2030-08-15      ┆ 1062       ┆ 8.066    ┆ 8.126503     ┆ 8.058915  │
│ …               ┆ …          ┆ …        ┆ …            ┆ …         │
│ 2040-08-15      ┆ 3571       ┆ 7.4812   ┆ 6.670623     ┆ 7.341193  │
│ 2045-05-15      ┆ 4758       ┆ 7.3767   ┆ 6.785801     ┆ 7.202367  │
│ 2050-08-15      ┆ 6073       ┆ 7.3      ┆ 6.642902     ┆ 7.080976  │
│ 2055-05-15      ┆ 7262       ┆ 7.2662   ┆ 6.768381     ┆ 7.029733  │
│ 2060-08-15      ┆ 8579       ┆ 7.2674   ┆ 7.190818     ┆ 7.054446  │
└─────────────────┴────────────┴──────────┴──────────────┴───────────┘
```

A curva retornada contém apenas os vencimentos informados. Para estimar uma taxa zero em um prazo intermediário, use o método de interpolação desejado. Neste exemplo, `flat_forward` preserva a hipótese de forwards constantes por trecho usada no bootstrap:

```pycon
>>> curva_decimal = ntnb.taxas_zero(
...     data_liquidacao="18-05-2026",
...     vencimentos=df["data_vencimento"],
...     taxas=df["taxa_indicativa"],
... )
>>> interpolar = yd.Interpolador(
...     curva_decimal["dias_uteis"],
...     curva_decimal["taxa_zero"],
...     metodo="flat_forward",
... )
>>> du_intermediario = (
...     curva_decimal["dias_uteis"][0] + curva_decimal["dias_uteis"][1]
... ) // 2
>>> 0 < interpolar(du_intermediario) < 1
True
```

## `vencimentos(data)`

Busca os vencimentos de NTN-B disponíveis para a data de referência.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data` | `DateLike` | Data da consulta. | *required* |

Returns:

| Type | Description |
| --- | --- |
| `Series` | pl.Series: Série de datas de vencimento de NTN-B. |

Examples:

```pycon
>>> from pyield import ntnb
>>> ntnb.vencimentos("16-08-2024")
shape: (14,)
Series: 'data_vencimento' [date]
[
    2025-05-15
    2026-08-15
    2027-05-15
    2028-08-15
    2029-05-15
    …
    2040-08-15
    2045-05-15
    2050-08-15
    2055-05-15
    2060-08-15
]
```
