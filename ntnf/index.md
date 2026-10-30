## Comparação experimental de curvas zero

`yd.ntnf.taxas_zero_forwards` constrói uma curva conjunta de LTN e NTN-F, com forwards constantes entre vértices. A LTN tem prioridade somente quando os vencimentos coincidem. Uma NTN-F entre duas LTNs participa da calibração. O método existente `yd.ntnf.taxas_zero` e `yd.tpf.curva_pre` permanecem inalterados.

```python
import polars as pl

import pyield as yd

ltn = yd.ltn.dados("04-09-2026")
ntnf = yd.ntnf.dados("04-09-2026")
entradas = dict(
    data_liquidacao="04-09-2026",
    vencimentos_ltn=ltn["data_vencimento"],
    taxas_ltn=ltn["taxa_indicativa"],
    vencimentos_ntnf=ntnf["data_vencimento"],
    taxas_ntnf=ntnf["taxa_indicativa"],
)
antiga = yd.ntnf.taxas_zero(**entradas)
proposta = yd.ntnf.taxas_zero_forwards(**entradas)
comparacao = antiga.join(
    proposta, on=["data_vencimento", "dias_uteis"], suffix="_proposta"
).with_columns(
    diferenca_pb=(pl.col("taxa_zero_proposta") - pl.col("taxa_zero")) * 10_000
)
```

A proposta retorna todos os vencimentos selecionados, inclusive os de LTN. Preserve todos esses vértices ao interpolar por `flat_forward`: filtrar apenas as datas de NTN-F pode mudar os descontos intermediários. O preço-alvo da calibração não aplica arredondamentos ou truncamentos, como no bootstrap da NTN-B; por isso, pode diferir ligeiramente do PU oficial.

Cálculos e dados de NTN-F.

Convenções de precificação (STN, tabela 3): - Prazo de desconto: dias úteis / 252, truncado a 14 casas. - PU: truncado a 6 casas. - Cada fluxo descontado: arredondado a 9 casas.

Valores de referência derivados conforme as regras da STN: TAXA\_CUPOM = (0.10 + 1) \*\* 0.5 - 1 -\> 10% a.a. com capitalização semestral VALOR\_FACE = 1000 VALOR\_CUPOM = round(VALOR\_FACE \* TAXA\_CUPOM, 5) VALOR\_FINAL = VALOR\_FACE + VALOR\_CUPOM

A NTN-F paga dois cupons por ano (semestrais). As datas de cupom são derivadas do vencimento (retrocedendo 6 em 6 meses), sem depender de meses fixos. Ex.: vencimento 01-01-2027 gera cupons em 01-07-2026, 01-01-2026, ...

## `dados(data)`

Busca as taxas indicativas de NTN-F para a data de referência.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data` | `DateLike` | Data da consulta. | *required* |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | pl.DataFrame: DataFrame Polars com os dados de NTN-F. Na ausência de dados, retorna vazio com as mesmas colunas e tipos. |

**Output Columns**

- data\_referencia (Date): Data de referência dos dados.
- titulo (String): Tipo do título (ex.: "NTN-F").
- codigo\_selic (Int64): Código do título no SELIC.
- data\_base (Date): Data base de emissão do título.
- data\_vencimento (Date): Data de vencimento do título.
- dias\_uteis (Int64): Dias úteis entre referência e vencimento.
- prazo\_medio (Float64): Prazo médio do título em dias corridos.
- duration (Float64): Macaulay Duration do título (anos).
- dv01 (Float64): Variação no preço para 1bp de taxa.
- pu (Float64): Preço unitário (PU).
- taxa\_compra (Float64): Taxa de compra (decimal).
- taxa\_venda (Float64): Taxa de venda (decimal).
- taxa\_indicativa (Float64): Taxa indicativa (decimal).
- taxa\_di (Float64): Taxa de ajuste do DI Futuro interpolada pelo método flat forward.
- taxa\_zero (Float64): Taxa zero (zero cupom via bootstrap).
- premio (Float64): prêmio, isto é, o spread sobre o DI.
- premio\_limpo (Float64): prêmio limpo sobre a curva DI.
- rentabilidade (Float64): Rentabilidade da NTN-F sobre a curva DI.

Examples:

```pycon
>>> from pyield import ntnf
>>> df_ntnf = ntnf.dados("23-08-2024")
```

## `datas_pagamento(data_liquidacao, data_vencimento)`

Gera todas as datas de pagamento entre liquidação e vencimento.

As datas são exclusivas para a liquidação e inclusivas para o vencimento. Os pagamentos são semestrais, em 1º de janeiro e 1º de julho. No vencimento, o fluxo inclui o último cupom e a amortização do principal. O título NTN-F é determinado pela data de vencimento. As datas são contratuais e não são ajustadas para dias úteis.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data_liquidacao` | `DateLike` | Data de liquidação. | *required* |
| `data_vencimento` | `DateLike` | Data de vencimento. | *required* |

Returns:

| Type | Description |
| --- | --- |
| `Series` | pl.Series: Série com as datas de pagamento entre a liquidação (exclusiva) e o vencimento (inclusivo). Retorna série vazia se o vencimento for menor ou igual à liquidação. |

**Notes**

Para obter as datas efetivas de processamento, use `yd.du.deslocar(..., 0)`.

Examples:

```pycon
>>> from pyield import ntnf
>>> ntnf.datas_pagamento("15-05-2024", "01-01-2027")
shape: (6,)
Series: 'datas_pagamento' [date]
[
    2024-07-01
    2025-01-01
    2025-07-01
    2026-01-01
    2026-07-01
    2027-01-01
]
```

A data de liquidação coincidente com um pagamento é exclusiva:

```pycon
>>> ntnf.datas_pagamento("01-07-2024", "01-01-2027")
shape: (5,)
Series: 'datas_pagamento' [date]
[
    2025-01-01
    2025-07-01
    2026-01-01
    2026-07-01
    2027-01-01
]
```

## `duration(data_liquidacao, data_vencimento, taxa)`

Calcula a Macaulay duration de uma NTN-F em anos úteis.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data_liquidacao` | `DateLike` | Data de liquidação para o cálculo. | *required* |
| `data_vencimento` | `DateLike` | Data de vencimento do título. | *required* |
| `taxa` | `float \| str` | TIR anual efetiva, em decimal, na base de 252 dias úteis, usada para descontar os fluxos. Aceita também percentual explícito: "5.75%" ou "5,75%". | *required* |

Returns:

| Name | Type | Description |
| --- | --- | --- |
| `float` | `float` | Macaulay duration em anos úteis. Retorna NaN se inválido. |

**Notes**

A Macaulay duration \\(D\\), em anos de 252 dias úteis, é:

\\\[ D = \\frac{ \\sum\_{i=1}^{n}t\_i\\,\\mathrm{FC}\_i/(1+y)^{t\_i}} {\\mathrm{PU}} \\\]

onde:

- \\(\\mathrm{FC}\_i\\): valor do pagamento \\(i\\), incluindo cupom e amortização quando aplicável.
- \\(\\mathrm{DU}\_i\\): dias úteis entre liquidação e pagamento \\(i\\).
- \\(t\_i = \\mathrm{DU}\_i/252\\): prazo até o pagamento \\(i\\), em anos de 252 dias úteis.
- \\(n\\): número de pagamentos restantes.
- \\(y\\): TIR anual efetiva informada em `taxa`, em decimal, na base de 252 dias úteis.
- \\(\\mathrm{PU}\\): soma dos fluxos descontados pela TIR \\(y\\).

Os fluxos descontados não aplicam os arredondamentos e truncamentos do PU oficial.

Examples:

```pycon
>>> from pyield import ntnf
>>> ntnf.duration("02-09-2024", "01-01-2035", "12.1785%")
6.32854218039796
```

## `duration_expr(data_liquidacao, data_vencimento, taxa)`

Cria expressão Polars para a duration da NTN-F.

O cálculo é aplicado linha a linha porque a duration depende dos fluxos de caixa do título.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data_liquidacao` | `Expr \| str` | Nome de coluna ou expressão Polars com a data de liquidação. | *required* |
| `data_vencimento` | `Expr \| str` | Nome de coluna ou expressão Polars com a data de vencimento. | *required* |
| `taxa` | `Expr \| str` | Nome de coluna ou expressão Polars com a TIR anual efetiva, em decimal, na base de 252 dias úteis. | *required* |

Returns:

| Type | Description |
| --- | --- |
| `Expr` | pl.Expr: Expressão sem alias com a Macaulay duration em anos úteis. |

## `dv01(data_liquidacao, data_vencimento, taxa)`

Calcula o DV01 (Dollar Value of 01) de uma NTN-F em R$.

Representa a redução do PU teórico para um aumento de 1 bp (0,01 ponto percentual) na taxa, calculada pela diferença entre os preços antes e depois do aumento.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data_liquidacao` | `DateLike` | Data de liquidação. | *required* |
| `data_vencimento` | `DateLike` | Data de vencimento. | *required* |
| `taxa` | `float \| Decimal \| str` | TIR anual efetiva, em decimal, na base de 252 dias úteis. Aceita também percentual explícito: "5.75%" ou "5,75%". | *required* |

Returns:

| Name | Type | Description |
| --- | --- | --- |
| `float` | `float` | DV01, variação de preço para 1 bp. |

**Notes**

Mantendo liquidação e vencimento fixos, os fluxos e seus prazos também ficam fixos. Nesse contexto, o preço teórico em função da TIR é:

\\\[ \\mathrm{PU}(y) = \\sum\_{i=1}^{n}\\frac{\\mathrm{FC}\_i}{(1+y)^{t\_i}} \\\]

O \\(\\mathrm{DV01}\\) é a redução do preço, em R$, para um aumento de 1 bp na TIR:

\\\[ \\mathrm{DV01} = \\mathrm{PU}(y) - \\mathrm{PU}(y+0.0001) \\\]

onde:

- \\(\\mathrm{FC}\_i\\): valor do pagamento \\(i\\), incluindo cupom e amortização quando aplicável.
- \\(\\mathrm{DU}\_i\\): dias úteis entre liquidação e pagamento \\(i\\).
- \\(t\_i = \\mathrm{DU}\_i/252\\): prazo até o pagamento \\(i\\), em anos de 252 dias úteis.
- \\(n\\): número de pagamentos restantes.
- \\(y\\): TIR anual efetiva informada em `taxa`, em decimal, na base de 252 dias úteis.
- \\(0.0001\\): aumento de 1 bp na TIR em decimal.

Os preços não aplicam os arredondamentos e truncamentos do PU oficial.

Examples:

```pycon
>>> from pyield import ntnf
>>> ntnf.dv01("26-03-2025", "01-01-2035", "15.1375%")
0.39025157587195736
```

## `dv01_expr(data_liquidacao, data_vencimento, taxa)`

Cria expressão Polars para o DV01 da NTN-F.

O cálculo é aplicado linha a linha e reprifica o PU teórico para um aumento de 1 bp na taxa.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data_liquidacao` | `Expr \| str` | Nome de coluna ou expressão Polars com a data de liquidação. | *required* |
| `data_vencimento` | `Expr \| str` | Nome de coluna ou expressão Polars com a data de vencimento. | *required* |
| `taxa` | `Expr \| str` | Nome de coluna ou expressão Polars com a TIR anual efetiva, em decimal, na base de 252 dias úteis. | *required* |

Returns:

| Type | Description |
| --- | --- |
| `Expr` | pl.Expr: Expressão sem alias com o DV01. |

## `fluxos_caixa(data_liquidacao, data_vencimento)`

Gera os fluxos de caixa da NTN-F entre liquidação (exclusiva) e vencimento (inclusivo). Os fluxos incluem cupons e o pagamento final no vencimento.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data_liquidacao` | `DateLike` | Data de liquidação (exclusiva). | *required* |
| `data_vencimento` | `DateLike` | Data de vencimento do título. | *required* |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | pl.DataFrame: DataFrame com as colunas `data_pagamento` e `valor_pagamento`. |

**Output Columns**

- data\_pagamento (Date): Data contratual do pagamento, sem ajuste para dia útil.
- valor\_pagamento (Float64): Valor do pagamento.

**Notes**

Para obter as datas efetivas de processamento, use `yd.du.deslocar(..., 0)`.

Examples:

```pycon
>>> from pyield import ntnf
>>> ntnf.fluxos_caixa("15-05-2024", "01-01-2027")
shape: (6, 2)
┌────────────────┬─────────────────┐
│ data_pagamento ┆ valor_pagamento │
│ ---            ┆ ---             │
│ date           ┆ f64             │
╞════════════════╪═════════════════╡
│ 2024-07-01     ┆ 48.80885        │
│ 2025-01-01     ┆ 48.80885        │
│ 2025-07-01     ┆ 48.80885        │
│ 2026-01-01     ┆ 48.80885        │
│ 2026-07-01     ┆ 48.80885        │
│ 2027-01-01     ┆ 1048.80885      │
└────────────────┴─────────────────┘
```

## `premio_limpo(data_liquidacao, data_vencimento, taxa_ntnf, vencimentos_di, taxas_di)`

Calcula o spread líquido (prêmio limpo no jargão de mercado) da NTN-F sobre a curva DI.

A função determina o spread que iguala o valor presente dos fluxos ao preço do título. Interpola as taxas DI nas datas de pagamento e encontra o spread que zera a diferença de preços, retornado em decimal e conversível para bps.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data_liquidacao` | `DateLike` | Data de liquidação para o cálculo. | *required* |
| `data_vencimento` | `DateLike` | Data de vencimento do título. | *required* |
| `taxa_ntnf` | `float \| str` | TIR anual efetiva, em decimal, na base de 252 dias úteis. Aceita também percentual explícito: "5.75%" ou "5,75%". | *required* |
| `vencimentos_di` | `DatesLike` | Vencimentos da curva DI. | *required* |
| `taxas_di` | `ArrayLike` | Taxas DI anuais efetivas, em decimal, na base de 252 dias úteis, correspondentes aos vencimentos. | *required* |

Returns:

| Name | Type | Description |
| --- | --- | --- |
| `float` | `float` | Spread líquido em formato decimal (ex.: 0.0012 = 12 bps). Retorna NaN para entradas ausentes ou fluxos vazios. Também retorna NaN se a resolução numérica falhar. |

**Notes**

O spread \\(s\\), adicionado a cada taxa DI anual em decimal, é a solução de:

\\\[ \\sum\_{i=1}^{n} \\frac{\\mathrm{FC}\_i}{(1+\\mathrm{DI}\_i+s)^{t\_i}} = \\mathrm{PU} = \\sum\_{i=1}^{n} \\frac{\\mathrm{FC}\_i}{(1+y)^{t\_i}} \\\]

onde:

- \\(\\mathrm{FC}\_i\\): valor do pagamento \\(i\\), incluindo cupom e amortização quando aplicável.
- \\(\\mathrm{DU}\_i\\): dias úteis entre liquidação e pagamento \\(i\\).
- \\(t\_i = \\mathrm{DU}\_i/252\\): prazo até o pagamento \\(i\\), em anos de 252 dias úteis.
- \\(n\\): número de pagamentos restantes.
- \\(y\\): TIR anual efetiva informada em `taxa_ntnf`, em decimal, na base de 252 dias úteis.
- \\(\\mathrm{DI}\_i := \\mathrm{DI}(t\_i)\\): taxa DI anual efetiva, em decimal, para o prazo do pagamento \\(i\\), obtida por interpolação `flat_forward`, na base de 252 dias úteis.

Para expressar o spread em pontos-base, multiplique por 10.000. Os preços são teóricos, sem os arredondamentos e truncamentos do PU oficial.

Examples:

### Obs: apenas algumas taxas DI serão usadas no exemplo.

```pycon
>>> exp_dates = ["2025-01-01", "2030-01-01", "2035-01-01"]
>>> taxas_di = [0.10823, 0.11594, 0.11531]
>>> spread = premio_limpo(
...     data_liquidacao="23-08-2024",
...     data_vencimento="01-01-2035",
...     taxa_ntnf=0.116586,
...     vencimentos_di=exp_dates,
...     taxas_di=taxas_di,
... )
>>> round(spread * 10_000, 2)  # Converte para bps para exibição
12.13
```

## `premio_limpo_expr(data_liquidacao, data_vencimento, taxa_ntnf, vencimentos_di, taxas_di)`

Cria expressão Polars para o prêmio limpo da NTN-F sobre a curva DI.

O cálculo é aplicado linha a linha porque o prêmio limpo depende dos fluxos de caixa do título, da interpolação da curva DI e da resolução de raiz. Falhas de resolução retornam NaN apenas na linha afetada. Outros erros são propagados.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data_liquidacao` | `DateLike` | Data de liquidação para o cálculo. | *required* |
| `data_vencimento` | `Expr \| str` | Nome de coluna ou expressão Polars com a data de vencimento da NTN-F. | *required* |
| `taxa_ntnf` | `Expr \| str` | Nome de coluna ou expressão Polars com a TIR anual efetiva da NTN-F, em decimal, na base de 252 dias úteis. | *required* |
| `vencimentos_di` | `DatesLike` | Datas de vencimento da curva DI. | *required* |
| `taxas_di` | `ArrayLike` | Taxas DI anuais efetivas, em decimal, na base de 252 dias úteis, correspondentes aos vencimentos. | *required* |

Returns:

| Type | Description |
| --- | --- |
| `Expr` | pl.Expr: Expressão sem alias com o prêmio limpo da NTN-F sobre a curva |
| `Expr` | DI. |

## `pu(data_liquidacao, data_vencimento, taxa)`

Calcula o PU da NTN-F pela metodologia da STN para leilões primários.

O preço equivale ao valor presente dos fluxos descontados pela TIR informada.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data_liquidacao` | `DateLike` | Data de liquidação para cálculo do preço. | *required* |
| `data_vencimento` | `DateLike` | Data de vencimento do título. | *required* |
| `taxa` | `float \| Decimal \| str` | TIR anual efetiva, em decimal, na base de 252 dias úteis. Aceita também percentual explícito: "5.75%" ou "5,75%". Antes do cálculo, é truncada em oito casas decimais (seis na forma percentual), descartando as casas excedentes sem arredondar. | *required* |

Returns:

| Name | Type | Description |
| --- | --- | --- |
| `Decimal` | `Decimal` | Preço da NTN-F truncado em seis casas decimais. |

**Notes**

O PU teórico é:

\\\[ \\mathrm{PU} = \\sum\_{i=1}^{n} \\frac{\\mathrm{FC}\_i}{(1+y)^{t\_i}} \\\]

onde:

- \\(\\mathrm{FC}\_i\\): valor do pagamento \\(i\\), incluindo cupom e amortização quando aplicável.
- \\(\\mathrm{DU}\_i\\): dias úteis entre liquidação e pagamento \\(i\\).
- \\(t\_i = \\mathrm{DU}\_i/252\\): prazo até o pagamento \\(i\\), em anos de 252 dias úteis.
- \\(n\\): número de pagamentos restantes.
- \\(y\\): TIR anual efetiva informada em `taxa`, em decimal, na base de 252 dias úteis.

Os fluxos e os prazos são determinados pela liquidação e pelo vencimento. No cálculo oficial, além do truncamento da TIR descrito em `Args`, cada prazo \\(t\_i\\) é truncado em 14 casas decimais, cada fluxo descontado é arredondado em nove casas e a soma é truncada em seis.

**References**

- Secretaria do Tesouro Nacional. Metodologia de Cálculo dos Títulos Públicos Federais Ofertados nos Leilões Primários. https://crdcj.github.io/PYield/referencias/metodologia-calculo-tpf-stn/
- O cupom semestral é 48,81, que representa 10% a.a. com capitalização semestral e arredondamento para 5 casas, conforme a STN.

Examples:

```pycon
>>> from pyield import ntnf
>>> ntnf.pu("05-07-2024", "01-01-2035", "11.921%")
Decimal('895.359254')
>>> ntnf.pu("01-07-2024", "01-01-2027", "10%")
Decimal('999.931303')
```

## `rentabilidade(data_liquidacao, data_vencimento, taxa_ntnf, vencimentos_di, taxas_di)`

Calcula a rentabilidade de uma NTN-F sobre a curva DI.

Compara as taxas diárias equivalentes da TIR da NTN-F e da TIR de referência obtida pela curva DI, conforme o Anexo 2 de *Dívida Pública: a experiência brasileira*, do Tesouro Nacional. Interpola as taxas DI nas datas de pagamento, calcula o valor presente dos fluxos contratuais usando essas taxas e encontra a TIR única que reproduz esse preço de referência.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data_liquidacao` | `DateLike` | Data de liquidação para o cálculo. | *required* |
| `data_vencimento` | `DateLike` | Data de vencimento da NTN-F. | *required* |
| `taxa_ntnf` | `float \| str` | TIR anual efetiva da NTN-F, em formato decimal. Aceita também percentual explícito: "5.75%" ou "5,75%". | *required* |
| `vencimentos_di` | `DatesLike` | Datas de vencimento da curva DI. | *required* |
| `taxas_di` | `ArrayLike` | Taxas DI anuais efetivas, em formato decimal, correspondentes aos vencimentos. | *required* |

Returns:

| Name | Type | Description |
| --- | --- | --- |
| `float` | `float` | Razão entre as taxas diárias equivalentes. Por exemplo, 1.01 representa 101% da taxa diária equivalente de referência DI. Retorna NaN para entradas ausentes, fluxos vazios ou preço calculado NaN. Também retorna NaN se a resolução numérica falhar. |

Examples:

```pycon
>>> # Obs: apenas algumas taxas DI serão usadas no exemplo.
>>> exp_dates = ["2025-01-01", "2030-01-01", "2035-01-01"]
>>> taxas_di = [0.10823, 0.11594, 0.11531]
>>> rentabilidade(
...     data_liquidacao="23-08-2024",
...     data_vencimento="01-01-2035",
...     taxa_ntnf="11.6586%",
...     vencimentos_di=exp_dates,
...     taxas_di=taxas_di,
... )
1.0099602280683393
```

**Notes**

**Preço de referência pela curva DI**

O preço de referência é:

\\\[ P\_{\\mathrm{DI}} = \\sum\_{i=1}^{n} \\frac{\\mathrm{FC}\_i}{(1+\\mathrm{DI}\_i)^{t\_i}} \\\]

onde:

- \\(\\mathrm{FC}\_i\\): valor do pagamento \\(i\\), incluindo cupom e amortização quando aplicável.
- \\(\\mathrm{DU}\_i\\): dias úteis entre liquidação e pagamento \\(i\\).
- \\(t\_i = \\mathrm{DU}\_i/252\\): prazo até o pagamento \\(i\\), em anos de 252 dias úteis.
- \\(n\\): número de pagamentos restantes.
- \\(\\mathrm{DI}(t)\\): taxa DI para o prazo \\(t\\), anual efetiva e em decimal, na base de 252 dias úteis.
- \\(\\mathrm{DI}\_i := \\mathrm{DI}(t\_i)\\): taxa DI para o prazo do pagamento \\(i\\), obtida por interpolação `flat_forward`.

**TIR equivalente da referência**

A função resolve numericamente a TIR anual efetiva \\(y\_{\\mathrm{DI}}\\), em decimal, que desconta os mesmos fluxos ao preço de referência:

\\\[ \\sum\_{i=1}^{n} \\frac{\\mathrm{FC}\_i}{(1+y\_{\\mathrm{DI}})^{t\_i}} = P\_{\\mathrm{DI}} \\\]

Essa TIR é uma taxa única para todos os pagamentos, enquanto cada \\(\\mathrm{DI}\_i\\) corresponde ao prazo do pagamento \\(i\\).

**Rentabilidade relativa**

A rentabilidade relativa \\(q\\) é a razão entre as taxas diárias equivalentes:

\\\[ q = \\frac{(1+y\_{\\mathrm{NTNF}})^{1/252}-1} {(1+y\_{\\mathrm{DI}})^{1/252}-1} \\\]

onde \\(y\_{\\mathrm{NTNF}}\\) é a TIR informada em `taxa_ntnf`.

O indicador compara taxas implícitas; não representa o retorno realizado entre compra e venda. Também não calibra um spread ou um multiplicador uniforme sobre os vértices da curva DI.

Os fluxos mantêm as datas contratuais, excluem pagamentos na liquidação e incluem o vencimento. O preço de referência não aplica os arredondamentos e truncamentos do PU oficial. Não há extrapolação na ponta longa da curva; pagamentos além do último vértice DI resultam em NaN.

> [!NOTE]
>
> **Comparação com a LTN**
>
> A LTN possui um único pagamento. Nesse caso, a TIR equivalente da referência coincide com a taxa zero DI do vencimento, e a fórmula se reduz à utilizada por `yd.ltn.rentabilidade`. Na NTN-F, a TIR equivalente considera também os cupons intermediários.

Referência: Tesouro Nacional, *Dívida Pública: a experiência brasileira*, Parte 3, capítulo 2, Anexo 2, item 2, páginas 329–330: [Prêmio das NTN-Fs](<https://thot-arquivos.tesouro.gov.br/publicacao-anexo/4710>).

## `rentabilidade_expr(data_liquidacao, data_vencimento, taxa_ntnf, vencimentos_di, taxas_di)`

Cria expressão Polars para a rentabilidade da NTN-F sobre a curva DI.

O cálculo é aplicado linha a linha porque a rentabilidade depende dos fluxos de caixa do título, da interpolação da curva DI e da resolução de raiz. Falhas de resolução retornam NaN apenas na linha afetada. Outros erros são propagados.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data_liquidacao` | `DateLike` | Data de liquidação para o cálculo. | *required* |
| `data_vencimento` | `Expr \| str` | Nome de coluna ou expressão Polars com a data de vencimento da NTN-F. | *required* |
| `taxa_ntnf` | `Expr \| str` | Nome de coluna ou expressão Polars com a TIR anual efetiva da NTN-F, em decimal, na base de 252 dias úteis. | *required* |
| `vencimentos_di` | `DatesLike` | Datas de vencimento da curva DI. | *required* |
| `taxas_di` | `ArrayLike` | Taxas DI anuais efetivas, em decimal, na base de 252 dias úteis, correspondentes aos vencimentos. | *required* |

Returns:

| Type | Description |
| --- | --- |
| `Expr` | pl.Expr: Expressão sem alias com a rentabilidade da NTN-F sobre a curva |
| `Expr` | DI. |

## `taxa(data_liquidacao, data_vencimento, pu)`

Calcula a TIR implícita de uma NTN-F a partir de um PU informado.

A função resolve numericamente a taxa que aproxima o PU informado pelo valor presente bruto dos fluxos. Como o valor presente não é arredondado nem truncado, o resultado não pretende reproduzir exatamente a taxa usada para gerar um PU canônico por `pu()`.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data_liquidacao` | `DateLike` | Data de liquidação. | *required* |
| `data_vencimento` | `DateLike` | Data de vencimento. | *required* |
| `pu` | `float \| Decimal` | Preço unitário (PU) do título. | *required* |

Returns:

| Name | Type | Description |
| --- | --- | --- |
| `float` | `float` | TIR implícita em formato decimal, sem arredondamento. Retorna NaN para entradas ausentes, PU não positivo ou falha na resolução numérica. |

**Notes**

A TIR implícita \\(y\\), anual efetiva, em decimal e na base de 252 dias úteis, é a solução de:

\\\[ \\sum\_{i=1}^{n} \\frac{\\mathrm{FC}\_i}{(1+y)^{t\_i}} = \\mathrm{PU}\_{\\mathrm{informado}} \\\]

onde:

- \\(\\mathrm{FC}\_i\\): valor do pagamento \\(i\\), incluindo cupom e amortização quando aplicável.
- \\(\\mathrm{DU}\_i\\): dias úteis entre liquidação e pagamento \\(i\\).
- \\(t\_i = \\mathrm{DU}\_i/252\\): prazo até o pagamento \\(i\\), em anos de 252 dias úteis.
- \\(n\\): número de pagamentos restantes.
- \\(\\mathrm{PU}\_{\\mathrm{informado}}\\): preço recebido em `pu`.

A soma representa o preço teórico, sem os arredondamentos e truncamentos do PU oficial.

Examples:

Exibe as taxas em formato decimal:

```pycon
>>> from pyield import ntnf
>>> ntnf.taxa("13-03-2026", "01-01-2035", 820.995125)
0.1427430001649857
>>> ntnf.taxa("21-05-2008", "01-01-2014", 903.039091) * 100
13.661101023197176
```

## `taxas_zero(data_liquidacao, vencimentos_ltn, taxas_ltn, vencimentos_ntnf, taxas_ntnf, incluir_cupons=False)`

Calcula as taxas spot (zero cupom) para NTN-F usando bootstrap.

O bootstrap determina as taxas spot a partir das TIRs dos títulos. O método resolve iterativamente as taxas que descontam os fluxos ao preço. Usa as LTNs (zero cupom) até o último vencimento LTN disponível. Após isso, calcula as taxas spot a partir das NTN-F.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data_liquidacao` | `DateLike` | Data de liquidação para o cálculo. | *required* |
| `vencimentos_ltn` | `DatesLike` | Datas de vencimento das LTNs usadas como vértices prefixados zero cupom. | *required* |
| `taxas_ltn` | `ArrayLike` | Taxas das LTNs. Como a LTN é zero cupom, essas taxas são usadas diretamente como taxas zero no bootstrap. | *required* |
| `vencimentos_ntnf` | `DatesLike` | Datas de vencimento das NTN-F usadas no bootstrap. | *required* |
| `taxas_ntnf` | `ArrayLike` | TIRs das NTN-F correspondentes aos vencimentos informados. | *required* |
| `incluir_cupons` | `bool` | Se True, inclui as datas de cupom (julho). Padrão False. | `False` |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | pl.DataFrame: DataFrame com colunas `data_vencimento`, `dias_uteis` e `taxa_zero`. |

**Output Columns**

- data\_vencimento (Date): Data de vencimento.
- dias\_uteis (Int64): Dias úteis entre liquidação e vencimento.
- taxa\_zero (Float64): Taxa zero (zero cupom), em formato decimal.

**Notes**

Para cada NTN-F considerada no bootstrap, o preço teórico é:

\\\[ \\mathrm{PU} = \\sum\_{i=1}^{n} \\frac{\\mathrm{FC}\_i}{(1+y)^{t\_i}} \\\]

onde:

- \\(\\mathrm{FC}\_i\\): valor do pagamento \\(i\\), incluindo cupom e amortização quando aplicável.
- \\(\\mathrm{DU}\_i\\): dias úteis entre liquidação e pagamento \\(i\\).
- \\(t\_i = \\mathrm{DU}\_i/252\\): prazo até o pagamento \\(i\\), em anos de 252 dias úteis.
- \\(n\\): número de pagamentos restantes.
- \\(y\\): TIR obtida por interpolação `flat_forward` das TIRs informadas para NTN-F.

Após o último vencimento LTN, o bootstrap encontra a taxa zero do pagamento final usando as taxas já determinadas para os pagamentos anteriores:

\\\[ r\_n = \\left( \\frac{\\mathrm{FC}\_n} {\\mathrm{PU}-\\sum\_{i=1}^{n-1}\\mathrm{FC}\_i/(1+r\_i)^{t\_i}} \\right)^{1/t\_n}-1 \\\]

onde \\(r\_i := r(t\_i)\\) é a taxa zero para o prazo do pagamento \\(i\\).

As taxas de entrada e de saída são anuais efetivas, em decimal, na base de 252 dias úteis. Para a LTN, a TIR coincide com a taxa zero, pois há apenas um pagamento. O preço teórico não aplica os arredondamentos e truncamentos do PU oficial.

Vencimentos menores ou iguais à liquidação são ignorados antes da construção dos interpoladores.

Examples:

```pycon
>>> from pyield import ntnf, ltn
>>> import polars.selectors as cs
>>> df_ltn = ltn.dados("03-09-2024")
>>> df_ntnf = ntnf.dados("03-09-2024")
>>> curva = ntnf.taxas_zero(
...     data_liquidacao="03-09-2024",
...     vencimentos_ltn=df_ltn["data_vencimento"],
...     taxas_ltn=df_ltn["taxa_indicativa"],
...     vencimentos_ntnf=df_ntnf["data_vencimento"],
...     taxas_ntnf=df_ntnf["taxa_indicativa"],
... )
>>> curva.with_columns(cs.starts_with("taxa_") * 100)
shape: (6, 3)
┌─────────────────┬────────────┬───────────┐
│ data_vencimento ┆ dias_uteis ┆ taxa_zero │
│ ---             ┆ ---        ┆ ---       │
│ date            ┆ i64        ┆ f64       │
╞═════════════════╪════════════╪═══════════╡
│ 2025-01-01      ┆ 83         ┆ 10.8837   │
│ 2027-01-01      ┆ 584        ┆ 11.998105 │
│ 2029-01-01      ┆ 1083       ┆ 12.211302 │
│ 2031-01-01      ┆ 1584       ┆ 12.223069 │
│ 2033-01-01      ┆ 2088       ┆ 12.135544 │
│ 2035-01-01      ┆ 2587       ┆ 12.139796 │
└─────────────────┴────────────┴───────────┘
```

## `taxas_zero_forwards(data_liquidacao, vencimentos_ltn, taxas_ltn, vencimentos_ntnf, taxas_ntnf, incluir_cupons=False)`

Calcula uma curva zero experimental conjunta de LTN e NTN-F.

Usa forwards constantes entre vencimentos, em base 252 dias úteis. Em vencimentos coincidentes, a LTN tem prioridade. Nos demais, a NTN-F determina o forward desde o vértice anterior, reproduzindo seu preço teórico. As taxas podem vir da ANBIMA ou ser fornecidas pelo usuário; não há consulta à rede. Este método é uma alternativa experimental a `taxas_zero`.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data_liquidacao` | `DateLike` | Data de liquidação. | *required* |
| `vencimentos_ltn` | `DatesLike` | Vencimentos das LTNs; aceita coleção vazia. | *required* |
| `taxas_ltn` | `ArrayLike` | Taxas zero das LTNs em formato decimal. | *required* |
| `vencimentos_ntnf` | `DatesLike` | Vencimentos das NTN-F; aceita coleção vazia. | *required* |
| `taxas_ntnf` | `ArrayLike` | TIRs das NTN-F em formato decimal. | *required* |
| `incluir_cupons` | `bool` | Inclui também as datas dos cupons das NTN-F selecionadas. | `False` |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | pl.DataFrame: União dos vencimentos futuros de LTN e NTN-F, ordenada. Retorna vazio com esquema quando não há títulos futuros. |

**Output Columns**

- data\_vencimento (Date): Vértice da curva prefixada conjunta.
- dias\_uteis (Int64): Dias úteis desde a liquidação.
- taxa\_zero (Float64): Taxa zero nominal anual em formato decimal.

Raises:

| Type | Description |
| --- | --- |
| `ValueError` | Entradas inválidas, vencimentos duplicados na mesma família, prazos úteis coincidentes ou preço incompatível com a curva anterior. |
| `RuntimeError` | Falha na calibração numérica. |

**Notes**

A prioridade da LTN vale apenas no mesmo vencimento, não em todo o intervalo até a última LTN. NTN-F entre duas LTNs participa da calibração. Antes do primeiro vértice, a taxa zero é constante. Use todos os vértices retornados com `Interpolador(..., metodo="flat_forward")` para preservar os descontos dos cupons. O retorno inclui LTNs mesmo sem NTN-F nessa data, diferentemente do método antigo.

A calibração usa os fluxos de `ntnf.fluxos_caixa`, DU/252 sem truncamento e valores presentes sem arredondamento, como no bootstrap de NTN-B. Portanto, o preço-alvo pode diferir ligeiramente de `ntnf.pu`, que aplica arredondamentos e truncamentos. As datas contratuais seguem a convenção de `ntnf.fluxos_caixa`. Vencimentos até a liquidação são ignorados.

## `vencimentos(data)`

Busca os vencimentos de NTN-F disponíveis para a data de referência.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data` | `DateLike` | Data da consulta. | *required* |

Returns:

| Type | Description |
| --- | --- |
| `Series` | pl.Series: Série de datas de vencimento de NTN-F. |

Examples:

```pycon
>>> from pyield import ntnf
>>> ntnf.vencimentos("23-08-2024")
shape: (6,)
Series: 'data_vencimento' [date]
[
    2025-01-01
    2027-01-01
    2029-01-01
    2031-01-01
    2033-01-01
    2035-01-01
]
```
