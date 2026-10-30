Cálculos e dados de LFT.

Convenções de precificação (STN, tabela 3): - Cotação em base 100, truncada a 4 casas. - VNA recebido no cálculo do PU: truncado a 6 casas. - Prazo de desconto: dias úteis / 252, truncado a 14 casas. - PU: truncado a 6 casas.

## `cotacao(data_liquidacao, data_vencimento, taxa)`

Calcula a cotação de uma LFT pela metodologia da STN para leilões primários.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data_liquidacao` | `DateLike` | Data de liquidação do título. | *required* |
| `data_vencimento` | `DateLike` | Data de vencimento do título. | *required* |
| `taxa` | `float \| Decimal \| str` | Taxa de ágio/deságio da LFT sobre a Selic, anual efetiva, em decimal, na base de 252 dias úteis. Aceita também percentual explícito: "5.75%" ou "5,75%". Antes do cálculo, é truncada em oito casas decimais (seis na forma percentual), descartando as casas excedentes sem arredondar. | *required* |

Returns:

| Name | Type | Description |
| --- | --- | --- |
| `Decimal` | `Decimal` | Cotação em base 100, truncada em 4 casas decimais. Retorna `Decimal("NaN")` para entradas nulas ou quando o prazo até o vencimento não é positivo. |

**Notes**

A cotação teórica em base 100 é:

\\\[ \\mathrm{COT} = \\frac{100}{(1+s)^t} \\\]

onde:

- \\(\\mathrm{DU}\\): dias úteis entre liquidação e vencimento.
- \\(t = \\mathrm{DU}/252\\): prazo até o vencimento, em anos de 252 dias úteis.
- \\(s\\): taxa de ágio/deságio informada em `taxa`, anual efetiva, em decimal, na base de 252 dias úteis. Taxa positiva corresponde a deságio; negativa, a ágio; zero, a cotação ao par.

No cálculo oficial, além do truncamento da taxa descrito em `Args`, o prazo é truncado em 14 casas decimais.

A cotação é calculada e retornada na escala percentual (base 100), truncada em 4 casas conforme a STN. Por exemplo, `99.3651` representa 99,3651% do VNA; o PU é calculado como `VNA * cotacao / 100`.

Examples:

Calcula a cotação de uma LFT com taxa de 0,1717%:

```pycon
>>> from pyield import lft
>>> lft.cotacao(
...     data_liquidacao="24-07-2024",
...     data_vencimento="01-09-2030",
...     taxa="0.1717%",
... )
Decimal('98.9645')
```

## `dados(data)`

Busca as taxas indicativas de LFT para a data de referência na ANBIMA.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data` | `DateLike` | Data da consulta. | *required* |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | pl.DataFrame: DataFrame Polars com os dados de LFT. Na ausência de dados, retorna vazio com as mesmas colunas e tipos. |

**Output Columns**

- data\_referencia (Date): Data de referência dos dados.
- titulo (String): Tipo do título (ex.: "LFT").
- codigo\_selic (Int64): Código do título no SELIC.
- data\_base (Date): Data base de emissão do título.
- data\_vencimento (Date): Data de vencimento do título.
- dias\_uteis (Int64): Dias úteis entre referência e vencimento.
- prazo\_medio (Float64): Prazo médio do título em anos.
- pu (Float64): Preço unitário (PU).
- taxa\_compra (Float64): Taxa de compra (decimal).
- taxa\_venda (Float64): Taxa de venda (decimal).
- taxa\_indicativa (Float64): Taxa indicativa (decimal).
- taxa\_di (Float64): Taxa de ajuste do DI Futuro interpolada pelo método flat forward.
- rentabilidade (Float64): Razão entre a taxa diária equivalente da LFT, usando o DI como referência para a Selic, e a taxa diária DI.

Examples:

```pycon
>>> from pyield import lft
>>> df_lft = lft.dados("23-08-2024")
```

## `pu(vna, cotacao)`

Calcula o PU da LFT pela metodologia da STN para leilões primários.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `vna` | `float \| Decimal` | Valor nominal atualizado (VNA), truncado em seis casas decimais antes do cálculo, sem arredondar. | *required* |
| `cotacao` | `float \| Decimal` | Cotação da LFT em base 100. Truncada em quatro casas decimais antes do cálculo, sem arredondar. | *required* |

Returns:

| Name | Type | Description |
| --- | --- | --- |
| `Decimal` | `Decimal` | Preço da LFT truncado em 6 casas decimais. |

**Notes**

O preço unitário é:

\\\[ \\mathrm{PU} = \\mathrm{VNA}\\,\\frac{\\mathrm{COT}}{100} \\\]

onde:

- \\(\\mathrm{VNA}\\): valor nominal atualizado recebido em `vna`.
- \\(\\mathrm{COT}\\): cotação recebida em `cotacao`, em base 100.

A cotação representa o percentual do VNA usado no preço. O cálculo aplica os truncamentos de VNA e cotação descritos em `Args` e trunca o PU em seis casas decimais.

**References**

- Secretaria do Tesouro Nacional. Metodologia de Cálculo dos Títulos Públicos Federais Ofertados nos Leilões Primários. https://crdcj.github.io/PYield/referencias/metodologia-calculo-tpf-stn/

Examples:

```pycon
>>> from pyield import lft
>>> lft.pu(15785.324502, 99.9291)
Decimal('15774.132706')
```

## `rentabilidade(taxa_lft, taxa_di)`

Calcula a rentabilidade da LFT sobre a taxa de DI Futuro.

Segue a metodologia do Anexo 2, item 3, de *Dívida Pública: a experiência brasileira*, do Tesouro Nacional, que denomina esse indicador prêmio da LFT.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `taxa_lft` | `float \| str` | Taxa de ágio/deságio da LFT sobre a Selic, anual efetiva, em decimal, na base de 252 dias úteis. Aceita também percentual explícito: "5.75%" ou "5,75%". | *required* |
| `taxa_di` | `float \| str` | Taxa DI anual efetiva, em decimal, na base de 252 dias úteis, para o mesmo vencimento da LFT (interpolada quando necessário). Aceita também percentual explícito: "5.75%" ou "5,75%". | *required* |

Returns:

| Name | Type | Description |
| --- | --- | --- |
| `float` | `float` | Razão entre a taxa diária equivalente da LFT, usando o DI como referência para a Selic, e a taxa diária DI. Por exemplo, 1.01 representa 101% da taxa diária equivalente DI. |

**Notes**

A rentabilidade relativa \\(q\\) combina o fator diário da taxa da LFT com o fator diário DI e compara a taxa resultante com a taxa DI:

\\\[ q = \\frac{(1+s)^{1/252}(1+\\mathrm{DI})^{1/252}-1} {(1+\\mathrm{DI})^{1/252}-1} \\\]

onde:

- \\(s\\): taxa de ágio/deságio informada em `taxa_lft`.
- \\(\\mathrm{DI}\\): taxa DI informada em `taxa_di`, para o mesmo vencimento da LFT.

O cálculo usa o DI como referência para a atualização pela Selic. A composição é multiplicativa entre fatores; não soma a taxa da LFT à taxa DI. O indicador não representa o retorno realizado entre compra e venda.

A função retorna a razão \\(q\\). O livro apresenta o indicador em percentual, correspondente a \\(100q\\).

Referência: Tesouro Nacional, *Dívida Pública: a experiência brasileira*, Parte 3, capítulo 2, Anexo 2, item 3, páginas 330–331: [Prêmio das LFTs](<https://thot-arquivos.tesouro.gov.br/publicacao-anexo/4710>).

Examples:

Calcula a rentabilidade de uma LFT em 28/04/2025:

```pycon
>>> from pyield import lft
>>> taxa_lft = "0.1124%"
>>> taxa_di = "13.967670224373396%"
>>> lft.rentabilidade(taxa_lft, taxa_di)
1.008594331960501
```

## `rentabilidade_expr(taxa_lft, taxa_di)`

Cria expressão Polars para a rentabilidade da LFT sobre o DI.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `taxa_lft` | `Expr \| str` | Nome de coluna ou expressão Polars com a taxa de ágio/deságio da LFT sobre a Selic, anual efetiva, em decimal, na base de 252 dias úteis. | *required* |
| `taxa_di` | `Expr \| str` | Nome de coluna ou expressão Polars com a taxa DI anual efetiva, em decimal, na base de 252 dias úteis, para o mesmo vencimento da LFT (interpolada quando necessário). | *required* |

Returns:

| Type | Description |
| --- | --- |
| `Expr` | pl.Expr: Expressão sem alias com a rentabilidade da LFT sobre o DI. |

## `taxa(data_liquidacao, data_vencimento, vna, pu)`

Calcula a taxa implícita de uma LFT a partir do preço (PU).

A função resolve numericamente a taxa que aproxima o PU informado por seu valor teórico, sem arredondamento nem truncamento intermediário. O resultado não pretende reproduzir exatamente a taxa usada para gerar um PU canônico por `pu(vna, cotacao(...))`.

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
| `float` | `float` | Taxa implícita em formato decimal, sem arredondamento. Retorna NaN para entradas ausentes, PU não positivo, prazo útil não positivo ou falha na resolução numérica. |

**Notes**

A taxa de ágio/deságio \\(s\\), anual efetiva, em decimal e na base de 252 dias úteis, é encontrada resolvendo:

\\\[ \\frac{\\mathrm{VNA}}{(1+s)^t} = \\mathrm{PU} \\\]

onde:

- \\(\\mathrm{VNA}\\): valor nominal atualizado recebido em `vna`.
- \\(\\mathrm{PU}\\): preço recebido em `pu`.
- \\(\\mathrm{DU}\\): dias úteis entre liquidação e vencimento.
- \\(t = \\mathrm{DU}/252\\): prazo até o vencimento, em anos de 252 dias úteis.

Essa taxa é aplicada sobre a atualização pela Selic, representada pelo VNA; não é a taxa de retorno total do título. Taxa positiva corresponde a deságio, negativa a ágio e zero a preço igual ao VNA.

Examples:

Exibe as taxas em formato decimal:

```pycon
>>> from pyield import lft
>>> lft.taxa("24-07-2024", "01-09-2030", 15785.324502, 15621.867466)
0.0017170148895820606
>>> lft.taxa("24-07-2024", "01-03-2025", 15785.324502, 15774.132706) * 100
0.11612671421607959
>>> lft.taxa("21-05-2008", "07-03-2014", 3451.215345, 3426.649594) * 100
0.12345862697111447
```

## `vencimentos(data)`

Busca os vencimentos disponíveis para a data de referência.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data` | `DateLike` | Data da consulta. | *required* |

Returns:

| Type | Description |
| --- | --- |
| `Series` | pl.Series: Série de datas de vencimento disponíveis. |

Examples:

```pycon
>>> from pyield import lft
>>> lft.vencimentos("22-08-2024")
shape: (14,)
Series: 'data_vencimento' [date]
[
    2024-09-01
    2025-03-01
    2025-09-01
    2026-03-01
    2026-09-01
    …
    2029-03-01
    2029-09-01
    2030-03-01
    2030-06-01
    2030-09-01
]
```
