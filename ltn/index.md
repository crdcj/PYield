Cálculos e dados de LTN.

Convenções de precificação (STN, tabela 3): - Valor de face: 1000 reais. - Prazo de desconto: dias úteis / 252, truncado a 14 casas. - PU: truncado a 6 casas.

## `dados(data)`

Busca as taxas indicativas de LTN na ANBIMA para a data de referência.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data` | `DateLike` | Data da consulta. | *required* |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | pl.DataFrame: DataFrame Polars com os dados de LTN. Na ausência de dados, retorna vazio com as mesmas colunas e tipos. |

**Output Columns**

- data\_referencia (Date): Data de referência dos dados.
- titulo (String): Tipo do título (ex.: "LTN").
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
- premio (Float64): prêmio sobre o DI, isto é, o spread sobre a taxa DI.
- rentabilidade (Float64): Razão entre as taxas diárias equivalentes da LTN e do DI.

Examples:

```pycon
>>> from pyield import ltn
>>> df_ltn = ltn.dados("23-08-2024")
```

## `duration_expr(data_liquidacao, data_vencimento)`

Cria expressão Polars para a duration da LTN em anos úteis.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data_liquidacao` | `Expr \| str` | Nome de coluna ou expressão Polars com a data de liquidação. | *required* |
| `data_vencimento` | `Expr \| str` | Nome de coluna ou expressão Polars com a data de vencimento. | *required* |

Returns:

| Type | Description |
| --- | --- |
| `Expr` | pl.Expr: Expressão sem alias com a duration em anos úteis. |

**Notes**

Como a LTN possui um único pagamento, a Macaulay duration \\(D\\) coincide com o prazo até o vencimento:

\\\[ D = t = \\frac{\\mathrm{DU}}{252} \\\]

onde \\(\\mathrm{DU}\\) é o número de dias úteis entre liquidação e vencimento. O resultado é expresso em anos de 252 dias úteis.

## `dv01(data_liquidacao, data_vencimento, taxa)`

Calcula o DV01 (Dollar Value of 01) da LTN em R$.

Representa a redução do PU teórico para um aumento de 1 bp (0,01 ponto percentual) na taxa, calculada pela diferença entre os preços antes e depois do aumento.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data_liquidacao` | `DateLike` | Data de liquidação. | *required* |
| `data_vencimento` | `DateLike` | Data de vencimento. | *required* |
| `taxa` | `float \| Decimal \| str` | Taxa anual efetiva, em decimal, na base de 252 dias úteis. Aceita também percentual explícito: "5.75%" ou "5,75%". | *required* |

Returns:

| Name | Type | Description |
| --- | --- | --- |
| `float` | `float` | DV01, variação de preço para 1 bp. Retorna `NaN` quando o prazo até o vencimento não é positivo. |

**Notes**

Mantendo liquidação e vencimento fixos, o pagamento e seu prazo também ficam fixos. Nesse contexto, o preço teórico em função da taxa é:

\\\[ \\mathrm{PU}(y) = \\frac{\\mathrm{FC}}{(1+y)^t} \\\]

O \\(\\mathrm{DV01}\\) é a redução do preço, em R$, para um aumento de 1 bp na taxa:

\\\[ \\mathrm{DV01} = \\mathrm{PU}(y)-\\mathrm{PU}(y+0.0001) \\\]

onde:

- \\(\\mathrm{FC}\\): pagamento único no vencimento, de R$ 1.000.
- \\(\\mathrm{DU}\\): dias úteis entre liquidação e vencimento.
- \\(t = \\mathrm{DU}/252\\): prazo até o vencimento, em anos de 252 dias úteis.
- \\(y\\): taxa anual efetiva informada em `taxa`, em decimal, na base de 252 dias úteis.
- \\(0.0001\\): aumento de 1 bp na taxa em decimal.

Os preços não aplicam os truncamentos do PU oficial.

Examples:

```pycon
>>> from pyield import ltn
>>> ltn.dv01("26-03-2025", "01-01-2032", "15.097%")
0.2269055067940826
```

## `dv01_expr(data_liquidacao, data_vencimento, taxa)`

Cria expressão Polars para o DV01 da LTN.

O cálculo é aplicado linha a linha e reprifica o PU teórico para um aumento de 1 bp na taxa.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data_liquidacao` | `Expr \| str` | Nome de coluna ou expressão Polars com a data de liquidação. | *required* |
| `data_vencimento` | `Expr \| str` | Nome de coluna ou expressão Polars com a data de vencimento. | *required* |
| `taxa` | `Expr \| str` | Nome de coluna ou expressão Polars com a taxa anual efetiva, em decimal, na base de 252 dias úteis. | *required* |

Returns:

| Type | Description |
| --- | --- |
| `Expr` | pl.Expr: Expressão sem alias com o DV01. |

## `pu(data_liquidacao, data_vencimento, taxa)`

Calcula o PU da LTN pela metodologia da STN para leilões primários.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data_liquidacao` | `DateLike` | Data de liquidação. | *required* |
| `data_vencimento` | `DateLike` | Data de vencimento. | *required* |
| `taxa` | `float \| Decimal \| str` | Taxa anual efetiva, em decimal, na base de 252 dias úteis. Aceita também percentual explícito: "5.75%" ou "5,75%". Antes do cálculo, é truncada em oito casas decimais (seis na forma percentual), descartando as casas excedentes sem arredondar. | *required* |

Returns:

| Name | Type | Description |
| --- | --- | --- |
| `Decimal` | `Decimal` | PU da LTN truncado em seis casas decimais. Retorna `Decimal("NaN")` quando o prazo até o vencimento não é positivo. |

**Notes**

O PU teórico é:

\\\[ \\mathrm{PU} = \\frac{\\mathrm{FC}}{(1+y)^t} \\\]

onde:

- \\(\\mathrm{FC}\\): pagamento único no vencimento, de R$ 1.000.
- \\(\\mathrm{DU}\\): dias úteis entre liquidação e vencimento.
- \\(t = \\mathrm{DU}/252\\): prazo até o vencimento, em anos de 252 dias úteis.
- \\(y\\): taxa anual efetiva informada em `taxa`, em decimal, na base de 252 dias úteis.

Como a LTN possui um único pagamento, sua taxa anual efetiva coincide com a taxa zero e com a TIR para o prazo do título. O fluxo e o prazo são determinados pelo título e pela liquidação. No cálculo oficial, além do truncamento da taxa descrito em `Args`, o prazo é truncado em 14 casas decimais e o PU é truncado em seis.

**References**

- Secretaria do Tesouro Nacional. Metodologia de Cálculo dos Títulos Públicos Federais Ofertados nos Leilões Primários. https://crdcj.github.io/PYield/referencias/metodologia-calculo-tpf-stn/

Examples:

```pycon
>>> from pyield import ltn
>>> ltn.pu("05-07-2024", "01-01-2030", "12.145%")
Decimal('535.279902')
>>> ltn.pu("21-05-2008", "01-07-2010", "14.36%")
Decimal('753.315323')
```

## `rentabilidade(taxa_ltn, taxa_di)`

Calcula a rentabilidade da LTN sobre a taxa de DI Futuro.

Segue a metodologia do Anexo 2, item 1, de *Dívida Pública: a experiência brasileira*, do Tesouro Nacional, que denomina esse indicador prêmio da LTN.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `taxa_ltn` | `float \| str` | Taxa anual efetiva da LTN, em decimal, na base de 252 dias úteis. Aceita também percentual explícito: "5.75%" ou "5,75%". | *required* |
| `taxa_di` | `float \| str` | Taxa DI anual efetiva, em decimal, na base de 252 dias úteis, para o mesmo prazo da LTN. Aceita também percentual explícito: "5.75%" ou "5,75%". | *required* |

Returns:

| Name | Type | Description |
| --- | --- | --- |
| `float` | `float` | Razão entre as taxas diárias equivalentes da LTN e do DI. Por exemplo, 1.01 representa 101% da taxa diária equivalente DI. |

**Notes**

A rentabilidade relativa \\(q\\) é a razão entre as taxas diárias equivalentes:

\\\[ q = \\frac{(1+y\_{\\mathrm{LTN}})^{1/252}-1} {(1+\\mathrm{DI})^{1/252}-1} \\\]

onde:

- \\(y\_{\\mathrm{LTN}}\\): taxa informada em `taxa_ltn`.
- \\(\\mathrm{DI}\\): taxa DI informada em `taxa_di`, para o mesmo prazo da LTN.

Como a LTN possui um único pagamento, a TIR equivalente da referência coincide com a taxa DI para o prazo do título. O indicador compara taxas implícitas; não representa o retorno realizado entre compra e venda.

A função retorna a razão \\(q\\). O livro apresenta o indicador em percentual, correspondente a \\(100q\\).

Referência: Tesouro Nacional, *Dívida Pública: a experiência brasileira*, Parte 3, capítulo 2, Anexo 2, item 1, páginas 328–329: [Prêmio das LTNs](<https://thot-arquivos.tesouro.gov.br/publicacao-anexo/4710>).

Examples:

Data de referência: 22-08-2024. Taxa da LTN com vencimento em 01-01-2030: 0.118746. Taxa de ajuste do DI (JAN30): 0.11725.

```pycon
>>> from pyield import ltn
>>> ltn.rentabilidade("11.8746%", "11.725%")
1.0120718007994287
```

## `rentabilidade_expr(taxa_ltn, taxa_di)`

Cria expressão Polars para a rentabilidade da LTN sobre o DI.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `taxa_ltn` | `Expr \| str` | Nome de coluna ou expressão Polars com a taxa anual efetiva da LTN, em decimal, na base de 252 dias úteis. | *required* |
| `taxa_di` | `Expr \| str` | Nome de coluna ou expressão Polars com a taxa DI anual efetiva, em decimal, na base de 252 dias úteis, para o mesmo prazo da LTN. | *required* |

Returns:

| Type | Description |
| --- | --- |
| `Expr` | pl.Expr: Expressão sem alias com a rentabilidade da LTN sobre o DI. |

## `taxa(data_liquidacao, data_vencimento, preco_unitario)`

Calcula a taxa implícita da LTN a partir do preço unitário (PU).

Inverte algebricamente o valor presente, sem truncamentos da STN:

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data_liquidacao` | `DateLike` | Data de liquidação. | *required* |
| `data_vencimento` | `DateLike` | Data de vencimento. | *required* |
| `preco_unitario` | `float \| Decimal` | PU do título. | *required* |

Returns:

| Name | Type | Description |
| --- | --- | --- |
| `float` | `float` | Taxa implícita em formato decimal, sem arredondamento. Retorna NaN para entradas ausentes, PU não positivo ou prazo útil não positivo. |

**Notes**

A taxa implícita \\(y\\), anual efetiva, em decimal e na base de 252 dias úteis, é:

\\\[ y = \\left(\\frac{\\mathrm{FC}}{\\mathrm{PU}}\\right)^{1/t}-1 \\\]

onde:

- \\(\\mathrm{FC}\\): pagamento único no vencimento, de R$ 1.000.
- \\(\\mathrm{PU}\\): preço recebido em `preco_unitario`.
- \\(\\mathrm{DU}\\): dias úteis entre liquidação e vencimento.
- \\(t = \\mathrm{DU}/252\\): prazo até o vencimento, em anos de 252 dias úteis.

Como há apenas um pagamento, a taxa anual efetiva coincide com a taxa zero e com a TIR para o prazo do título. A inversão usa o preço e o prazo sem os truncamentos do PU oficial; por isso, pode não recuperar exatamente a taxa usada para gerar um PU por `pu`.

Examples:

Exibe as taxas em formato decimal:

```pycon
>>> from pyield import ltn
>>> ltn.taxa("05-07-2024", "01-01-2030", 535.279902)
0.12145000037780962
>>> ltn.taxa("13-03-2026", "01-01-2027", 895.563913) * 100
14.830700071776182
>>> ltn.taxa("21-05-2008", "01-07-2010", 753.3) * 100
14.361101890993865
```

## `taxas_forward(data)`

Calcula as taxas forward da LTN para uma data de referência.

As taxas indicativas da LTN já são taxas zero por construção, pois o título não paga cupons. Portanto o cálculo de forward é direto usando a estrutura de vencimentos e suas taxas.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data` | `DateLike` | Data das taxas indicativas. | *required* |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | pl.DataFrame: DataFrame com as taxas forward. |

**Output Columns**

- data\_vencimento (Date): Data de vencimento.
- dias\_uteis (Int64): Dias úteis entre referência e vencimento.
- taxa\_indicativa (Float64): Taxa spot (zero cupom), em formato decimal.
- taxa\_forward (Float64): Taxa forward, em formato decimal.

**Notes**

A taxa forward \\(f(t\_1,t\_2)\\), anual efetiva, em decimal e na base de 252 dias úteis, entre dois vencimentos consecutivos é:

\\\[ f(t\_1,t\_2) = \\left( \\frac{(1+r\_2)^{t\_2}}{(1+r\_1)^{t\_1}} \\right)^{1/(t\_2-t\_1)}-1 \\\]

onde:

- \\(\\mathrm{DU}\_1\\), \\(\\mathrm{DU}\_2\\): dias úteis entre a data de referência e cada vencimento, com \\(\\mathrm{DU}\_1\<\\mathrm{DU}\_2\\).
- \\(t\_1 = \\mathrm{DU}\_1/252\\), \\(t\_2 = \\mathrm{DU}\_2/252\\): prazos em anos de 252 dias úteis.
- \\(r\_1\\), \\(r\_2\\): taxas zero anuais efetivas, em decimal, correspondentes aos prazos. São as taxas indicativas das LTNs.

No primeiro vencimento, a coluna `taxa_forward` recebe a própria taxa zero, pois não há um vértice anterior.

Examples:

```pycon
>>> from pyield import ltn
>>> import polars.selectors as cs
>>> curva = ltn.taxas_forward("17-10-2025")
>>> curva.with_columns(cs.starts_with("taxa_") * 100)
shape: (13, 4)
┌─────────────────┬────────────┬─────────────────┬──────────────┐
│ data_vencimento ┆ dias_uteis ┆ taxa_indicativa ┆ taxa_forward │
│ ---             ┆ ---        ┆ ---             ┆ ---          │
│ date            ┆ i64        ┆ f64             ┆ f64          │
╞═════════════════╪════════════╪═════════════════╪══════════════╡
│ 2026-01-01      ┆ 52         ┆ 14.8307         ┆ 14.8307      │
│ 2026-04-01      ┆ 113        ┆ 14.7173         ┆ 14.62072     │
│ 2026-07-01      ┆ 174        ┆ 14.5206         ┆ 14.157112    │
│ 2026-10-01      ┆ 239        ┆ 14.2424         ┆ 13.501001    │
│ 2027-04-01      ┆ 361        ┆ 13.8155         ┆ 12.983814    │
│ …               ┆ …          ┆ …               ┆ …            │
│ 2028-07-01      ┆ 676        ┆ 13.3411         ┆ 13.165428    │
│ 2029-01-01      ┆ 800        ┆ 13.4254         ┆ 13.886075    │
│ 2029-07-01      ┆ 924        ┆ 13.5264         ┆ 14.180178    │
│ 2030-01-01      ┆ 1049       ┆ 13.5967         ┆ 14.11771     │
│ 2032-01-01      ┆ 1553       ┆ 13.883          ┆ 14.481206    │
└─────────────────┴────────────┴─────────────────┴──────────────┘
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
>>> from pyield import ltn
>>> ltn.vencimentos("22-08-2024")
shape: (13,)
Series: 'data_vencimento' [date]
[
    2024-10-01
    2025-01-01
    2025-04-01
    2025-07-01
    2025-10-01
    …
    2026-10-01
    2027-07-01
    2028-01-01
    2028-07-01
    2030-01-01
]
```
