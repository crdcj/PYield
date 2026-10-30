Precificação de NTN-B1.

## `NomeComercial`

Bases: `Enum`

Enum do nome comercial usado para identificar o tipo de NTN-B1 (Renda+ ou Educa+).

## `cotacao(data_liquidacao, data_vencimento, taxa, nome_comercial)`

Calcula a cotação da NTN-B1 em base 100.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data_liquidacao` | `DateLike` | Data de liquidação da operação. | *required* |
| `data_vencimento` | `DateLike` | Data de vencimento da NTN-B1. | *required* |
| `taxa` | `float \| Decimal \| str` | Taxa de desconto (YTM) usada no valor presente. Aceita também percentual explícito: "5.75%" ou "5,75%". A taxa de entrada não é truncada antes do cálculo. | *required* |
| `nome_comercial` | `NomeComercial` | Nome comercial (Renda+ ou Educa+). | *required* |

Returns:

| Name | Type | Description |
| --- | --- | --- |
| `Decimal` | `Decimal` | Cotação da NTN-B1 em base 100, truncada em 4 casas decimais. |

**References**

- Tesouro Direto, *Cálculo da Rentabilidade dos Títulos Públicos Ofertados no Tesouro Direto — Tesouro Educa+ (NTN-B1)*.
- Tesouro Direto, *Cálculo da Rentabilidade dos Títulos Públicos Ofertados no Tesouro Direto — Tesouro RendA+ (NTN-B1)*.

Examples:

```pycon
>>> from pyield import ntnb1
>>> r_mais = ntnb1.NomeComercial.RENDA_MAIS
>>> ntnb1.cotacao("18-06-2025", "15-12-2084", "7.01%", r_mais)
Decimal('3.8332')
>>> educa_mais = ntnb1.NomeComercial.EDUCA_MAIS
>>> ntnb1.cotacao("22-06-2023", "15-12-2034", "5.36%", educa_mais)
Decimal('62.6809')
```

## `cotacao_curva_zero(data_liquidacao, data_vencimento, curva_zero, nome_comercial)`

Calcula a cotação de uma NTN-B1 descontando cada fluxo pela curva zero.

A função usa interpolação flat-forward entre os vértices da curva e mantém a última taxa zero após o maior vértice. Cada valor presente, em base 100, é arredondado na 10ª casa decimal; a soma final não é truncada porque ela é o alvo da calibração da TIR equivalente.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data_liquidacao` | `DateLike` | Data de liquidação. | *required* |
| `data_vencimento` | `DateLike` | Data da última amortização da NTN-B1. | *required* |
| `curva_zero` | `DataFrame` | DataFrame com as colunas `dias_uteis` e `taxa_zero`. | *required* |
| `nome_comercial` | `NomeComercial` | Nome comercial, Renda+ ou Educa+. | *required* |

Returns:

| Name | Type | Description |
| --- | --- | --- |
| `float` | `float` | Cotação em base 100 calculada pela curva zero. |

**Notes**

Os documentos oficiais descrevem o desconto dos fluxos por uma taxa única pactuada. Eles não definem a interpolação ou a extrapolação da curva zero usada nesta função.

**References**

- Tesouro Direto, *Cálculo da Rentabilidade dos Títulos Públicos Ofertados no Tesouro Direto — Tesouro Educa+ (NTN-B1)*.
- Tesouro Direto, *Cálculo da Rentabilidade dos Títulos Públicos Ofertados no Tesouro Direto — Tesouro RendA+ (NTN-B1)*.

## `datas_pagamento(data_liquidacao, data_vencimento, nome_comercial)`

Gera todas as datas de amortização entre liquidação e vencimento.

A liquidação é exclusiva e o vencimento é inclusivo. Os pagamentos ocorrem de 15/01 do ano de conversão até 15/12 do ano de vencimento. As datas são contratuais e não são ajustadas para dias úteis.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data_liquidacao` | `DateLike` | Data de liquidação (exclusiva). | *required* |
| `data_vencimento` | `DateLike` | Data de vencimento. | *required* |
| `nome_comercial` | `NomeComercial` | Nome comercial (Renda+ ou Educa+). | *required* |

Returns:

| Type | Description |
| --- | --- |
| `Series` | pl.Series: Série de datas de amortização no intervalo. |

**Notes**

Para obter as datas efetivas de processamento, use `yd.du.deslocar(..., 0)`.

**References**

- Tesouro Direto, *Cálculo da Rentabilidade dos Títulos Públicos Ofertados no Tesouro Direto — Tesouro Educa+ (NTN-B1)*.
- Tesouro Direto, *Cálculo da Rentabilidade dos Títulos Públicos Ofertados no Tesouro Direto — Tesouro RendA+ (NTN-B1)*.

Examples:

```pycon
>>> from pyield import ntnb1
>>> r_mais = ntnb1.NomeComercial.RENDA_MAIS
>>> ntnb1.datas_pagamento("10-05-2024", "15-12-2050", r_mais)
shape: (240,)
Series: 'datas_pagamento' [date]
[
    2031-01-15
    2031-02-15
    2031-03-15
    2031-04-15
    2031-05-15
    …
    2050-08-15
    2050-09-15
    2050-10-15
    2050-11-15
    2050-12-15
]
```

## `duration(data_liquidacao, data_vencimento, taxa, nome_comercial)`

Calcula a Macaulay duration da NTN-B1 em anos úteis.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data_liquidacao` | `DateLike` | Data de liquidação da operação. | *required* |
| `data_vencimento` | `DateLike` | Data de vencimento. | *required* |
| `taxa` | `float` | Taxa de desconto usada no cálculo. Aceita também percentual explícito: "5.75%" ou "5,75%". | *required* |
| `nome_comercial` | `NomeComercial` | Nome comercial (Renda+ ou Educa+). | *required* |

Returns:

| Name | Type | Description |
| --- | --- | --- |
| `float` | `float` | Macaulay duration em anos úteis. |

Examples:

```pycon
>>> from pyield import ntnb1
>>> r_mais = ntnb1.NomeComercial.RENDA_MAIS
>>> ntnb1.duration("23-06-2025", "15-12-2084", "6.86%", r_mais)
47.10494386899199
```

## `dv01(data_liquidacao, data_vencimento, taxa, pu, nome_comercial=NomeComercial.RENDA_MAIS)`

Calcula o DV01 (Dollar Value of 01) da NTN-B1 em R$.

Representa a variação do PU informado para um aumento de 1 bp (0,01%) na taxa.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data_liquidacao` | `DateLike` | Data de liquidação. | *required* |
| `data_vencimento` | `DateLike` | Data de vencimento. | *required* |
| `taxa` | `float` | Taxa de desconto (YTM) da NTN-B1. Aceita também percentual explícito: "5.75%" ou "5,75%". | *required* |
| `pu` | `float \| Decimal` | PU usado como base para o cálculo. | *required* |
| `nome_comercial` | `NomeComercial` | Nome comercial (Renda+ ou Educa+). | `RENDA_MAIS` |

Returns:

| Name | Type | Description |
| --- | --- | --- |
| `float` | `float` | DV01, variação de preço para 1 bp. |

Examples:

```pycon
>>> from pyield import ntnb1
>>> r_mais = ntnb1.NomeComercial.RENDA_MAIS
>>> cot = ntnb1.cotacao("23-06-2025", "15-12-2084", "6.86%", r_mais)
>>> pu = ntnb1.pu(4299.160173, cot)
>>> ntnb1.dv01("23-06-2025", "15-12-2084", "6.86%", pu, r_mais)
0.7746782352496799
```

## `fluxos_caixa(data_liquidacao, data_vencimento, nome_comercial)`

Gera os fluxos de caixa da NTN-B1 entre liquidação e vencimento.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data_liquidacao` | `DateLike` | Data de liquidação (exclusiva). | *required* |
| `data_vencimento` | `DateLike` | Data de vencimento. | *required* |
| `nome_comercial` | `NomeComercial` | Nome comercial (Renda+ ou Educa+). | *required* |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | pl.DataFrame: DataFrame com as colunas de fluxo. |

**Output Columns**

- data\_pagamento (Date): Data contratual do pagamento, sem ajuste para dia útil.
- valor\_pagamento (Float64): Valor do pagamento em base 100.

**Notes**

Para obter as datas efetivas de processamento, use `yd.du.deslocar(..., 0)`.

**References**

- Tesouro Direto, *Cálculo da Rentabilidade dos Títulos Públicos Ofertados no Tesouro Direto — Tesouro Educa+ (NTN-B1)*.
- Tesouro Direto, *Cálculo da Rentabilidade dos Títulos Públicos Ofertados no Tesouro Direto — Tesouro RendA+ (NTN-B1)*.

Examples:

```pycon
>>> from pyield import ntnb1
>>> r_mais = ntnb1.NomeComercial.RENDA_MAIS
>>> ntnb1.fluxos_caixa("10-05-2024", "15-12-2060", r_mais)
shape: (240, 2)
┌────────────────┬─────────────────┐
│ data_pagamento ┆ valor_pagamento │
│ ---            ┆ ---             │
│ date           ┆ f64             │
╞════════════════╪═════════════════╡
│ 2041-01-15     ┆ 0.416666        │
│ 2041-02-15     ┆ 0.416666        │
│ 2041-03-15     ┆ 0.416666        │
│ 2041-04-15     ┆ 0.416666        │
│ 2041-05-15     ┆ 0.416666        │
│ …              ┆ …               │
│ 2060-08-15     ┆ 0.416666        │
│ 2060-09-15     ┆ 0.416666        │
│ 2060-10-15     ┆ 0.416666        │
│ 2060-11-15     ┆ 0.416666        │
│ 2060-12-15     ┆ 0.416826        │
└────────────────┴─────────────────┘
```

## `pu(vna, cotacao)`

Calcula o preço (PU) da NTN-B1 pelas regras do Tesouro Nacional.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `vna` | `float \| Decimal` | Valor nominal atualizado (VNA), truncado em seis casas decimais antes do cálculo, sem arredondar. | *required* |
| `cotacao` | `float \| Decimal` | Cotação da NTN-B1 em base 100. Truncada em quatro casas decimais antes do cálculo, sem arredondar. | *required* |

Returns:

| Name | Type | Description |
| --- | --- | --- |
| `Decimal` | `Decimal` | Preço da NTN-B1 truncado em 6 casas decimais. |

**References**

- Tesouro Direto, *Cálculo da Rentabilidade dos Títulos Públicos Ofertados no Tesouro Direto — Tesouro Educa+ (NTN-B1)*.
- Tesouro Direto, *Cálculo da Rentabilidade dos Títulos Públicos Ofertados no Tesouro Direto — Tesouro RendA+ (NTN-B1)*. - Secretaria do Tesouro Nacional, [consulta pública sobre a composição dos pagamentos mensais da NTN-B1](<https://www.gov.br/participamaisbrasil/consulta-portaria-composicao-ntn-b11>), processo SEI `17944.005214/2024-09`.

Examples:

```pycon
>>> from pyield import ntnb1
>>> ntnb1.pu(4299.160173, 99.3651)
Decimal('4271.864805')
>>> ntnb1.pu(4315.498383, 100.6409)
Decimal('4343.156412')
>>> ntnb1.pu(4128.272299, 62.6809)
Decimal('2587.638231')
```

## `taxa_curva_zero(data_liquidacao, data_vencimento, curva_zero, nome_comercial)`

Calcula a TIR equivalente de uma NTN-B1 pela curva zero.

Primeiro, cada amortização mensal do Renda+ ou Educa+ é descontada pela taxa zero correspondente à sua data. Em seguida, a função encontra por bisseção a taxa única que produz a mesma cotação quando aplicada a todos os fluxos. Essa é a taxa equivalente do título.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data_liquidacao` | `DateLike` | Data de liquidação. | *required* |
| `data_vencimento` | `DateLike` | Data da última amortização da NTN-B1. | *required* |
| `curva_zero` | `DataFrame` | DataFrame com as colunas `dias_uteis` e `taxa_zero`. | *required* |
| `nome_comercial` | `NomeComercial` | Nome comercial, Renda+ ou Educa+. | *required* |

Returns:

| Name | Type | Description |
| --- | --- | --- |
| `float` | `float` | TIR equivalente anualizada, em formato decimal. Retorna NaN para entradas ausentes. Também retorna NaN se a resolução numérica falhar. |

**Notes**

Os documentos oficiais descrevem o desconto dos fluxos por uma taxa única pactuada. Eles não definem a interpolação ou a extrapolação da curva zero usada nesta função.

**References**

- Tesouro Direto, *Cálculo da Rentabilidade dos Títulos Públicos Ofertados no Tesouro Direto — Tesouro Educa+ (NTN-B1)*.
- Tesouro Direto, *Cálculo da Rentabilidade dos Títulos Públicos Ofertados no Tesouro Direto — Tesouro RendA+ (NTN-B1)*.
