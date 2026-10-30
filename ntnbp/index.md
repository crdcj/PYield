Precificação de NTN-B Principal (Tesouro IPCA+ sem cupons).

**Convenções de cálculo**

- Fluxo único no vencimento, atualizado pelo IPCA via VNA.
- Prazo de desconto: dias úteis / 252, truncado a 14 casas.
- Cotação: base 100, truncada a 4 casas.
- VNA de entrada e PU retornado: truncados a 6 casas.
- Taxa da curva: float interpolado por flat-forward, sem arredondamento.

O exemplo de precificação do Tesouro Direto (páginas 2 a 4) usa cotação com 4 casas, embora o texto da página 4 mencione 2. O PU da biblioteca mantém 6 casas; o preço em centavos do documento exige truncamento adicional. O documento parte de uma taxa pactuada e não especifica a regra de arredondamento da taxa obtida por curva zero.

**References**

- Tesouro Direto — Cálculo da Rentabilidade: Tesouro IPCA+. [https://www.tesourodireto.com.br/documents/d/guest/tesouro\_ipca\_juros\_semestrais](<https://www.tesourodireto.com.br/documents/d/guest/tesouro_ipca_juros_semestrais>)

## `cotacao(data_liquidacao, data_vencimento, taxa_tir)`

Calcula a cotação da NTN-B Principal em base 100 descontando o principal pela TIR.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data_liquidacao` | `DateLike` | Data de liquidação. | *required* |
| `data_vencimento` | `DateLike` | Data de vencimento. | *required* |
| `taxa_tir` | `float \| Decimal \| str` | Taxa interna de retorno anualizada do título. Aceita também percentual explícito: "5.75%" ou "5,75%". A taxa de entrada não é truncada antes do cálculo. | *required* |

Returns:

| Name | Type | Description |
| --- | --- | --- |
| `Decimal` | `Decimal` | Cotação em base 100, truncada em 4 casas decimais. Retorna `Decimal("NaN")` se a liquidação for igual ou posterior ao vencimento. |

Examples:

```pycon
>>> from pyield import ntnbp
>>> ntnbp.cotacao("02-12-2025", "15-05-2029", "7.77%")
Decimal('77.4630')
```

Exemplo do Tesouro Direto, páginas 2 a 4: liquidação em 03/01/2012, vencimento em 15/05/2015, prazo de 846 dias úteis e taxa de 5,17% a.a.

```pycon
>>> ntnbp.cotacao("03-01-2012", "15-05-2015", "5.17%")
Decimal('84.4317')
```

**References**

- Tesouro Direto — Tesouro IPCA+, páginas 2 a 4. [https://www.tesourodireto.com.br/documents/d/guest/tesouro\_ipca\_juros\_semestrais](<https://www.tesourodireto.com.br/documents/d/guest/tesouro_ipca_juros_semestrais>)

## `dv01(data_liquidacao, data_vencimento, taxa_tir, pu)`

Calcula o DV01 (Dollar Value of 01) da NTN-B Principal em R$.

Representa a variação do PU informado para um aumento de 1 bp (0,01%) na taxa.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data_liquidacao` | `DateLike` | Data de liquidação. | *required* |
| `data_vencimento` | `DateLike` | Data de vencimento. | *required* |
| `taxa_tir` | `float` | Taxa interna de retorno anualizada do título. Aceita também percentual explícito: "5.75%" ou "5,75%". | *required* |
| `pu` | `float \| Decimal` | PU usado como base para o cálculo. | *required* |

Returns:

| Name | Type | Description |
| --- | --- | --- |
| `float` | `float` | DV01 (Dollar Value of 01), variação de preço para 1 bp. Retorna `NaN` se a liquidação for igual ou posterior ao vencimento. |

Examples:

```pycon
>>> from pyield import ntnbp as bp
>>> cot = bp.cotacao("02-12-2025", "15-05-2029", "7.77%")
>>> pu = bp.pu(4567.033825, cot)
>>> bp.dv01("02-12-2025", "15-05-2029", "7.77%", pu)
1.120055806382451
```

## `pu(vna, cotacao)`

Calcula o preço (PU) da NTN-B Principal.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `vna` | `float \| Decimal` | Valor nominal atualizado (VNA), truncado em seis casas decimais antes do cálculo, sem arredondar. | *required* |
| `cotacao` | `float \| Decimal` | Cotação da NTN-B Principal em base 100. Truncada em quatro casas decimais antes do cálculo, sem arredondar. | *required* |

Returns:

| Name | Type | Description |
| --- | --- | --- |
| `Decimal` | `Decimal` | Preço da NTN-B Principal truncado em 6 casas decimais. |

Examples:

```pycon
>>> from pyield import ntnbp
>>> cot = ntnbp.cotacao("02-12-2025", "15-05-2029", "7.77%")
>>> ntnbp.pu(4567.033825, cot)
Decimal('3537.761411')
```

Exemplo do Tesouro Direto, página 4, com o VNA projetado publicado. O retorno preserva seis casas; truncado em centavos, corresponde ao preço de R$ 1.776,77 apresentado no documento.

```pycon
>>> cot = ntnbp.cotacao("03-01-2012", "15-05-2015", "5.17%")
>>> ntnbp.pu(Decimal("2104.390122"), cot)
Decimal('1776.772354')
```

**References**

- Tesouro Direto — Tesouro IPCA+, página 4. [https://www.tesourodireto.com.br/documents/d/guest/tesouro\_ipca\_juros\_semestrais](<https://www.tesourodireto.com.br/documents/d/guest/tesouro_ipca_juros_semestrais>)

## `taxa(data_liquidacao, data_vencimento, curva_zero)`

Obtém a TIR da NTN-B Principal a partir da curva zero informada.

A taxa zero correspondente ao vencimento é interpolada por flat-forward, sem arredondamento. Como a NTN-B Principal possui um único fluxo no vencimento, essa taxa zero também é a TIR do título.

Arredondamentos comerciais e spreads de compra ou venda ficam a cargo do consumidor.

A curva pode ser produzida por :func:`pyield.ntnb.taxas_zero`. Para cálculos em lote, ela deve ser construída uma única vez e reutilizada entre os títulos.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data_liquidacao` | `DateLike` | Data de liquidação usada na construção da curva. | *required* |
| `data_vencimento` | `DateLike` | Data de vencimento da NTN-B Principal. | *required* |
| `curva_zero` | `DataFrame` | DataFrame com as colunas `dias_uteis` e `taxa_zero`. | *required* |

Returns:

| Name | Type | Description |
| --- | --- | --- |
| `float` | `float` | TIR anualizada em formato decimal, sem arredondamento. Retorna `NaN` se a liquidação for igual ou posterior ao vencimento. |
