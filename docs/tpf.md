# Títulos Públicos Federais (TPF)

Porta de entrada principal para dados de mercado de títulos públicos: taxas
indicativas, vencimentos, estoque, dealers, negociações secundárias, leilões
e benchmarks.

O Relatório Mensal da Dívida (RMD) usa dados do Tesouro Nacional, mas representa
a dívida pública de forma mais ampla do que os títulos públicos federais. Por
isso, sua entrada é `yd.rmd(aba)`, exposta na raiz, e não `yd.tpf.rmd(aba)`.

Para precificação e análise por tipo de título (cotação, duration, prêmio),
consulte as páginas individuais: [LFT](lft.md), [LTN](ltn.md),
[NTN-B](ntnb.md), [NTN-F](ntnf.md), etc.

## Taxas indicativas

Use `yd.tpf.taxas(...)` para consultar uma data e
`yd.tpf.taxas_historicas(...)` para consultar um período ou todo o histórico
disponível. As duas funções retornam o mesmo conjunto estável de colunas.

```python
import pyield as yd

taxas_dia = yd.tpf.taxas("23-08-2024", titulo="PRE")
taxas_periodo = yd.tpf.taxas_historicas(
    inicio="01-08-2024",
    fim="31-08-2024",
    titulo="PRE",
)
```

## Convenções de escala e precisão

Nos cálculos escalares de LTN, LFT, NTN-B, NTN-C, NTN-F, NTN-B Principal e
NTN-B1, taxas podem ser números decimais (`0.0575`) ou strings percentuais
explícitas (`"5.75%"` ou `"5,75%"`). O símbolo `%` é obrigatório em strings.
São aceitos sinal e espaços nas extremidades ou antes do símbolo, como
`" -0,02% "`. Não são aceitos separadores de milhar.

```python
yd.ntnb.cotacao("31-05-2024", "15-05-2035", "6,1490%")
# Decimal('99.3651')
```

A conversão ocorre antes das regras de precisão de cada função. Taxas numéricas
e retornos de taxa mantêm a convenção decimal: `5.75` continua significando
575%. Curvas e coleções de taxas continuam numéricas. Nas funções `_expr`,
strings passadas como argumentos continuam identificando colunas Polars.

A tabela resume as regras adotadas pela PYield na precificação de títulos
públicos federais. LTN, NTN-F, NTN-B, NTN-C e LFT seguem a metodologia da STN
para títulos ofertados em leilões primários. A NTN-B Principal e a NTN-B1,
vendidas exclusivamente pelo Tesouro Direto, seguem as regras próprias desse
programa.

| Variáveis | LTN | NTN-F | NTN-B | NTN-B Principal | NTN-B1 | NTN-C | LFT |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Taxa de retorno | T8 / I8 | T8 / I8 | T8 / I8 | A4 | I | T8 / I8 | T8 / I8 |
| Juros semestrais | -- | A5 | A6 | -- | -- | A6 | -- |
| Fluxo de pagamentos descontados | -- | A9 | A10 | -- | A10 | A10 | -- |
| Cotação (base 100) | -- | -- | T4 | T4 | T4 | T4 | T4 |
| Valor nominal atualizado (VNA) | -- | -- | T6 / I6 | I6 | I6 | T6 / I6 | T6 / I6 |
| Valor nominal atualizado (VNA, projeções) | -- | -- | T6 | -- | -- | T6 | T6 |
| Fator acumulado da taxa Selic | -- | -- | -- | -- | -- | -- | A16 |
| Projeções | -- | -- | A4 | -- | -- | A4 | -- |
| Fator pro rata (projeções) | -- | -- | T14 | -- | -- | T14 | -- |
| Variação do mês oficial | -- | -- | T16 | -- | -- | T16 | -- |
| Exponencial de dias | T14 | T14 | T14 | T14 | T14 | T14 | T14 |
| Preço unitário (PU) | T6 / I6 | T6 / I6 | T6 | T6 | T6 | T6 | T6 |
| Valor financeiro | T2 | T2 | T2 | T2 | T2 | T2 | T2 |

Na tabela, **T** significa truncado, **A**, arredondado, e **I**, informado.
Na NTN-B1, `T4` descreve a função `cotacao`. A função
`cotacao_curva_zero` arredonda cada fluxo em `A10`, mas não trunca a soma final,
pois ela é usada como alvo da calibração da taxa equivalente.

Os cupons, os fluxos e as cotações de NTN-B e NTN-C são calculados em base
100, com as casas decimais da tabela da STN. LFT, NTN-B Principal e NTN-B1
também retornam cotação em base 100. A NTN-F mantém os fluxos em base 1000.

Taxas e projeções de inflação continuam em formato decimal na API: T6 na taxa
percentual equivale a T8 na taxa decimal, e A2 na projeção percentual equivale
a A4 na projeção decimal. A normalização da taxa de precificação dos títulos
de leilão trunca a taxa decimal em oito casas antes do desconto.

Por exemplo, a cotação `99,3651` da STN é retornada como `Decimal("99.3651")`.
A relação entre cotação e preço é:

```python
pu = vna * cotacao / 100
```

Use `yd.ntnb.pu(vna, cotacao)` (ou a função do título correspondente) para
aplicar também os truncamentos de VNA, cotação e PU.

As regras usadas para LTN, NTN-F, NTN-B, NTN-C e LFT estão na
[metodologia da STN para os títulos ofertados em leilões primários](referencias/metodologia-calculo-tpf-stn.md).
Para a NTN-B1, consulte a [metodologia específica do Tesouro Direto](referencias/metodologia-calculo-ntnb1.md).

### Exibição de taxas em percentual

As funções recebem e retornam taxas em formato decimal. Nos exemplos, a
formatação percentual facilita a leitura sem alterar o valor usado nos cálculos:

```python
import pyield as yd

taxa = yd.lft.taxa("24-07-2024", "01-09-2030", 15785.324502, 15621.867466)
f"{taxa:.6%}"  # -> '0.171691%'
```

As seis casas percentuais preservam as oito casas decimais do retorno de
`taxa` para LFT, LTN, NTN-B, NTN-C e NTN-F. A formatação produz uma string;
continue usando a variável `taxa` nos cálculos.

Nas tabelas dos exemplos, a multiplicação das colunas de taxas por `100`
também serve apenas para exibição. Prêmios apresentados em pontos-base usam
o multiplicador `10_000`.

::: pyield.tpf

## Acesso técnico à fonte ANBIMA

O módulo `pyield.anbima.taxas` permite baixar ou ler o arquivo da ANBIMA com
todas as colunas processadas da fonte. Essa camada é indicada para integração
com a fonte; para análises de TPF, prefira a visão estável de `yd.tpf`.

::: pyield.anbima.taxas

## secundario

::: pyield.tpf.secundario

## leiloes

::: pyield.tpf.leiloes.leiloes

## dealers

::: pyield.tpf.dealers.dealers

## Relatório Mensal da Dívida (RMD)

::: pyield.rmd
