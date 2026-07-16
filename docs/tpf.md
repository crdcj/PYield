# Títulos Públicos Federais (TPF)

Porta de entrada principal para dados de mercado de títulos públicos: taxas
indicativas, vencimentos, estoque, negociações secundárias, leilões, benchmarks
e Relatório Mensal da Dívida (RMD).

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

A tabela resume as regras adotadas pela PYield na precificação de títulos
públicos federais. LTN, NTN-F, NTN-B, NTN-C e LFT seguem a metodologia da
ANBIMA; a NTN-B Principal e a NTN-B1 seguem o método do Tesouro Direto.

| Variáveis | LTN | NTN-F | NTN-B | NTN-B Principal | NTN-B1 | NTN-C | LFT |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Taxa de retorno | T6 / I6 | T6 / I6 | T6 / I6 | A4 | I | T6 / I6 | T6 / I6 |
| Juros semestrais (a.a.) | -- | A5 | A8 | -- | -- | A8 | -- |
| Fluxo de pagamentos descontados | -- | A9 | A12 | -- | A12 | A12 | -- |
| Cotação | -- | -- | T6 | T6 | T6 | T6 | T6 |
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
Na NTN-B1, `T6` descreve a função `cotacao`. A função
`cotacao_curva_zero` arredonda cada fluxo em `A12`, mas não trunca a soma final,
pois ela é usada como alvo da calibração da taxa equivalente.

Na metodologia da ANBIMA, taxas, projeções, cupons e cotações são apresentados
na escala percentual ou em base 100. A PYield armazena esses valores como fatores
decimais em base 1. Por isso, as regras correspondentes são deslocadas em duas
casas: T4 torna-se T6, A6 torna-se A8 e A10 torna-se A12.

Por exemplo, a cotação `99,3651` apresentada pela ANBIMA corresponde ao fator
`0,993651` retornado pela PYield. As duas representações preservam o mesmo valor
e a mesma precisão normativa:

```python
pu = vna * cotacao
```

As regras usadas para LTN, NTN-F, NTN-B, NTN-C e LFT estão nas
[Metodologias ANBIMA de Precificação de Títulos Públicos](https://www.anbima.com.br/data/files/A0/02/CC/70/8FEFC8104606BDC8B82BA2A8/Metodologias%20ANBIMA%20de%20Precificacao%20Titulos%20Publicos.pdf).

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
