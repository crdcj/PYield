# Novidades na versão 0.58.0

Nota reconstituída a partir do histórico Git, usando o [commit de incremento da versão](https://github.com/crdcj/PYield/commit/dbcec48a) e o [diff desde o incremento anterior](https://github.com/crdcj/PYield/compare/d9b5bcea...dbcec48a). O recorte é definido pelos commits de versão, não por tags de publicação.

As orientações abaixo descrevem a API histórica. Para chegar à API atual,
consulte também as notas das versões seguintes no
[histórico de versões](https://crdcj.github.io/PYield/releases/).

## Cotações em base 100 e taxas percentuais

As funções escalares de cálculo dos títulos também aceitam taxas percentuais
explícitas, como `"5.75%"` ou `"5,75%"`, equivalentes a `0.0575`.
Números continuam em formato decimal; strings sem `%` são rejeitadas.

As funções `cotacao` de `yd.lft`, `yd.ntnb`, `yd.ntnc`, `yd.ntnbp` e
`yd.ntnb1` passam a retornar base 100 com quatro casas decimais:
`0.993651` passa a ser `99.3651`. As funções `pu` desses títulos recebem a
cotação nessa mesma base e aplicam `VNA * cotacao / 100`.

A coluna `valor_pagamento` de `fluxos_caixa` de NTN-B, NTN-C e NTN-B1 e o
retorno de `ntnb1.cotacao_curva_zero` também passam para base 100. Esta última
mantém a soma sem truncamento para calibrar a TIR equivalente.

Para migrar, multiplique cotações e fluxos antigos armazenados por 100, remova
multiplicações por 100 usadas apenas para exibir cotações e ajuste cálculos
manuais de PU para dividir por 100. A composição `pu(vna, cotacao(...))`
preserva a unidade e as regras de precisão do preço. Taxas continuam em formato
decimal, e VNA e PU continuam em reais.

## Migração acumulada desde a 0.56.0

O README dessa versão também preservava o quadro abaixo. Ele reúne alterações
do intervalo desde a `0.56.0`; não significa que todas foram introduzidas na
`0.58.0`.

Este resumo parte da API da `0.56.0`. Mudanças que já faziam parte dessa versão
ou de versões anteriores estão no histórico das
[releases do GitHub](https://github.com/crdcj/PYield/releases).

| Antes | Agora |
|---|---|
| `ntnbp.taxas_zero(...)` | `ntnb.taxas_zero(...)`, sem `incluir_vertices` |
| `ntnb.taxas_zero(..., incluir_cupons=...)` | `ntnb.taxas_zero(...)`, sem `incluir_cupons` |
| `ntnb.taxas_zero(..., percentual=...)` | `ntnb.taxas_zero(...)`, com taxas em formato decimal |
| `yd.tpf.ntnb` | `yd.ntnb` ou `from pyield import ntnb` |
| `yd.tpf.rmd(aba)` | `yd.rmd(aba)` ou `from pyield import rmd` |
| `pyield.tpf.vna.calcular_vna(...)` | `yd.vna.calcular_vna(...)` |
| `yd.ntnb.vna(data)` | `yd.vna.valor("NTN-B", data)` |
| `yd.ntnb.vnas()` | `yd.vna.historico("NTN-B")` |
| `vna_projetado(...)` nos módulos de títulos | `yd.vna.projetado(titulo, data, vna_base, inflacao)` |
| `vigencia(data)` nos módulos de títulos | `yd.vna.vigencia(titulo, data)` |

O namespace público segue a autonomia semântica do conceito que o usuário
precisa conhecer, e não apenas a fonte dos dados ou sua relação temática com
outro domínio. Por isso, conceitos como `yd.ltn`, `yd.ntnb`, `yd.vna` e `yd.rmd`
ficam na raiz, enquanto as implementações podem continuar agrupadas
internamente por domínio para manter o código coeso.
Consulte as [releases](https://github.com/crdcj/PYield/releases) para o
histórico completo.

