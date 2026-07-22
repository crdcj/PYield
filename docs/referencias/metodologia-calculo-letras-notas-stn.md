# Metodologia de cálculo das Letras e Notas — STN

Este caderno é uma referência complementar para os títulos custodiados no
Sistema Especial de Liquidação e de Custódia (SELIC). Ele abrange os mesmos
títulos tratados na
[metodologia de precificação dos títulos ofertados em leilões primários](metodologia-calculo-tpf-stn.md),
além de outros ativos.

## Documento

- Órgão: Secretaria do Tesouro Nacional (STN).
- Título catalogado: *Metodologia de Cálculo das Letras e Notas do Tesouro
  Nacional*.
- Título nos metadados do PDF: *CAD FÓRMULAS - SELIC (revisão - NTN-I)_v2*.
- [Publicação oficial no Tesouro Transparente](https://www.tesourotransparente.gov.br/publicacoes/metodologia-de-calculo-das-letras-e-notas-do-tesouro-nacional/2008/26).
- Data catalogada pelo Tesouro Transparente: 01/01/2008.
- Metadados do PDF: criado e modificado em 28/07/2016.
- [Baixar a cópia preservada](metodologia-calculo-letras-notas-stn.pdf).
- SHA-256:
  `230440b86f6a80c13c58fbafd233979dc62be2509608f99a141998e636f52657`.

A divergência entre a data catalogada e os metadados é registrada sem assumir
que uma delas representa uma nova edição normativa.

## Escopo

O documento descreve cálculos de valor nominal, preço unitário, juros
pró-rata, amortizações e pagamentos para ativos custodiados no SELIC. Além de
LFT, LTN, NTN-B e NTN-C, inclui BTN, LFT-A, LFT-B e várias séries legadas de
NTN.

Ele é especialmente relevante para:

- atualização do valor nominal;
- fatores de juros e indexação;
- eventos de juros e amortização;
- valores financeiros associados à custódia e ao pagamento.

## Relação com a metodologia de leilões

| Contexto | Referência principal |
| --- | --- |
| Taxa, cotação e PU em ofertas públicas | Metodologia dos TPF ofertados em leilões primários |
| Formação do VNA, juros pró-rata, amortizações e eventos | Este caderno de fórmulas |
| VNA da LFT consumido pelo PYield | Arquivo diário oficial do Banco Central |
| Produtos específicos do Tesouro Direto | Metodologia própria do Tesouro Direto |

Os documentos não descrevem universos independentes: LTN, LFT, NTN-B e NTN-C
são custodiadas no SELIC e aparecem nas duas referências.

## LFT implementada no PYield

O PYield trata a LFT comum, código SELIC `210100`, com data-base `01/07/2000`.
Ela corresponde à seção 4 deste caderno, e não às séries `LFT-A` ou `LFT-B`,
descritas separadamente nas seções 4.1 e 4.2.

`lft.vna()` não calcula o fator acumulado nem reconstrói o VNA. A função baixa
do Banco Central o VNA oficial do código `210100` e valida que o valor é único
entre os vencimentos. Esse dado oficial é a entrada usada pelo módulo de LFT.

O caderno define o fator acumulado `C` com oito casas, enquanto o guia de
leilões apresenta o índice Selic acumulado com dezesseis. Essa diferença é uma
observação sobre a formação externa do dado e não representa uma divergência
da implementação atual. Ela só precisará ser analisada se o PYield passar a
recalcular localmente o VNA da LFT.

## Limite para produtos do Tesouro Direto

A presença de NTN-B no caderno não estende automaticamente suas regras a
`ntnb1` ou `ntnbp`. Esses títulos, vendidos exclusivamente pelo Tesouro Direto,
devem continuar vinculados às metodologias específicas do programa.

## Uso no PYield

Este documento fica disponível para a revisão futura de juros, amortização,
eventos e da metodologia de formação dos valores nominais. Sua inclusão, por
si só, não altera fórmulas já implementadas: cada mudança deve identificar a
seção aplicável e acrescentar um teste de referência.
