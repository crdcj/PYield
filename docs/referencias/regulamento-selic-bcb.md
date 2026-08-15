# Regulamento do Selic — BCB

Esta é a referência normativa adotada pelo PYield para determinar a posição de
custódia considerada nos pagamentos de juros, amortizações e resgates dos
títulos registrados no Sistema Especial de Liquidação e de Custódia (Selic).

## Documento

- Órgão: Banco Central do Brasil (BCB).
- Norma: Resolução BCB nº 55, de 16 de dezembro de 2020.
- Objeto: Regulamento do Sistema Especial de Liquidação e de Custódia (Selic).
- [Versão vigente no Banco Central](https://www.bcb.gov.br/estabilidadefinanceira/exibenormativo?numero=55&tipo=Resolu%C3%A7%C3%A3o+BCB).
- Versão consultada em 14/08/2026, indicada pelo BCB como atualizada em
  02/09/2024.

## Titularidade dos fluxos

O art. 31 estabelece que, para pagamentos de juros, amortizações e resgates, a
posição de cada conta corresponde ao saldo de fechamento do dia útil
imediatamente anterior. A exceção prevista para títulos resgatados no dia do
evento considera apenas recompras e revendas previamente assumidas.

O parágrafo único do mesmo artigo equipara a resgate a amortização da última
parcela. Portanto, a regra abrange cupons, amortizações intermediárias e o
pagamento final do principal; ela não cria uma diferença geral de inclusão
entre cupom e principal.

O art. 32 acrescenta que não é permitida a movimentação do título no dia de seu
resgate, salvo as exceções expressamente previstas no Regulamento.

## Data contratual e data efetiva

A data em que o pagamento é processado não deve ser confundida com a posição de
custódia que tem direito ao fluxo. Quando um evento contratual recai em dia não
útil e sua efetivação ocorre no dia útil subsequente, o deslocamento operacional
não transfere o direito ao pagamento para uma operação nova liquidada nesse dia.

Para a seleção dos fluxos de uma cotação, a consequência é:

1. identificar os eventos contratuais posteriores à liquidação;
2. excluir eventos cujo direito já tenha sido determinado por uma posição
   anterior;
3. somente então tratar a data efetiva do pagamento, quando isso for necessário
   para o cálculo.

Filtrar os fluxos depois de deslocar todas as datas contratuais pode incluir
indevidamente um evento que já pertence à posição anterior.

## Caso de referência: NTN-B 2026

O vencimento contratual da NTN-B 2026 é 15/08/2026, um sábado. Para uma operação
liquidada em 14/08/2026, o evento ainda é posterior à liquidação e integra a
posição de fechamento considerada no resgate. Para uma suposta liquidação em
17/08/2026, o direito ao fluxo já foi determinado e o título está em processo de
resgate; a cotação não é aplicável e deve ser representada por `NaN`, não por
zero.

Zero representaria uma cotação válida sem valor econômico. `NaN` representa a
ausência de uma cotação aplicável para aquela combinação de liquidação e
vencimento.

## Limite desta decisão

Esta referência resolve a titularidade e a inclusão dos fluxos. Ela não basta,
isoladamente, para definir se o prazo de desconto deve terminar na data
contratual ou na data efetiva de um pagamento postergado. Qualquer alteração
desse prazo deve ser confirmada na metodologia específica do título e protegida
por teste de referência próprio.
