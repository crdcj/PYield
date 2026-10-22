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

`yd.vna.valor("LFT", data)` não calcula o fator acumulado nem reconstrói o VNA. A função baixa
do Banco Central o VNA oficial do código `210100` e valida que o valor é único
entre os vencimentos. Esse dado oficial é a entrada usada pelo módulo de LFT.

Para reconstruir o acruamento diário, a referência direta é a série SGS 11,
que publica a taxa Selic diária em percentual. Em 18/09/2026, por exemplo, a
série retornou `0,050788%`, equivalente ao fator `1,00050788`. Aplicado ao VNA
de 17/09/2026, o resultado truncado em seis casas é exatamente o VNA publicado
para 18/09/2026. A série SGS 1178, embora seja chamada Selic Over, publica a
taxa anualizada e arredondada; ela não deve ser usada como fator diário exato.

O caderno define o fator acumulado `C` com oito casas, enquanto o guia de
leilões apresenta o índice Selic acumulado com dezesseis. Essa diferença é uma
observação sobre a formação externa do dado e não representa uma divergência
da consulta atual. A conferência experimental abaixo registra o efeito dessa
precisão na reconstrução local, sem alterar o contrato da consulta.

### Conferência da reconstrução desde a origem

`tests/vna/test_reconstrucao_lft.py` preserva o experimento com 6.607 taxas
diárias da série SGS 11, de 03/07/2000 a 21/10/2026. A taxa é divulgada em
percentual ao dia; o fator diário é `1 + taxa / 100`. A
[documentação do BCB](https://www3.bcb.gov.br/novoselic/partials/observacao-taxa-selic.html)
descreve o fator diário com oito casas decimais.

O produtório é acumulado com `Decimal` e precisão de 28 dígitos, sem truncar
o VNA a cada dia. Adotando o A-16 do manual de leilões e o T-6 do resultado:

```text
fator_acumulado = A16(produto(1 + taxa_sgs11 / 100))
vna = T6(1000 * fator_acumulado)
```

A reconstrução reproduz exatamente as seis casas de 39 arquivos oficiais
APC238: um de janeiro de 2020, um por mês de janeiro de 2024 a outubro de
2026 e quatro datas adicionais de maio de 2024, setembro e outubro de 2026.
As taxas e os relatórios foram capturados em 22/10/2026 e preservados byte a
byte nas 43 entradas de `tests/vna/data/lft_selic.zip`.
A comparação chama `yd.vna.valor` com
somente o fetch substituído pelos arquivos locais.

Em 21/10/2026, o relatório publica `20139,601018`. Arredondar o fator acumulado
em oito casas retorna `20139,601020`; truncar o VNA diariamente desde a origem
retorna `20139,583411`. Os testes registram ambas as diferenças sem tolerância
nem alteração das referências. A precisão documental A-16 é compatível com
a amostra, que não permite identificar sozinha todos os detalhes internos
da acumulação no BCB.

### Data do arquivo e data de vigência

O arquivo APC238 é gerado no dia da apuração da Selic e informa, no cabeçalho,
os índices **para o próximo dia útil**. O arquivo de 31/05/2024, por exemplo,
contém `14903,011480` para 03/06/2024; o de 18/09/2026 contém `19915,882980`
para 21/09/2026. Para reproduzir o conteúdo do arquivo, o teste inclui a taxa
SGS 11 da data de geração. Para calcular o VNA de uma data de vigência,
a data final da acumulação é exclusiva.

Essa última convenção também reproduz o exemplo do manual de leilões:
o VNA de 20/05/2008 é `3449,694215`, acumulando a Selic até 19/05/2008.
A consulta atual `yd.vna.valor("LFT", data)` usa `data` no nome do arquivo
APC238; esta conferência não muda essa seleção nem desloca datas automaticamente.

### Consequência para o cálculo público

A operação `yd.vna.calcular_base("LFT", fator_acumulado)` recebe o fator Selic
acumulado desde 01/07/2000 e aplica `T6(1000 * A16(fator_acumulado))`.
Esse insumo não é uma taxa anual, uma taxa diária nem somente o fator de um
dia. Sua formação exige a série diária desde a origem, enquanto os números-índice
de IPCA e IGP-M já incorporam o acúmulo em cada observação mensal.

Os testes das 39 referências agora chamam o cálculo público e comparam o
resultado com os relatórios oficiais e o modelo de precisão independente.
A base de R$ 1.000 é constante interna; o fator deve ser positivo e finito.
A função não recebe vencimento para LFT, não acumula a série automaticamente
e mantém o cálculo explícito, separado das consultas e das projeções.

## Limite para produtos do Tesouro Direto

A presença de NTN-B no caderno não estende automaticamente suas regras a
`ntnb1` ou `ntnbp`. Esses títulos, vendidos exclusivamente pelo Tesouro Direto,
devem continuar vinculados às metodologias específicas do programa.

## Uso no PYield

Este documento fica disponível para a revisão futura de juros, amortização,
eventos e da metodologia de formação dos valores nominais. Sua inclusão, por
si só, não altera fórmulas já implementadas: cada mudança deve identificar a
seção aplicável e acrescentar um teste de referência.
