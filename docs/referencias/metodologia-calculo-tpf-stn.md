# Metodologia de cálculo dos TPF — STN

Esta é a referência normativa adotada pelo PYield para a precificação dos
títulos públicos federais ofertados nos leilões primários.

## Documento

- Órgão: Secretaria do Tesouro Nacional (STN).
- Título: *Metodologia de Cálculo dos Títulos Públicos Federais Ofertados nos
  Leilões Primários*.
- [Publicação oficial no Tesouro Transparente](https://www.tesourotransparente.gov.br/publicacoes/metodologia-de-calculo-dos-titulos-da-divida-interna/2008/26).
- Data catalogada pelo Tesouro Transparente: 01/01/2008.
- Data editorial: não indicada no conteúdo do documento.
- Metadados do PDF: criado em 07/09/2008 e modificado em 24/04/2024.
- Nome do arquivo recebido: `Metodologia de Calculo - 290424.pdf`.
- [Baixar o PDF](metodologia-calculo-tpf-stn.pdf).
- SHA-256:
  `1dca58d55be1fadcfdd039faf83cbab429026c87c6f7bd31847f2ce8d3ae536d`.

O PDF é mantido no repositório sem alteração do conteúdo original. O hash
permite verificar que o arquivo usado na implementação e nos testes continua
sendo exatamente a mesma versão.

A cópia atualmente publicada pelo Tesouro possui o mesmo SHA-256 da cópia
preservada neste repositório.

## Documento complementar

O [Caderno de Fórmulas do SELIC](metodologia-calculo-letras-notas-stn.md)
complementa esta referência na formação de valores nominais, juros pró-rata,
amortizações e eventos dos mesmos títulos custodiados no SELIC.

O PYield implementa a LFT comum de código SELIC `210100`, não as séries `LFT-A`
ou `LFT-B`. Seu VNA é obtido diretamente do arquivo diário oficial do Banco
Central; o fator acumulado não é recalculado localmente. Portanto, diferenças
entre os documentos sobre a precisão desse fator não alteram o resultado atual
da biblioteca.

## Escopo

A metodologia se aplica aos títulos descritos no documento:

- LTN;
- LFT;
- NTN-B;
- NTN-C;
- NTN-F.

`ntnb1` e `ntnbp` são títulos vendidos exclusivamente pelo Tesouro Direto e não
estão no escopo desta referência. Suas regras devem ser verificadas em fontes
próprias antes de qualquer alteração de precificação.

A ANBIMA pode ser a fonte dos dados de mercado utilizados pela biblioteca, mas
não é a referência normativa adotada aqui para as fórmulas e precisões de
precificação dos títulos de leilão.

## Regras de precisão

`A` significa arredondar e `T` significa truncar.

| Variável | LTN | LFT | NTN-B | NTN-C | NTN-F |
| --- | ---: | ---: | ---: | ---: | ---: |
| Juros semestrais | — | — | A-6 | A-6 | A-5 |
| Projeções | — | — | A-2 | A-2 | — |
| Fator acumulado da Selic | — | A-16 | — | — | — |
| Fluxos descontados | — | — | A-10 | A-10 | A-9 |
| Fator pró-rata das projeções | — | — | T-14 | T-14 | — |
| Fator acumulado do índice de preços | — | — | T-16 | T-16 | — |
| Taxa de retorno em percentual | T-6 | T-6 | T-6 | T-6 | T-6 |
| VNA ou VNA projetado | — | T-6 | T-6 | T-6 | — |
| PU | T-6 | T-6 | T-6 | T-6 | T-6 |
| Exponencial de dias | T-14 | T-14 | T-14 | T-14 | T-14 |
| Cotação percentual | — | T-4 | T-4 | T-4 | — |
| Valor financeiro | T-2 | T-2 | T-2 | T-2 | T-2 |

As taxas da API são representadas em formato decimal. Por isso, truncar uma
taxa percentual em seis casas equivale a truncar a taxa decimal em oito casas.

As cotações de LFT, NTN-B e NTN-C são representadas pela API em base 1. O
truncamento em seis casas nessa representação equivale ao truncamento em quatro
casas da cotação percentual em base 100 usada no documento.

## Rastreabilidade no código

- A normalização da taxa de entrada está centralizada em
  `pyield/tpf/titulos/_utils.py`.
- As fórmulas de preço e cotação ficam em `pyield/tpf/titulos/`.
- O VNA projetado de NTN-B e NTN-C fica em `pyield/tpf/vna/`.
- Os exemplos numéricos das páginas 3 a 12 são executados como doctests nas
  funções públicas correspondentes.

Os fatores acumulados recebidos de fontes oficiais não são reconstruídos pela
precificação quando o VNA já é fornecido. O cálculo de valor financeiro também
não faz parte atualmente das funções escalares de PU.

## Atualização da referência

Uma nova edição identificada da metodologia deve ser adicionada como outro
arquivo, sem sobrescrever silenciosamente esta referência. Mudanças de regra
devem atualizar esta página, a implementação afetada e os testes de referência
no mesmo conjunto de alterações.
