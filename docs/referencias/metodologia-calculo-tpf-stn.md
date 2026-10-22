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

Esta metodologia não é a referência de precificação da NTN-B1 (Tesouro Educa+
e Tesouro RendA+), que é vendida exclusivamente pelo Tesouro Direto e possui
documentação específica. Consulte a [metodologia de cálculo da NTN-B1 do
Tesouro Direto](metodologia-calculo-ntnb1.md).

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

As cotações de LFT, NTN-B e NTN-C são calculadas e retornadas em base 100,
truncadas em quatro casas, como no documento. Os cupons de NTN-B e NTN-C usam
seis casas e cada fluxo descontado é arredondado em dez casas antes da soma.
O PU é calculado como `VNA * cotacao / 100`, truncado em seis casas.

## VNA-base mensal da NTN-B

A fórmula por razão de números-índice é explicitada na seção 7, item 1
(páginas impressas 20–21), do
[Caderno de Fórmulas do Selic](metodologia-calculo-letras-notas-stn.md).
Esse documento também determina seis casas sem arredondamento no VNA.
O truncamento T-16 do fator acumulado vem da tabela de precisões do manual
de leilões, reproduzida acima; são referências complementares.

`yd.vna.calcular_base("NTN-B", indice_atual)` aplica a atualização acumulada
desde a base original, selecionada internamente. Para a série da NTN-B,
a referência original é R$ 1.000 em
15/07/2000, associada ao número-índice do IPCA de junho de 2000, `1614,62`.
A referência mensal do dia 15 usa o índice do mês anterior:

```text
fator_acumulado = T16(indice_atual / indice_base)
vna_base_atual = T6(vna_base * fator_acumulado)
```

São dois truncamentos, nesta ordem: dezesseis casas no fator acumulado e seis
no VNA resultante. Os números-índice não são substituídos pela taxa mensal
arredondada. Não há pró-rata de dias nem arredondamento da inflação projetada.
A implementação usa `Decimal` com precisão local de 28 dígitos.

Calcular pela razão desde a origem não pressupõe uma recalibração ocasional.
Sem truncamentos intermediários, o produto das razões mensais equivale à
razão entre os índices final e inicial. A equivalência deixa de ser exata
quando cada VNA mensal é truncado antes de alimentar o mês seguinte.

O VNA do mês anterior já sofreu truncamento. Usá-lo como nova base não preserva
toda a precisão do fator acumulado desde a origem. Por exemplo, em 15/10/2000,
partir da base original produz `1031,778375`, igual ao publicado; partir do
VNA de setembro produz `1031,778374`. Encadear todos os meses de julho de 2000
a outubro de 2026 termina em `4766,334768`, enquanto a referência publicada é
`4766,335112`.

### Registro da validação histórica

Os testes em `tests/vna/test_calcular_base.py` usam as 316 referências da NTN-B
preservadas no Parquet do Tesouro, de 15/07/2000 a 15/10/2026, e a resposta
bruta do agregado 1737, variável 2266, do IBGE, capturada em 22/10/2026 para
junho de 2000 a setembro de 2026. O JSON está preservado byte a byte em
`tests/vna/data/ipca_indices_200006_202609.json`.

A fórmula desde a base original reproduz exatamente 315 das 316 referências,
incluindo a inicial. Há uma divergência registrada separadamente, sem tolerância
nem alteração da referência:

| Referência | VNA publicado pelo Tesouro | VNA calculado com o índice do IBGE |
| --- | ---: | ---: |
| 15/02/2014 | 2376,026557 | 2376,020363 |

O índice de janeiro de 2014 retornado atualmente pelo IBGE é `3836,37`.
Entretanto, o [fascículo de janeiro de 2014 do próprio IBGE](https://biblioteca.ibge.gov.br/visualizacao/periodicos/236/inpc_ipca_2014_jan.pdf),
na tabela "Série histórica do IPCA", página impressa 16 (página 17 do PDF),
registra `3836,38`. Com esse valor, a mesma fórmula retorna `2376,026557`,
exatamente o VNA publicado pelo Tesouro.

Já o [fascículo de fevereiro de 2014](https://biblioteca.ibge.gov.br/visualizacao/periodicos/236/inpc_ipca_2014_fev.pdf),
na mesma tabela, registra janeiro com `3836,37`. A diferença entre as duas
publicações explica numericamente a divergência encontrada. Não foi localizado
um comunicado formal que estabeleça o motivo e a data da alteração; não se
presume revisão metodológica nem acesso à implementação interna do Tesouro.
Os testes preservam a comparação com a série atual, sem modificar os índices
nem os VNAs de referência para eliminar a divergência.

A aderência histórica é evidência de compatibilidade com a fórmula, não acesso
à implementação interna do Tesouro. Tampouco identifica isoladamente a precisão
intermediária do fator; o T-16 é adotado da referência documental.

## Verificação do VNA-base da NTN-C

O teste em `tests/vna/test_calcular_base_ntnc.py` exercita a função pública
`yd.vna.calcular_base` com os números-índice do IGP-M da FGV disponibilizados
pelo Ipeadata. Usa o JSON já preservado em
`tests/igpm/data/igpm_ipeadata_202609.json` e o Parquet oficial de NTN-C,
sem alterar os valores de referência. Complementa o teste de reconstrução
que já existia em `tests/igpm/test_indices.py`.

| Série por anos de vencimento | Referências verificadas | Origem do cálculo | Resultado |
| --- | ---: | --- | --- |
| 2005, 2008, 2011, 2017, 2021 e 2031 | 316, de 01/07/2000 a 01/10/2026 | R$ 1.000 em 01/07/2000; índice de junho de 2000 | Todas iguais ao publicado |
| 2002 e 2006 | 78, de 01/07/2000 a 01/12/2006 | Emissão de 03/12/1999, com ajuste inicial proporcional | Todas iguais ao publicado |

Na primeira série, o cálculo é diretamente
`T6(1000 * T16(indice_atual / indice_junho_2000))`. O índice de destino
corresponde ao mês anterior à referência do VNA, que ocorre no primeiro dia
do mês.

Na série de 2002/2006, o teste histórico existente considera 29 dias de
atualização em dezembro de 1999. A biblioteca prepara internamente a base sem
truncamento intermediário, usando os índices históricos constantes de novembro
(`174,939`) e dezembro (`178,099`) de 1999:

```text
expoente_inicial = T14(29 / 31)
base_ajustada = 1000 * (indice_dezembro_1999 / indice_novembro_1999) ** expoente_inicial
vna_mensal = T6(base_ajustada * T16(indice_atual / indice_dezembro_1999))
```

A base ajustada é mantida como `Decimal` com 28 dígitos de precisão.
Substituí-la pelo primeiro VNA mensal publicado, já truncado, perde precisão:
em 01/09/2000, usar o VNA de 01/07/2000 como base retorna `1091,045593`,
enquanto o publicado é `1091,045594`.

As 394 referências confirmam a aplicação do cálculo por razão de índices
à NTN-C, com a origem própria de cada série. O teste chama
`yd.vna.calcular_base("NTN-C", indice_atual, vencimento=...)`: a seleção do
vencimento e a preparação da origem pertencem à implementação. A reconstrução
independente no teste verifica o resultado contra o Parquet e a fórmula
anteriormente validada. Os valores de referência permanecem inalterados.

## VNA realizado da NTN-B entre datas-base

Em uma data entre dois VNAs mensais já publicados, `yd.vna.valor("NTN-B", data)` combina duas
fontes oficiais:

- o VNA no início da vigência vem da planilha do Tesouro Nacional;
- os números-índice do IPCA vêm do agregado 1737 do
  [SIDRA/IBGE](https://sidra.ibge.gov.br/tabela/1737).

Para uma data `d` entre as datas-base `t0` e `t1`, a sequência de precisão é:

```text
fator_ipca = T16(indice_t1 / indice_t0)
pro_rata = T14((d - t0) / (t1 - t0))
vna_d = T6(vna_t0 * fator_ipca ** pro_rata)
```

Os números-índice correspondem aos meses que atualizam cada data-base. Assim,
na vigência de 15/07/2026 a 15/08/2026, são usados os índices de junho e
julho de 2026.

Não se deve obter `fator_ipca` dividindo os dois VNAs mensais. Esses VNAs já
foram truncados em seis casas, e sua razão perde parte da precisão do fator de
preços. O caso de 13/08/2026 evidencia a diferença:

```text
vna_t0 = 4739,424756
indice_t0 = 7652,37
indice_t1 = 7657,73
fator_ipca = 1,0007004365967667
pro_rata = 0,93548387096774
vna_13_08_2026 = 4742,530180
```

Essa reconstrução é usada apenas quando os dois VNAs mensais e os dois
números-índice já estão disponíveis. Ela não transforma `yd.vna.valor("NTN-B", data)` em uma
função de projeção.

## Rastreabilidade no código

- A normalização da taxa de entrada está centralizada em
  `pyield/tpf/titulos/_utils.py`.
- As fórmulas de preço e cotação ficam em `pyield/tpf/titulos/`.
- O VNA projetado de NTN-B e NTN-C fica em `pyield/vna/`.
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
