# Novidades na versão 0.63.0

Esta versão ajusta as escalas decimais dos indicadores e do contexto de VNA, adiciona `yd.di_over_serie` e inclui os números-índice da inflação em `yd.vna.contexto`. Os índices do IGP-M passam a cobrir agosto de 1994 em diante, com três casas decimais.

Há mudanças no esquema dos DataFrames e na cobertura histórica. Quem compara esquemas, concatena dados persistidos ou usa posições de colunas deve revisar essas integrações antes de atualizar. As políticas de consultas sem cache e retentativas introduzidas na [0.62.0](<https://crdcj.github.io/PYield/release-0.62.0/>) permanecem.

## Escalas decimais dos indicadores

As escalas passam a acompanhar a precisão publicada em cada modalidade, sem zeros adicionais para acomodar outra série. Os valores continuam sendo lidos diretamente do texto da fonte, sem passagem por `float`, e as taxas continuam em unidade decimal.

| Dado | 0.62.0 | 0.63.0 |
| --- | --- | --- |
| Selic Over e DI Over anualizados; Selic Meta | `Decimal(18, 10)` | `Decimal(18, 4)` |
| Selic Over e DI Over diários | `Decimal(18, 10)` | `Decimal(18, 8)` |
| PTAX | `Decimal(18, 4)` | `Decimal(18, 4)` |
| Número-índice do IGP-M | `Decimal(18, 10)` | `Decimal(18, 3)`, desde agosto de 1994 |
| Inflação realizada em `vna.contexto` | `Decimal(38, 18)` | `Decimal(38, 4)` |
| Projeção informada em `vna.contexto` | `Decimal(38, 18)` | `Decimal(38, escala da entrada)` |

As duas casas percentuais das taxas anuais correspondem a quatro casas em unidade decimal; as seis casas percentuais das diárias correspondem a oito. Escalares, colunas de séries e resultados vazios seguem a escala da modalidade. Por exemplo, a taxa anual antes representada por `Decimal("0.1315000000")` passa a ser `Decimal("0.1315")`: o valor econômico é o mesmo, mas a escala e a representação textual mudam. Escalares sem dados continuam retornando `Decimal("NaN")`.

### Migração dos esquemas

Atualize os esquemas esperados e os tipos usados em persistência, junções e concatenações. Se uma integração precisar manter temporariamente a escala anterior, converta explicitamente na aplicação:

```python
import polars as pl
import pyield as yd

serie = yd.selic.over_serie(ultimos=5)
serie_compativel = serie.with_columns(taxa=pl.col("taxa").cast(pl.Decimal(18, 10)))
```

Esse cast acrescenta zeros à representação; não aumenta a precisão da fonte nem recupera índices do IGP-M anteriores ao novo corte histórico. Para análises que exigem ponto flutuante, use `float(taxa)` ou `pl.col("taxa").cast(pl.Float64)`, considerando a possível aproximação binária.

## Série histórica de DI Over

`yd.di_over_serie` consulta um intervalo inteiro de DI Over ou os últimos registros disponíveis. Reutiliza o SGS do Banco Central e dispensa laços de consultas escalares e a montagem manual do DataFrame.

```python
serie_anual = yd.di_over_serie("27-02-2025", "28-02-2025")
serie_diaria = yd.di_over_serie("27-02-2025", "28-02-2025", anualizada=False)
ultimas_taxas = yd.di_over_serie(ultimos=5)
```

A assinatura é `yd.di_over_serie(inicio=None, fim=None, *, ultimos=None, anualizada=True)`. Informe `inicio` ou `ultimos`. O intervalo é inclusivo e `fim=None` considera a data atual no Brasil. `ultimos`, quando informado, tem prioridade sobre o intervalo e deve ser maior que zero. Intervalos longos são divididos automaticamente em blocos seguros para o SGS.

O retorno tem `data` do tipo `Date` e `taxa` do tipo `Decimal(18, 4)` na modalidade anual ou `Decimal(18, 8)` na diária. A consulta usa SGS 4389 (anualizada, base 252 dias úteis) ou SGS 12 (diária), sem recalcular uma modalidade a partir da outra. Datas sem dados são omitidas; resultados vazios preservam o esquema. A consulta escalar `yd.di_over` permanece disponível. Veja a [referência de DI Over](<https://crdcj.github.io/PYield/di-over/>).

## Contexto de VNA com números-índice

`yd.vna.contexto` passa a incluir quatro colunas:

| Coluna | Tipo Polars | Significado |
| --- | --- | --- |
| `competencia_anterior` | `Date` | Primeiro dia do mês anterior à `competencia` |
| `indice_anterior` | `Decimal(18, 2)` para IPCA; `Decimal(18, 3)` para IGP-M | Número-índice da competência anterior; nulo se ausente |
| `indice_atual` | Mesmo tipo de `indice_anterior` | Número-índice da competência da vigência; nulo se ausente |
| `origem_indice` | `String` | `IBGE` ou `FGV/Ipeadata`; nula se ambos os índices estiverem ausentes |

Os índices das duas competências são consultados independentemente da taxa mensal e da publicação dos VNAs. Não são estimados por projeção nem pela razão entre VNAs. Na NTN-C, a taxa mensal continua vindo do SGS/BCB, enquanto os índices vêm da FGV via Ipeadata; essas fontes podem ter disponibilidades diferentes. A nova consulta pode propagar erros operacionais da fonte, como as demais consultas externas.

```python
contexto = yd.vna.contexto("NTN-B", "30-06-2026")
indices = contexto.select(
    "competencia_anterior", "competencia", "indice_anterior", "indice_atual"
)
```

A inflação realizada preserva as quatro casas das taxas do IPCA ou IGP-M, sem preenchimento até 18 casas. Projeções informadas preservam a escala da entrada, sem arredondamento; inflação indisponível usa escala 4. Os VNAs permanecem com seis casas. A conversão de entradas percentuais textuais, como `"0,45%"`, constrói o `Decimal` pelo expoente, evitando arredondamento em uma divisão intermediária por 100.

### Migração do contexto

Selecione colunas pelo nome para evitar depender de posições alteradas pela inclusão dos novos campos. Ao comparar esquemas ou concatenar contextos, considere a escala dos índices de cada título e a escala da projeção informada.

`situacao_inflacao` continua descrevendo somente a taxa mensal. `pendencias` continua indicando falta de base ou taxa para projeção; uma lista vazia não atesta disponibilidade dos índices ou do VNA final. Verifique os campos nulos individualmente.

A razão `indice_atual / indice_anterior` preserva informação que a taxa mensal arredondada pode perder. Passar essa taxa a `yd.vna.projetado` aplica as regras de projeção e não reproduz necessariamente o pró-rata realizado da NTN-B. `yd.vna.valor` usa os índices do IPCA entre duas referências publicadas para NTN-B; para NTN-C, usa a razão entre os VNAs publicados. O contexto não calcula VNA nem elimina a exigência dessas referências. Veja a [documentação de VNA](<https://crdcj.github.io/PYield/vna/#contexto-da-vigencia>).

## Cobertura e organização do IGP-M

Os números-índice do IGP-M passam a usar `Decimal(18, 3)`, com cobertura a partir de agosto de 1994, base 100 da série. O corte evita impor a precisão do histórico antigo às consultas usadas em cálculos financeiros em reais: julho de 1994 ainda exige quatro casas na fonte (`92.9684`).

Competências anteriores são omitidas nas séries e retornam `Decimal("NaN")` na consulta escalar. Intervalos inteiramente anteriores retornam vazios, preservando o novo esquema; intervalos que atravessam o corte retornam apenas agosto de 1994 em diante. O limite também vale para `ultimos`. No contexto de VNA de agosto de 1994, `indice_anterior` fica nulo. A cobertura das taxas mensais segue a série SGS 189.

Se sua aplicação usa o histórico anterior, consulte diretamente a [série completa na API do Ipeadata](<https://www.ipeadata.gov.br/api/odata4/ValoresSerie(SERCODIGO=> "IGP12_IGPM12")) para preservar a precisão original. Um cast da saída atual não recupera esse período. Veja também a [referência do IGP-M](<https://crdcj.github.io/PYield/igpm/>).

O IGP-M passa a ser organizado no pacote `pyield/igpm/`, que reúne as implementações dos números-índice e do calendário. As taxas continuam sendo reexportadas diretamente do SGS/BCB. As chamadas públicas `yd.igpm.taxa`, `taxa_serie`, `indice`, `indice_serie` e `calendario` permanecem iguais; somente os caminhos internos de implementação mudam.
