# Valor nominal atualizado

As funções `yd.vna.vigencia("NTN-B", data)` e `yd.vna.vigencia("NTN-C", data)` retornam o início inclusivo e o fim exclusivo da vigência do VNA, sem consultar a rede. Essas datas permitem selecionar a base e a projeção mensal na aplicação, inclusive quando operação e liquidação pertencem a vigências diferentes.

- NTN-B: dia 15 até o dia 15 seguinte.
- NTN-C: primeiro dia do mês até o primeiro dia do mês seguinte.

A aplicação fornece o VNA ao calcular o preço completo dos títulos indexados:

```python
cotacao = yd.ntnb.cotacao("14-08-2026", "15-08-2026", 0.132098)
pu = yd.ntnb.pu(4742.6373, cotacao)
# Decimal('4880.439369')
```

A seleção do VNA oficial ou projetado permanece explícita no consumidor.

## Consulta por título

```python
import pyield as yd

ultimo = yd.vna.ultimo("NTN-B")  # DataFrame com data e VNA publicados
historico = yd.vna.historico("NTN-C", vencimento="01-01-2031")
valor = yd.vna.valor("NTN-B", "15-12-2025")
projecao_ntnb = yd.vna.projetado("NTN-B", "30-06-2026", 4731.856412, "0.45%")
projecao_lft = yd.vna.projetado(
    "LFT", "18-09-2026", 19905.773236,
    selic="13.65%",
)
```

`projetado` é a operação comum de projeção para todos os títulos. Para NTN-B e NTN-C, `inflacao` representa a variação mensal e aceita `float` ou `Decimal` em unidade decimal (`0.0045`) ou string percentual (`"0.45%"` ou `"0,45%"`). Para LFT, `selic` representa a taxa anual informada em formato decimal ou percentual explícito. Para reproduzir o acruamento oficial, derive a taxa Selic diária publicada pelo SGS 11. Como ela é publicada em percentual, `0,050788%` equivale à taxa decimal `0,00050788` e ao fator derivado `1,00050788`; converta essa taxa diária para uma taxa anual equivalente. A série 1178 é a Selic Over anualizada e serve apenas como aproximação. A data-base é o último dia útil anterior à data solicitada. A projeção usa a quantidade de dias úteis até a data solicitada e retorna `Decimal`. Intervalos diferentes de um dia útil são calculados normalmente e registrados no log. Todos os cálculos de VNA, inclusive o genérico `calcular_vna`, usam `Decimal` com 28 dígitos de precisão e retornam `Decimal`, para aplicar exatamente os truncamentos e arredondamentos da metodologia (fator do IPCA T-16, pró-rata T-14, projeção A-2 percentual, equivalente a A-4 decimal, e VNA T-6). Entradas `float` são convertidas pela sua representação textual, de modo que `4731.856412` vira `Decimal('4731.856412')`.

Por exemplo, o SGS 11 publicou `0,050788%` para 18/09/2026. Isso corresponde ao fator diário `1,00050788`; aplicado ao VNA de 17/09/2026 (`19905,773236`), produz `19915,882980111...`, truncado para `19915,882980`, exatamente o valor publicado pelo BCB. A série 1178 publicou `13,65%` anualizado na mesma data; usá-la diretamente produz uma pequena diferença por causa do arredondamento.

`ultimo` retorna a última referência de cada série; para NTN-C sem filtro de vencimento, pode retornar mais de uma linha. Datas e valores permanecem juntos. `historico` e `ultimo` retornam a coluna `vna` como `Decimal(18, 6)`, preservando exatamente as seis casas publicadas pelo Tesouro Nacional. Uma célula da planilha com data válida e valor ilegível gera erro, em vez de ser descartada e alterar silenciosamente o pró-rata entre referências. Não há projeção automática. A LFT não possui histórico mensal nem operação de última referência nessa API; sua projeção usa a quantidade de dias úteis do intervalo, base 252 e taxa anual constante dentro da mesma operação `projetado`. Quando o intervalo não corresponde a exatamente um dia útil, o cálculo é feito normalmente e a ocorrência é registrada no log. Em uma segunda-feira, por exemplo, a base é a sexta-feira útil anterior.

Use apenas `yd.vna` para operações de VNA. Os aliases `vna`, `vnas`, `vna_projetado` e `vigencia` foram removidos dos módulos dos títulos. As implementações internas e suas metodologias foram preservadas.

## Cálculo do VNA a partir da origem

`yd.vna.calcular_base(titulo, indice_atual, vencimento=None)` calcula uma nova base mensal da NTN-B ou NTN-C com o número-índice realizado do IPCA ou IGP-M, respectivamente, ou o VNA diário da LFT com o fator Selic acumulado desde 01/07/2000. As bases originais e os índices iniciais são constantes internas. O usuário fornece apenas o título e o índice ou fator novo; para NTN-C, informa também o vencimento para selecionar a série.

A função não consulta a rede: pode ser usada assim que o IBGE ou a FGV divulgar o índice, mesmo que o Tesouro ainda não tenha publicado o novo VNA. Aceita `float` ou `Decimal` e retorna `Decimal` com seis casas. Para LFT, o fator acumulado deve ser fornecido pronto; a função não busca nem acumula as taxas diárias. Não há preenchimento automático nas consultas de VNA publicado.

O índice corresponde ao mês anterior à nova data-base. Para NTN-B, a referência ocorre no dia 15; para NTN-C, no primeiro dia do mês. Os exemplos abaixo conferem o cálculo com valores conhecidos publicados pelas fontes oficiais.

### NTN-B: base de 15/08/2026

```python
indice_atual = 7657.73  # Pode ser obtido com yd.ipca.indice("01-07-2026")
nova_base = yd.vna.calcular_base("NTN-B", indice_atual)  # 15/08/2026
# Decimal('4742.744422')
assert nova_base == yd.vna.valor("NTN-B", "15-08-2026")
```

### NTN-C: base de 01/10/2026, série de 2031

```python
indice_atual = 1225.846  # Pode ser obtido com yd.igpm.indice("01-09-2026")
nova_base = yd.vna.calcular_base(
    "NTN-C", indice_atual, vencimento="01-01-2031",  # 01/10/2026
)
# Decimal('6671.452284')
assert nova_base == yd.vna.valor("NTN-C", "01-10-2026", "01-01-2031")
```

Para NTN-C, o vencimento é obrigatório e a seleção usa seu ano. Os vencimentos 2005, 2008, 2011, 2017, 2021 e 2031 usam R$ 1.000 em 01/07/2000. Os de 2002 e 2006 incluem internamente o ajuste inicial da emissão de 03/12/1999, sem perda de precisão intermediária. Um vencimento sem série conhecida retorna `Decimal('NaN')`. Para NTN-B e LFT, omita o vencimento.

### LFT: VNA válido em 03/06/2024

O fator abaixo foi conferido com o produtório da Selic diária SGS 11 desde 01/07/2000 até 31/05/2024, inclusive. Ele já incorpora toda a acumulação; uma taxa diária ou anual isolada não é essa entrada.

```python
fator_acumulado = 14.903011480282196
nova_base = yd.vna.calcular_base("LFT", fator_acumulado)
# Decimal('14903.011480')
assert nova_base == yd.vna.valor("LFT", "31-05-2024")
```

O arquivo APC238 gerado em 31/05/2024 informa o VNA válido para 03/06/2024, próximo dia útil. A consulta `valor` recebe a data do arquivo; o fator acumulado para uma data de vigência inclui as taxas até o dia útil anterior. A função de cálculo não seleciona datas nem consulta relatórios automaticamente.

### Precisão e referências

Para NTN-B e NTN-C, a sequência é `T6(vna_base * T16(indice_atual / indice_base))`: trunca o fator acumulado em dezesseis casas e o VNA em seis, com precisão interna de 28 dígitos. Não arredonda o fator nem usa a taxa mensal percentual arredondada. O cálculo da base não tem pró-rata de dias nem arredondamento de projeção. Para LFT, a sequência é `T6(1000 * A16(fator_acumulado))`: arredonda o fator em dezesseis casas (meio para cima) e trunca o VNA em seis. A base original de R$ 1.000 também fica embutida na biblioteca.

Usar o VNA do mês anterior já truncado pode perder uma unidade na sexta casa; encadear essas bases acumula a perda. Por isso, a referência de partida para reconstrução exata deve ser a original, selecionada pela biblioteca. Consulte o registro da fórmula, da validação histórica e da divergência identificada em fevereiro de 2014 na [metodologia da STN](<https://crdcj.github.io/PYield/referencias/metodologia-calculo-tpf-stn/#vna-base-mensal-da-ntn-b>). O valor calculado não substitui a consulta ao VNA publicado por `yd.vna.valor`. O registro da reconstrução e das datas da LFT está na [metodologia complementar](<https://crdcj.github.io/PYield/referencias/metodologia-calculo-letras-notas-stn/#conferencia-da-reconstrucao-desde-a-origem>).

## Interpolação e projeção

`calcular_vna` recebe um DataFrame porque trabalha sobre uma série de referências: retorna o ponto publicado na data ou seleciona as duas referências que a cercam e calcula o pró-rata. Não exige ordenação prévia nem usa necessariamente o último VNA da série. Para NTN-C, filtre o vencimento para não misturar séries.

```python
referencias = yd.vna.historico("NTN-C", vencimento="01-01-2031")
valor = yd.vna.calcular_vna(referencias, "16-12-2025")
# Decimal('6449.641358')
```

O VNA de entrada pode ser `float`; não é necessário construir objetos `Decimal`. A data da consulta aceita os formatos brasileiros `DD-MM-YYYY` e `DD/MM/YYYY`, além de `YYYY-MM-DD` e objetos de data. No DataFrame, a coluna `data` deve ser do tipo Polars `Date`, como já vem de `historico`. O retorno permanece `Decimal`.

O parâmetro opcional `fator_variacao` substitui a razão entre os dois VNAs, mas ainda exige as duas referências para determinar o intervalo. É um fator multiplicativo: `1.0045` representa alta de 0,45%. A função retorna `Decimal('NaN')` fora do intervalo publicado, mesmo com esse fator informado. Para NTN-B, prefira `yd.vna.valor` para reproduzir o valor oficial: ela usa os números-índice do IPCA e os truncamentos próprios desse cálculo, em vez de inferir o fator pela razão entre VNAs publicados.

Para projetar, use `projetado`, que já recebe um VNA-base escalar. Se a base vem do histórico, selecione a referência do **início da vigência da data-alvo**. Ela só coincide com o último VNA publicado quando essa última referência é o início da vigência desejada. Por exemplo, em um cenário de inflação de 0,45%:

```python
data = "30-06-2026"
inicio, fim = yd.vna.vigencia("NTN-B", data)
referencias = yd.vna.historico("NTN-B")
base = referencias.filter(pl.col("data") == inicio).item(0, "vna")
valor = yd.vna.projetado("NTN-B", data, base, "0.45%")
# Decimal('4742.491138')
```

O exemplo pressupõe que a referência inicial está disponível no histórico. Se o VNA da próxima referência já tiver sido publicado, os dias anteriores a ela continuam pertencendo à vigência anterior. Para calcular esses dias com a inflação realizada, use `yd.vna.valor`; na NTN-B, ela consulta os índices do IPCA. `projetado` aplica as regras de arredondamento da inflação projetada e não substitui esse cálculo realizado a partir dos números-índice.

## Migração das entradas de inflação

O parâmetro `inflacao` de NTN-B e NTN-C em `yd.vna.projetado` recebia números em percentual. Substitua `inflacao=0.45` por `inflacao="0.45%"` ou `inflacao=0.0045` para manter a inflação mensal de 0,45%. Pelo contrato atual, o número puro `0.45` representa 45%. Strings sem `%`, como `"0.45"`, são rejeitadas. Os resultados e as regras de arredondamento do cálculo permanecem iguais quando se informa a mesma inflação econômica.

## Contexto da vigência

`yd.vna.contexto` reúne os parâmetros disponíveis para uma vigência de NTN-B ou NTN-C em um DataFrame com uma linha. Inclui as datas da vigência, os VNAs publicados no início e no fim, a competência da inflação, sua origem e situação (`realizado`, `projetado` ou `indisponivel`) e uma lista de pendências. Dados numéricos indisponíveis são nulos. Para NTN-C, informe `vencimento`.

```python
contexto = yd.vna.contexto("NTN-B", "30-06-2026")
contexto_ntnc = yd.vna.contexto(
    "NTN-C", "15-06-2026", vencimento="01-01-2031",
)
```

A projeção opcional `inflacao_projetada` só é usada quando a taxa realizada estiver indisponível. Esse parâmetro aceita as mesmas entradas de `yd.vna.projetado`: `0.0045`, `Decimal("0.0045")`, `"0.45%"` ou `"0,45%"` representam 0,45%. As entradas são convertidas para `Decimal` sem arredondamento; a coluna `inflacao` permanece em unidade decimal e pode ser passada diretamente a `yd.vna.projetado`. O contexto não calcula o VNA nem busca projeções automaticamente. Ele reflete os dados disponíveis no momento da consulta, sem reconstruir o que era conhecido na data solicitada. Para comparar operação e liquidação, consulte cada data e compare `inicio_vigencia`.

### Transição e atenção à divulgação

Cada data seleciona sua própria base, inclusive quando um fim de semana separa operação e liquidação:

```python
d0 = yd.vna.contexto("NTN-B", "14-08-2026")
d1 = yd.vna.contexto("NTN-B", "17-08-2026")
mudou_vigencia = d0.item(0, "inicio_vigencia") != d1.item(0, "inicio_vigencia")
```

Os calendários permitem identificar projeções que merecem revisão, sem presumir que o índice já foi publicado. Por exemplo, para NTN-C:

```python
data_operacao = yd.hoje()
contexto = yd.vna.contexto(
    "NTN-C", data_operacao, vencimento="01-01-2031",
    inflacao_projetada="0,45%",
)
agenda = yd.igpm.calendario(contexto.item(0, "competencia").year)
diagnostico = contexto.join(agenda, on="competencia", how="left").with_columns(
    revisar_projecao=(
        (pl.col("situacao_inflacao") == "projetado")
        & (pl.col("data_divulgacao") <= data_operacao)
    ).fill_null(False)
)
```

Compare a divulgação com a **data de operação**, mesmo quando o contexto descreve a liquidação. D+1 não antecipa a publicação. Sem data oficial, o exemplo não inventa um alerta. Mensagens, cores, janelas de proximidade e a decisão de impedir uma precificação permanecem na aplicação. A inflação realizada pode estar disponível antes do VNA final da vigência.

Consulta e cálculo de VNA por título: valor, histórico, último e projeção.

## `calcular_base(titulo, indice_atual, vencimento=None)`

Calcula o VNA realizado da LFT, NTN-B ou NTN-C desde sua base original.

Permite calcular a referência mensal após a divulgação do índice pelo IBGE (IPCA) ou pela FGV (IGP-M), mesmo antes da publicação do VNA pelo Tesouro Nacional. Não consulta a rede e não calcula pró-rata entre datas-base. As bases históricas são selecionadas internamente pelo título e, para NTN-C, pela série de vencimento. Para LFT, recebe o fator Selic já acumulado desde 01/07/2000.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `titulo` | `TipoTitulo` | LFT, NTN-B ou NTN-C. | *required* |
| `indice_atual` | `float \| Decimal` | Número-índice realizado do IPCA (NTN-B) ou IGP-M (NTN-C), do mês anterior à nova data-base. Por exemplo, julho de 2026 para o VNA de 15/08/2026 da NTN-B ou 01/08/2026 da NTN-C. Aceita float ou Decimal; não recebe taxa percentual de inflação. Para LFT, é o fator multiplicativo Selic acumulado desde 01/07/2000 até a data final exclusiva de vigência. Não é taxa diária, taxa anual ou fator de apenas um dia. | *required* |
| `vencimento` | `DateLike \| None` | Obrigatório para selecionar a série da NTN-C; aceita os formatos de data da biblioteca. A seleção usa o ano de vencimento. Deve ser omitido para LFT e NTN-B. | `None` |

Returns:

| Name | Type | Description |
| --- | --- | --- |
| `Decimal` | `Decimal` | VNA-base calculado, truncado em seis casas decimais. Retorna `Decimal('NaN')` se não houver série de NTN-C para o vencimento. |

**Notes**

Para NTN-B e NTN-C, conforme a metodologia da STN, calcula `T6(vna_base * T16(indice_atual / indice_base))`. Primeiro trunca o fator acumulado em dezesseis casas; depois multiplica pela base e trunca o VNA em seis casas. Não arredonda os números-índice nem usa a taxa mensal percentual arredondada. O cálculo usa 28 dígitos de precisão, independentemente do contexto Decimal do chamador. Entradas float são convertidas pela representação textual.

Para LFT, calcula `T6(1000 * A16(indice_atual))`: arredonda o fator acumulado em dezesseis casas (meio para cima) e trunca o VNA em seis casas, com precisão local de 28 dígitos. A base de R$ 1.000 em 01/07/2000 é constante interna. O fator resulta do produtório de `1 + taxa_diaria`; a série SGS 11 publica a taxa em percentual ao dia, que deve ser dividida por 100. Preserve a precisão do produtório; truncar o VNA diariamente acumula perda.

O relatório APC238 gerado pelo BCB em um dia informa o VNA para o próximo dia útil. Por exemplo, o arquivo de 31/05/2024 informa o VNA válido para 03/06/2024 e inclui a taxa Selic de 31/05/2024. A consulta `yd.vna.valor("LFT", data)` seleciona a data do arquivo. Esta função não recebe datas nem acumula a série diária automaticamente.

A NTN-B usa R$ 1.000 em 15/07/2000 e IPCA de junho de 2000 (1614,62). A NTN-C dos vencimentos 2005, 2008, 2011, 2017, 2021 e 2031 usa R$ 1.000 em 01/07/2000 e IGP-M de junho de 2000 (183,745). São constantes internas; o usuário informa apenas o índice final.

Para NTN-C de 2002/2006, a origem é a emissão de 03/12/1999. A base ajustada é `1000 * (178.099 / 174.939) ** T14(29 / 31)`, mantida sem truncamento intermediário, com índice-base de dezembro de 1999 (178,099). As constantes de IGP-M foram verificadas com a série FGV/Ipeadata `IGP12_IGPM12`. Esse ajuste não é uma projeção mensal.

Usar o VNA mensal anterior já truncado como base perde precisão e pode diferir da série oficial na sexta casa. Encadear esses resultados acumula essa perda. Para reconstruir a série, a função mantém a base original e seu número-índice fixos. O resultado é calculado, não consultado na publicação do Tesouro; eventuais divergências devem ser investigadas.

**References**

- https://crdcj.github.io/PYield/referencias/metodologia-calculo-tpf-stn/
- https://crdcj.github.io/PYield/referencias/metodologia-calculo-letras-notas-stn/
- https://sidra.ibge.gov.br/tabela/1737

Raises:

| Type | Description |
| --- | --- |
| `ValueError` | Se o título for inválido, o índice não for positivo e finito, o vencimento da NTN-C estiver ausente ou malformado, ou houver vencimento informado para LFT ou NTN-B. |

Examples:

Usa o número-índice do IBGE para calcular a base de 15/08/2026:

```pycon
>>> indice_atual = 7657.73  # yd.ipca.indice("01-07-2026")
>>> calculado = yd.vna.calcular_base("NTN-B", indice_atual)
>>> calculado
Decimal('4742.744422')
>>> calculado == yd.vna.valor("NTN-B", "15-08-2026")
True
```

Calcula a base de 01/10/2026 da NTN-C da série com vencimento em 2031:

```pycon
>>> indice_atual = 1225.846  # yd.igpm.indice("01-09-2026")
>>> calculado = yd.vna.calcular_base(
...     "NTN-C", indice_atual, vencimento="01-01-2031"
... )
>>> calculado
Decimal('6671.452284')
>>> calculado == yd.vna.valor("NTN-C", "01-10-2026", "01-01-2031")
True
```

Usa o fator acumulado da Selic até 31/05/2024 (inclusive), conferido com a série SGS 11, para obter o VNA válido em 03/06/2024:

```pycon
>>> fator_acumulado = 14.903011480282196
>>> calculado = yd.vna.calcular_base("LFT", fator_acumulado)
>>> calculado
Decimal('14903.011480')
>>> calculado == yd.vna.valor("LFT", "31-05-2024")
True
```

## `calcular_vna(df, data, *, fator_variacao=None)`

Obtém o VNA publicado ou calcula o pró-rata entre pontos publicados.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `df` | `DataFrame` | DataFrame com as colunas `data` (Date) e `vna` (Decimal ou Float64), contendo no máximo um ponto publicado por data. | *required* |
| `data` | `DateLike` | Data para a qual o VNA será obtido ou interpolado. Aceita `DD-MM-YYYY`, `DD/MM/YYYY`, `YYYY-MM-DD` ou objeto de data. | *required* |
| `fator_variacao` | `float \| Decimal \| None` | Fator multiplicativo entre o ponto inicial e o final. Se omitido, é calculado pela razão entre os VNAs dos dois pontos. | `None` |

Returns:

| Name | Type | Description |
| --- | --- | --- |
| `Decimal` | `Decimal` | VNA publicado ou calculado por pró-rata exponencial. Retorna |
|  | `Decimal` | `Decimal('NaN')` quando não houver pontos publicados dos dois lados |
|  | `Decimal` | da data ou quando ela estiver fora do intervalo disponível. |

**Notes**

Um ponto publicado exatamente na data tem prioridade e ignora `fator_variacao`. O cálculo usa Decimal com 28 dígitos de precisão; o expoente do pró-rata é truncado em catorze casas e o resultado em seis casas. Entradas float são convertidas pela representação textual.

O DataFrame deve conter uma única série de VNA, como a retornada por `yd.vna.historico` com o filtro de vencimento quando necessário. A função seleciona as referências imediatamente anterior e posterior à data, sem exigir que o DataFrame esteja ordenado. Não extrapola após a última referência, mesmo quando `fator_variacao` é informado. Para projetar a partir de um VNA-base escalar, use `yd.vna.projetado`.

`fator_variacao` é multiplicativo: 1,0045 representa alta de 0,45%. Para reproduzir o VNA oficial da NTN-B entre referências, prefira `yd.vna.valor`, que obtém o fator dos números-índice do IPCA; a razão entre VNAs publicados pode produzir diferenças de truncamento.

Raises:

| Type | Description |
| --- | --- |
| `ValueError` | Se o VNA-base não for positivo ou a variação for menor ou igual a -100%. |

Examples:

Pró-rata entre referências da NTN-C com vencimento em 01/01/2031, consultadas no histórico publicado pelo Tesouro Nacional:

```pycon
>>> referencias = yd.vna.historico("NTN-C", vencimento="01-01-2031")
>>> yd.vna.calcular_vna(referencias, "16-12-2025")
Decimal('6449.641358')
```

## `contexto(titulo, data, vencimento=None, *, inflacao_projetada=None, atualizar=False)`

Consulta os parâmetros disponíveis para a vigência mensal do VNA.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `titulo` | `Literal['NTN-B', 'NTN-C']` | NTN-B ou NTN-C. | *required* |
| `data` | `DateLike` | Data contida na vigência, nos formatos aceitos pela biblioteca. | *required* |
| `vencimento` | `DateLike \| None` | Obrigatório para selecionar a série da NTN-C; não se aplica à NTN-B. | `None` |
| `inflacao_projetada` | `float \| Decimal \| str \| None` | Projeção mensal opcional em unidade decimal: 0.0045 representa 0,45%. Aceita também percentual explícito, como `"0.45%"` ou `"0,45%"`. Convertida para Decimal sem arredondar; usada somente se a taxa realizada estiver indisponível. Não busca projeções automaticamente. | `None` |
| `atualizar` | `bool` | Renova apenas o cache do histórico de VNA, como em historico. Não renova os caches de inflação. | `False` |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | DataFrame com uma linha, inclusive quando faltam parâmetros. Dados |
| `DataFrame` | numéricos indisponíveis são nulos, com pendências explícitas. |

**Output Columns**

titulo (String): Título consultado. data (Date): Data para a qual os parâmetros foram selecionados. inicio\_vigencia (Date): Início inclusivo, sem ajuste útil. fim\_vigencia (Date): Fim exclusivo, sem ajuste útil. competencia (Date): Primeiro dia do mês da inflação da vigência. vna\_base (Decimal(18, 6)): VNA publicado no início; nulo se ausente. vna\_fim (Decimal(18, 6)): VNA publicado no fim; nulo se ausente. inflacao (Decimal(38, 18)): Variação mensal decimal, realizada ou explicitamente projetada. Pode ser passada diretamente a projetado. situacao\_inflacao (String): "realizado", "projetado" ou "indisponivel". origem\_inflacao (String): "IBGE", "SGS/BCB" ou "informada"; nula sem taxa. pendencias (List\[String\]): "vna\_base\_indisponivel" e/ou "inflacao\_indisponivel". Lista vazia se ambos estiverem disponíveis.

**Notes**

Fontes: histórico do Tesouro Nacional; taxa do IPCA do IBGE e taxa do IGP-M do SGS/BCB. A competência é o mês do início da vigência: dia 15 para NTN-B e dia 1 para NTN-C. As consultas refletem os dados disponíveis agora, sem reconstruir o que era conhecido na data solicitada. A inflação realizada não comprova publicação do VNA final. Este contexto não calcula VNA, não infere inflação da razão entre VNAs e não estima uma base ausente. Projeções e dados oficiais permanecem identificados. Erros operacionais são propagados, sem tratá-los como falta de dados. Para detectar transição, compare inicio\_vigencia dos contextos de operação e liquidação; cada data seleciona sua própria base.

Raises:

| Type | Description |
| --- | --- |
| `ValueError` | Título ou data inválidos, vencimento incompatível/ausente, projeção não finita ou menor ou igual a -1, ou percentual textual inválido. |

Examples:

Consulta uma vigência histórica da NTN-B e verifica a competência da inflação, a base publicada e a disponibilidade dos parâmetros:

```pycon
>>> parametros = yd.vna.contexto("NTN-B", "30-06-2026")
>>> parametros.select(
...     "competencia", "vna_base", "inflacao", "situacao_inflacao", "pendencias"
... ).row(0)
(datetime.date(2026, 6, 1), Decimal('4731.856412'),
 Decimal('0.001600000000000000'), 'realizado', [])
```

Para a vigência atual da NTN-C, informa o vencimento e uma projeção de 0,45%, usada somente se a inflação realizada não estiver disponível. A situação e as pendências dependem dos dados publicados na consulta:

```pycon
>>> parametros = yd.vna.contexto(
...     "NTN-C", yd.hoje(), vencimento="01-01-2031",
...     inflacao_projetada="0,45%",
... )
>>> situacao = parametros.item(0, "situacao_inflacao")
>>> pendencias = parametros.item(0, "pendencias")
```

## `historico(titulo, vencimento=None, *, atualizar=False)`

Busca os VNAs mensais publicados pelo Tesouro Nacional.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `titulo` | `TipoTitulo` | NTN-B ou NTN-C. Histórico de LFT não está disponível. | *required* |
| `vencimento` | `DateLike \| None` | Filtro opcional de série para NTN-C. Se omitido, retorna todas as séries. Não se aplica à NTN-B. | `None` |
| `atualizar` | `bool` | Se True, baixa novamente o arquivo de VNA e renova seu cache de 60 segundos. Não atualiza o cache dos índices do IPCA. | `False` |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | DataFrame Polars ordenado por data. Sem série correspondente ao vencimento, retorna DataFrame vazio com o mesmo schema. |

**Output Columns**

- data (Date): Data de referência publicada.
- anos\_vencimento (List\[Int64\]): Apenas NTN-C; anos da série.
- vna (Decimal(18, 6)): Valor nominal atualizado publicado.

**Notes**

Fonte: planilhas de Valor Nominal de NTN-B e NTN-C do Tesouro Transparente. Preserva o schema das consultas específicas por título.

Raises:

| Type | Description |
| --- | --- |
| `ValueError` | Se o título não for NTN-B ou NTN-C, ou se houver filtro de vencimento para NTN-B. |

Examples:

Seleciona duas referências mensais da NTN-B no histórico publicado:

```pycon
>>> referencias = yd.vna.historico("NTN-B")
>>> referencias.filter(
...     pl.col("data").is_between(pl.date(2025, 12, 15), pl.date(2026, 1, 15))
... ).to_dicts()
[{'data': datetime.date(2025, 12, 15), 'vna': Decimal('4570.078408')},
 {'data': datetime.date(2026, 1, 15), 'vna': Decimal('4585.159356')}]
```

Para NTN-C, filtra o vencimento antes de interpolar para manter apenas uma série de VNA:

```pycon
>>> referencias = yd.vna.historico("NTN-C", vencimento="01-01-2031")
>>> yd.vna.calcular_vna(referencias, "16-12-2025")
Decimal('6449.641358')
```

## `projetado(titulo, data, vna_base, inflacao=None, *, selic=None)`

Projeta o VNA do título conforme sua metodologia.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `titulo` | `TipoTitulo` | LFT, NTN-B ou NTN-C. | *required* |
| `data` | `DateLike` | Data para a qual projetar o VNA. | *required* |
| `vna_base` | `float \| Decimal` | VNA-base correspondente ao início da vigência ou ao último dia útil anterior à data, conforme o título. | *required* |
| `inflacao` | `float \| Decimal \| str \| None` | Para NTN-B e NTN-C, variação mensal decimal; `0.0045` representa 0,45%. Aceita também percentual explícito, como `"0.45%"` ou `"0,45%"`. Para LFT, informe a taxa anual em `selic`. | `None` |
| `selic` | `float \| Decimal \| str \| None` | Obrigatória apenas para LFT. Aceita taxa decimal ou percentual explícito, como `"13.65%"`. Para reproduzir o acruamento oficial da LFT, use a taxa Selic diária publicada pelo SGS 11. Como ela é publicada em percentual, `0,050788%` equivale à taxa decimal `0,00050788` e ao fator derivado `1,00050788`. Converta essa taxa diária para uma taxa anual equivalente. A Selic Over do SGS 1178 é anualizada e serve apenas como aproximação. Para NTN-B e NTN-C, deve ser omitida. | `None` |

Returns:

| Name | Type | Description |
| --- | --- | --- |
| `Decimal` | `Decimal` | VNA projetado com seis casas; NaN para entradas nulas. |

**Notes**

Entradas numéricas são convertidas para Decimal pela representação textual; percentuais explícitos são convertidos para a unidade decimal sem arredondamento antes da aplicação das regras do cálculo. NTN-B e NTN-C seguem a metodologia STN: base truncada em seis casas, inflação percentual arredondada em duas casas (quatro na unidade decimal) e expoente em dias corridos truncado em catorze. LFT usa dias úteis, base 252 e capitalização pela taxa anual informada. A taxa Selic diária publicada pelo SGS 11 é a referência exata para o acruamento; o fator usado no cálculo é `1 + taxa` em formato decimal. A função não busca a taxa automaticamente nem faz projeções externas.

Raises:

| Type | Description |
| --- | --- |
| `ValueError` | Se o título for inválido, a taxa for informada para o título errado, ou houver violação dos limites da metodologia específica, ou percentual textual inválido. |

Examples:

```pycon
>>> import pyield as yd
>>> yd.vna.projetado("NTN-B", "30-06-2026", 4731.856412, "0.45%")
Decimal('4742.491138')
>>> yd.vna.projetado(
...     "LFT",
...     "18-09-2026",
...     19905.773236,
...     selic="13.65%",
... )
Decimal('19915.882987')
```

Projeta a NTN-C a partir da base de 01/12/2025 da série de 2031, em um cenário de inflação mensal de 0,45%. A taxa é uma hipótese explícita, não a inflação realizada desse mês:

```pycon
>>> yd.vna.projetado("NTN-C", "16-12-2025", 6450.107485, "0,45%")
Decimal('6464.135801')
```

## `ultimo(titulo, vencimento=None, *, atualizar=False)`

Busca a última referência publicada de cada série de VNA.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `titulo` | `TipoTitulo` | NTN-B ou NTN-C. Última publicação de LFT não está disponível. | *required* |
| `vencimento` | `DateLike \| None` | Filtro opcional da NTN-C. Sem filtro, retorna a última referência de cada série. Não se aplica à NTN-B. | `None` |
| `atualizar` | `bool` | Se True, baixa novamente o arquivo de VNA e renova seu cache de 60 segundos. Não atualiza o cache dos índices do IPCA. | `False` |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | DataFrame Polars com data e valor da última publicação de cada série. Retorna vazio quando o histórico consultado estiver vazio. |

**Output Columns**

- data (Date): Data de referência da última publicação da série.
- anos\_vencimento (List\[Int64\]): Apenas NTN-C; anos da série.
- vna (Decimal(18, 6)): VNA publicado nessa referência.

**Notes**

Fonte: históricos do Tesouro Nacional obtidos por `historico`. O resultado é publicado, sem projeção até a data atual. Preserva as colunas e os tipos do histórico.

Examples:

Extrai a data e o VNA da última referência da NTN-B. Os valores dependem da publicação mais recente do Tesouro Nacional:

```pycon
>>> referencia = yd.vna.ultimo("NTN-B")
>>> data_publicada = referencia.item(0, "data")
>>> vna_publicado = referencia.item(0, "vna")
```

Para NTN-C, seleciona uma série pelo vencimento. Sem esse filtro, o resultado pode conter mais de uma linha:

```pycon
>>> referencia = yd.vna.ultimo("NTN-C", vencimento="01-01-2031")
```

## `valor(titulo, data=None, vencimento=None, *, atualizar=False)`

Consulta o VNA do título na data informada.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `titulo` | `TipoTitulo` | LFT, NTN-B ou NTN-C. | *required* |
| `data` | `DateLike \| None` | Data de referência. Se nula, retorna `Decimal('NaN')`. | `None` |
| `vencimento` | `DateLike \| None` | Necessário para selecionar a série da NTN-C. Não se aplica aos demais títulos. | `None` |
| `atualizar` | `bool` | Se True, baixa novamente o arquivo de VNA e renova seu cache de 60 segundos. Não atualiza o cache dos índices do IPCA. | `False` |

Returns:

| Name | Type | Description |
| --- | --- | --- |
| `Decimal` | `Decimal` | VNA com seis casas ou `Decimal('NaN')` na ausência de dados, conforme o contrato da consulta específica do título. |

**Notes**

Fontes: Banco Central (arquivo diário SELIC) para LFT; Tesouro Nacional para NTN-B e NTN-C. Entre referências, NTN-B usa os números-índice do IPCA do IBGE, e NTN-C usa a razão entre os VNAs. Não realiza projeção automática. NTN-C sem vencimento retorna NaN.

Raises:

| Type | Description |
| --- | --- |
| `ValueError` | Se o título for inválido ou o vencimento for informado para um título diferente de NTN-C. |

Examples:

Consulta uma referência mensal publicada da NTN-B:

```pycon
>>> yd.vna.valor("NTN-B", "15-12-2025")
Decimal('4570.078408')
```

Entre referências da NTN-C, calcula o pró-rata da série selecionada:

```pycon
>>> yd.vna.valor("NTN-C", "16-12-2025", vencimento="01-01-2031")
Decimal('6449.641358')
```

Para LFT, informa a data do arquivo diário do BCB. O arquivo de 31/05/2024 contém o VNA válido no próximo dia útil, 03/06/2024:

```pycon
>>> yd.vna.valor("LFT", "31-05-2024")
Decimal('14903.011480')
```

## `vigencia(titulo, data)`

Obtém o intervalo mensal de atualização do VNA, sem acesso à rede.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `titulo` | `TipoTitulo` | NTN-B ou NTN-C; LFT não possui essa vigência mensal. | *required* |
| `data` | `DateLike` | Data contida na vigência. | *required* |

Returns:

| Type | Description |
| --- | --- |
| `tuple[date, date]` | tuple\[date, date\]: Início inclusivo e fim exclusivo, sem ajuste útil. NTN-B usa dia 15 a dia 15; NTN-C usa primeiro dia a primeiro dia. |

**Notes**

Segue os calendários de atualização do Tesouro Nacional implementados nas funções específicas de cada título.

Raises:

| Type | Description |
| --- | --- |
| `ValueError` | Se o título não for NTN-B ou NTN-C ou a data for inválida. |

Examples:

A NTN-B muda de vigência no dia 15, inclusive quando não é dia útil:

```pycon
>>> yd.vna.vigencia("NTN-B", "14-07-2026")
(datetime.date(2026, 6, 15), datetime.date(2026, 7, 15))
>>> yd.vna.vigencia("NTN-B", "15-07-2026")
(datetime.date(2026, 7, 15), datetime.date(2026, 8, 15))
```

A NTN-C muda no primeiro dia do mês:

```pycon
>>> yd.vna.vigencia("NTN-C", "15-07-2026")
(datetime.date(2026, 7, 1), datetime.date(2026, 8, 1))
```

## Atualização manual

As consultas `valor`, `historico` e `ultimo` aceitam `atualizar=True`:

```python
historico = yd.vna.historico("NTN-C", vencimento="01-01-2031", atualizar=True)
ultimo = yd.vna.ultimo("NTN-B", atualizar=True)
valor = yd.vna.valor("LFT", "31-05-2024", atualizar=True)
```

A opção ignora e renova o cache interno de 60 segundos do arquivo de VNA. Chamadas seguintes reutilizam o download renovado. Para NTN-B e NTN-C, o cache é por planilha do título, compartilhado entre datas e séries de vencimentos; para LFT, é por data de referência. Erros de download são propagados; a entrada anterior permanece disponível somente até sua expiração original.

O padrão continua sendo `atualizar=False`. A opção não invalida caches da aplicação nem o cache dos índices do IPCA usados no pró-rata da NTN-B. A coluna `data` representa a referência do VNA, não o horário do download ou uma nova publicação. Um novo download pode retornar os mesmos dados.
