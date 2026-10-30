# Títulos Públicos Federais (TPF)

Porta de entrada principal para dados de mercado de títulos públicos: taxas indicativas, vencimentos, estoque, dealers, negociações secundárias, leilões e benchmarks.

O Relatório Mensal da Dívida (RMD) usa dados do Tesouro Nacional, mas representa a dívida pública de forma mais ampla do que os títulos públicos federais. Por isso, sua entrada é `yd.rmd(aba)`, exposta na raiz, e não `yd.tpf.rmd(aba)`.

Para precificação e análise por tipo de título (cotação, duration, prêmio), consulte as páginas individuais: [LFT](<https://crdcj.github.io/PYield/lft/index.md>), [LTN](<https://crdcj.github.io/PYield/ltn/index.md>), [NTN-B](<https://crdcj.github.io/PYield/ntnb/index.md>), [NTN-F](<https://crdcj.github.io/PYield/ntnf/index.md>), etc.

## Taxas indicativas

Use `yd.tpf.taxas(...)` para consultar uma data e `yd.tpf.taxas_historicas(...)` para consultar um período ou todo o histórico disponível. As duas funções retornam o mesmo conjunto estável de colunas.

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

Nos cálculos escalares de LTN, LFT, NTN-B, NTN-C, NTN-F, NTN-B Principal e NTN-B1, taxas podem ser números decimais (`0.0575`) ou strings percentuais explícitas (`"5.75%"` ou `"5,75%"`). O símbolo `%` é obrigatório em strings. São aceitos sinal e espaços nas extremidades ou antes do símbolo, como `" -0,02% "`. Não são aceitos separadores de milhar.

```python
yd.ntnb.cotacao("31-05-2024", "15-05-2035", "6,1490%")
# Decimal('99.3651')
```

A conversão ocorre antes das regras de precisão de cada função. Taxas numéricas e retornos de taxa mantêm a convenção decimal: `5.75` continua significando 575%. Curvas e coleções de taxas continuam numéricas. Nas funções `_expr`, strings passadas como argumentos continuam identificando colunas Polars.

A tabela resume as regras adotadas pela PYield na precificação de títulos públicos federais. LTN, NTN-F, NTN-B, NTN-C e LFT seguem a metodologia da STN para títulos ofertados em leilões primários. A NTN-B Principal e a NTN-B1, vendidas exclusivamente pelo Tesouro Direto, seguem as regras próprias desse programa.

| Variáveis | LTN | NTN-F | NTN-B | NTN-B Principal | NTN-B1 | NTN-C | LFT |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Taxa de retorno | T8 / I8 | T8 / I8 | T8 / I8 | A4 | I | T8 / I8 | T8 / I8 |
| Juros semestrais | \-- | A5 | A6 | \-- | \-- | A6 | \-- |
| Fluxo de pagamentos descontados | \-- | A9 | A10 | \-- | A10 | A10 | \-- |
| Cotação (base 100) | \-- | \-- | T4 | T4 | T4 | T4 | T4 |
| Valor nominal atualizado (VNA) | \-- | \-- | T6 / I6 | I6 | I6 | T6 / I6 | T6 / I6 |
| Valor nominal atualizado (VNA, projeções) | \-- | \-- | T6 | \-- | \-- | T6 | T6 |
| Fator acumulado da taxa Selic | \-- | \-- | \-- | \-- | \-- | \-- | A16 |
| Projeções | \-- | \-- | A4 | \-- | \-- | A4 | \-- |
| Fator pro rata (projeções) | \-- | \-- | T14 | \-- | \-- | T14 | \-- |
| Variação do mês oficial | \-- | \-- | T16 | \-- | \-- | T16 | \-- |
| Exponencial de dias | T14 | T14 | T14 | T14 | T14 | T14 | T14 |
| Preço unitário (PU) | T6 / I6 | T6 / I6 | T6 | T6 | T6 | T6 | T6 |
| Valor financeiro | T2 | T2 | T2 | T2 | T2 | T2 | T2 |

Na tabela, **T** significa truncado, **A**, arredondado, e **I**, informado. Na NTN-B1, `T4` descreve a função `cotacao`. A função `cotacao_curva_zero` arredonda cada fluxo em `A10`, mas não trunca a soma final, pois ela é usada como alvo da calibração da taxa equivalente.

Os cupons, os fluxos e as cotações de NTN-B e NTN-C são calculados em base 100, com as casas decimais da tabela da STN. LFT, NTN-B Principal e NTN-B1 também retornam cotação em base 100. A NTN-F mantém os fluxos em base 1000.

Taxas de precificação e consultas de inflação usam unidade decimal: T6 na taxa percentual equivale a T8 na taxa decimal, e A2 na projeção percentual equivale a A4 na projeção decimal. Em `yd.vna.projetado`, o parâmetro `inflacao` de NTN-B e NTN-C aceita taxa decimal (`0.0045`) ou percentual explícito (`"0.45%"` ou `"0,45%"`), assim como `inflacao_projetada` de `yd.vna.contexto`. O retorno `Decimal("0.0045")` de `yd.ipca.taxa_projetada().valor_projetado` pode ser usado diretamente em `vna.projetado`. A normalização da taxa de precificação dos títulos de leilão trunca a taxa decimal em oito casas antes do desconto.

Por exemplo, a cotação `99,3651` da STN é retornada como `Decimal("99.3651")`. A relação entre cotação e preço é:

```python
pu = vna * cotacao / 100
```

Use `yd.ntnb.pu(vna, cotacao)` (ou a função do título correspondente) para aplicar também os truncamentos de VNA, cotação e PU.

As regras usadas para LTN, NTN-F, NTN-B, NTN-C e LFT estão na [metodologia da STN para os títulos ofertados em leilões primários](<https://crdcj.github.io/PYield/referencias/metodologia-calculo-tpf-stn/index.md>). Para a NTN-B1, consulte a [metodologia específica do Tesouro Direto](<https://crdcj.github.io/PYield/referencias/metodologia-calculo-ntnb1/index.md>).

### Funções canônicas e analíticas

Na precificação dos títulos, `pu` e `cotacao` reproduzem as regras oficiais de truncamento e arredondamento e retornam `Decimal`, com as casas definidas pelo Tesouro. As regras de atualização do VNA são descritas na página de [VNA](<https://crdcj.github.io/PYield/vna/index.md>).

As demais funções de títulos (`taxa`, `dv01`, `duration`, prêmios, taxas zero e rentabilidade) são medidas analíticas: descontam os fluxos com o prazo `dias úteis / 252` sem truncamentos intermediários e retornam `float`. Por isso, `taxa` aplicada a um PU gerado por `pu` recupera a taxa original apenas aproximadamente, dentro da precisão do PU e da cotação truncados.

### Exibição de taxas em percentual

As funções recebem e retornam taxas em formato decimal. Nos exemplos, a formatação percentual facilita a leitura sem alterar o valor usado nos cálculos:

```python
import pyield as yd

taxa = yd.lft.taxa("24-07-2024", "01-09-2030", 15785.324502, 15621.867466)
f"{taxa:.6%}"  # -> '0.171701%'
```

O retorno de `taxa` para LFT, LTN, NTN-B, NTN-C e NTN-F é um `float` sem arredondamento; seis casas percentuais equivalem à precisão das taxas da STN. A formatação produz uma string; continue usando a variável `taxa` nos cálculos.

Nas tabelas dos exemplos, a multiplicação das colunas de taxas por `100` também serve apenas para exibição. Prêmios apresentados em pontos-base usam o multiplicador `10_000`.

Títulos Públicos Federais.

## `benchmarks(titulo=None, incluir_historico=False)`

Busca benchmarks de títulos públicos brasileiros.

Fonte: API do Tesouro Nacional.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `titulo` | `str \| None` | Tipo do título a filtrar (ex.: `"LFT"`). Se `None`, retorna todos os títulos. | `None` |
| `incluir_historico` | `bool` | Se `True`, inclui benchmarks históricos; se `False` (padrão), retorna apenas benchmarks vigentes (on-the-run). | `False` |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | DataFrame Polars com os benchmarks. Retorna DataFrame vazio se |
| `DataFrame` | não houver dados. |

**Output Columns**

- titulo (String): tipo do título (ex.: `"LTN"`, `"LFT"`).
- data\_vencimento (Date): data de vencimento do benchmark.
- benchmark (String): nome/identificador do benchmark.
- data\_inicio (Date): data de início da vigência.
- data\_fim (Date): data de término da vigência.

**Notes**

Documentação da API: https://portal-conhecimento.tesouro.gov.br/catalogo-componentes/api-leil%C3%B5es

Examples:

```pycon
>>> df = yd.tpf.benchmarks(incluir_historico=False)
```

## `curva_pre(data)`

Constrói a curva PRE (taxas zero cupom prefixadas).

Combina taxas de LTN (já zero cupom) com taxas spot derivadas de NTN-F via bootstrap. O resultado é a curva de juros prefixada brasileira expressa em taxas zero cupom.

Fonte: ANBIMA (taxas indicativas de LTN e NTN-F).

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data` | `DateLike` | Data de referência. | *required* |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | DataFrame com a curva PRE para a data solicitada. Retorna DataFrame |
| `DataFrame` | vazio se não houver dados de LTN disponíveis. |

**Output Columns**

- data\_vencimento (Date): data de vencimento do vértice.
- dias\_uteis (Int64): dias úteis entre a data de referência e o vencimento.
- taxa\_zero (Float64): taxa zero cupom anualizada (base 252), em formato decimal.

Raises:

| Type | Description |
| --- | --- |
| `ValueError` | Se houver NTN-F sem dados de LTN para bootstrap. |

Examples:

```pycon
>>> import polars.selectors as cs
>>> curva = yd.tpf.curva_pre("18-06-2025")
>>> curva.with_columns(cs.starts_with("taxa_") * 100)
shape: (17, 3)
┌─────────────────┬────────────┬───────────┐
│ data_vencimento ┆ dias_uteis ┆ taxa_zero │
│ ---             ┆ ---        ┆ ---       │
│ date            ┆ i64        ┆ f64       │
╞═════════════════╪════════════╪═══════════╡
│ 2025-07-01      ┆ 8          ┆ 14.835    │
│ 2025-10-01      ┆ 74         ┆ 14.7463   │
│ 2026-01-01      ┆ 138        ┆ 14.7752   │
│ 2026-04-01      ┆ 199        ┆ 14.7947   │
│ 2026-07-01      ┆ 260        ┆ 14.7069   │
│ …               ┆ …          ┆ …         │
│ 2030-01-01      ┆ 1135       ┆ 13.7279   │
│ 2031-01-01      ┆ 1387       ┆ 13.815381 │
│ 2032-01-01      ┆ 1639       ┆ 13.876    │
│ 2033-01-01      ┆ 1891       ┆ 13.930017 │
│ 2035-01-01      ┆ 2390       ┆ 14.106777 │
└─────────────────┴────────────┴───────────┘
```

## `estoque(data)`

Busca dados de estoque de TPFs.

Fonte: IMA-Q da ANBIMA. Contém quantidade em mercado, valor de mercado e variação diária da quantidade dos títulos.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data` | `DateLike` | Data de referência. | *required* |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | DataFrame Polars com dados de estoque. Retorna DataFrame vazio se a |
| `DataFrame` | data for inválida ou não houver dados. |

**Output Columns**

- data\_referencia (Date): data de referência dos dados.
- titulo (String): tipo do título público.
- data\_vencimento (Date): data de vencimento do título.
- codigo\_selic (Int64): código SELIC do título.
- isin (String): código ISIN.
- pu (Float64): preço unitário do título em reais.
- quantidade\_mercado (Int64): quantidade em mercado.
- valor\_mercado (Int64): valor de mercado em reais.
- variacao\_quantidade (Int64): variação diária da quantidade.
- status\_titulo (String): status do título.

Examples:

```pycon
>>> import datetime as dt
>>> data = yd.du.deslocar(dt.date.today(), -2)
>>> df = yd.tpf.estoque(data)
>>> df.shape[0] > 0
True
```

## `leiloes_bcb(*, data=None, inicio=None, fim=None)`

Busca resultados de leilões de TPFs no BCB.

Fonte: Banco Central do Brasil, disponível desde 12/11/2012. Alternativa a `yd.tpf.leiloes` (Tesouro Nacional) para quando o Tesouro estiver indisponível ou ainda não trouxer a 2ª volta. Recebe os mesmos parâmetros e usa os mesmos nomes de colunas; colunas exclusivas de cada fonte ficam ausentes na outra. Os resultados podem ser unidos com `pl.concat(..., how="diagonal")`.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data` | `DateLike \| DatesLike \| None` | Data ou sequência de datas do leilão. Padrão é `None`. | `None` |
| `inicio` | `DateLike \| None` | Data inicial da consulta. Padrão é `None`. | `None` |
| `fim` | `DateLike \| None` | Data final da consulta. Padrão é `None`. | `None` |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | DataFrame Polars com os leilões no período, ordenado por data, título e |
| `DataFrame` | vencimento. Sem filtros temporais, retorna o histórico completo. |
| `DataFrame` | Retorna DataFrame vazio se não houver dados. |

**Output Columns**

- data\_1v (Date): data de realização do leilão.
- data\_liquidacao\_1v (Date): data de liquidação financeira da 1ª volta.
- numero\_edital (Int64): número do edital do leilão.
- tipo\_leilao (String): `"Venda"` ou `"Compra"`.
- tipo\_publico (String): público do leilão (exclusiva do BCB).
- titulo (String): LTN, LFT, NTN-B ou NTN-F.
- codigo\_selic (Int64): código do título no Selic (exclusiva do BCB).
- data\_vencimento (Date): data de vencimento do título.
- dias\_uteis (Int64): dias úteis entre liquidação e vencimento.
- dias\_corridos (Int64): dias corridos entre liquidação e vencimento.
- duration (Float64): duration de Macaulay em anos.
- prazo\_medio (Float64): maturidade média em anos.
- quantidade\_ofertada\_1v (Int64): quantidade ofertada na 1ª volta.
- quantidade\_ofertada\_2v (Int64): quantidade ofertada na 2ª volta.
- quantidade\_aceita\_1v (Int64): quantidade aceita na 1ª volta.
- quantidade\_aceita\_2v (Int64): quantidade aceita na 2ª volta.
- quantidade\_aceita\_total (Int64): quantidade aceita total.
- quantidade\_liquidada\_1v (Int64): quantidade liquidada na 1ª volta.
- quantidade\_liquidada\_2v (Int64): quantidade liquidada na 2ª volta.
- financeiro\_ofertado\_1v (Float64): quantidade ofertada na 1ª volta vezes `pu_medio`.
- financeiro\_ofertado\_2v (Float64): quantidade ofertada na 2ª volta vezes `pu_medio`.
- financeiro\_ofertado\_total (Float64): financeiro ofertado total.
- financeiro\_aceito\_1v (Float64): quantidade aceita na 1ª volta vezes `pu_medio`.
- financeiro\_aceito\_2v (Float64): quantidade aceita na 2ª volta vezes `pu_medio`.
- financeiro\_aceito\_total (Float64): financeiro aceito total.
- colocacao\_1v (Float64): taxa de colocação da 1ª volta.
- colocacao\_2v (Float64): taxa de colocação da 2ª volta.
- colocacao\_total (Float64): taxa de colocação total.
- dv01\_1v (Float64): DV01 da 1ª volta em reais.
- dv01\_2v (Float64): DV01 da 2ª volta em reais.
- dv01\_total (Float64): DV01 total em reais.
- ptax (Float64): PTAX usada na conversão para dólar.
- dv01\_1v\_usd (Float64): DV01 da 1ª volta em dólar.
- dv01\_2v\_usd (Float64): DV01 da 2ª volta em dólar.
- dv01\_total\_usd (Float64): DV01 total em dólar.
- pu\_minimo (Float64): PU de corte.
- pu\_medio (Float64): PU médio aceito.
- taxa\_media (Float64): taxa média aceita.
- taxa\_maxima (Float64): taxa de corte.

**Notes**

Diferenças em relação ao Tesouro Nacional:

- O BCB não publica `data_liquidacao_2v`, `tipo_ocorrencia`, `benchmark`, `quantidade_bcb`, `financeiro_bcb` nem `tipo_pu_medio`.
- O financeiro publicado pelo BCB é arredondado a R$ 100 mil. Por isso, os financeiros são calculados como quantidade vezes `pu_medio`, o que reproduz o Tesouro na 1ª volta; na 2ª volta, a diferença típica é inferior a 0,1%.
- Na 2ª volta, a quantidade ofertada do BCB é a nominal do edital, enquanto o Tesouro pode informar valores ligeiramente menores.
- Até 10/06/2024, o BCB publica cotação, e não PU, para LFT e NTN-B. Nesses casos, `pu_medio` é estimado pelo financeiro dividido pela quantidade aceita (sujeito ao arredondamento acima) e `pu_minimo` é nulo.

`data` não pode ser combinado com `inicio` ou `fim`. `fim` só pode ser usado junto com `inicio`.

Examples:

```pycon
>>> df = yd.tpf.leiloes_bcb(data="19-08-2025")
>>> df.select("titulo", "quantidade_aceita_total", "pu_medio", "taxa_media")
shape: (5, 4)
┌────────┬─────────────────────────┬──────────────┬────────────┐
│ titulo ┆ quantidade_aceita_total ┆ pu_medio     ┆ taxa_media │
│ ---    ┆ ---                     ┆ ---          ┆ ---        │
│ str    ┆ i64                     ┆ f64          ┆ f64        │
╞════════╪═════════════════════════╪══════════════╪════════════╡
│ LFT    ┆ 150000                  ┆ 17149.069465 ┆ 0.000636   │
│ LFT    ┆ 751003                  ┆ 17072.592044 ┆ 0.001068   │
│ NTN-B  ┆ 300759                  ┆ 4299.806542  ┆ 0.08167    │
│ NTN-B  ┆ 500542                  ┆ 4143.249763  ┆ 0.07748    │
│ NTN-B  ┆ 500000                  ┆ 4021.3613    ┆ 0.07323    │
└────────┴─────────────────────────┴──────────────┴────────────┘
```

## `premios_pre(data)`

Calcula o prêmio dos títulos prefixados (LTN e NTN-F) sobre o DI.

Em linguagem de mercado, esse valor é chamado de prêmio. Em termos descritivos, trata-se do spread sobre o DI.

**Definição do prêmio**

premio = taxa indicativa do PRE - taxa de ajuste do DI

A coluna retorna essa diferença em formato decimal (ex: 0.000439 ≈ 4.39 bps). Para exibir o prêmio em pontos-base, multiplique a coluna `premio` por 10\_000 no DataFrame retornado. No exemplo abaixo, essa coluna é sobrescrita apenas para facilitar a leitura em pontos-base.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data` | `DateLike` | Data da consulta para buscar as taxas. | *required* |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | DataFrame com as colunas do prêmio. Retorna DataFrame vazio se |
| `DataFrame` | não houver dados. |

**Output Columns**

- titulo (String): tipo do título.
- data\_vencimento (Date): data de vencimento.
- premio (Float64): prêmio em formato decimal (spread sobre o DI).

Examples:

```pycon
>>> # Exemplo em pontos-base para facilitar a leitura
>>> yd.tpf.premios_pre("30-05-2025").with_columns(
...     premio=pl.col("premio") * 10_000
... )
shape: (18, 3)
┌────────┬─────────────────┬────────┐
│ titulo ┆ data_vencimento ┆ premio │
│ ---    ┆ ---             ┆ ---    │
│ str    ┆ date            ┆ f64    │
╞════════╪═════════════════╪════════╡
│ LTN    ┆ 2025-07-01      ┆ 4.39   │
│ LTN    ┆ 2025-10-01      ┆ -9.0   │
│ LTN    ┆ 2026-01-01      ┆ -4.88  │
│ LTN    ┆ 2026-04-01      ┆ -4.45  │
│ LTN    ┆ 2026-07-01      ┆ 0.81   │
│ …      ┆ …               ┆ …      │
│ NTN-F  ┆ 2027-01-01      ┆ -3.31  │
│ NTN-F  ┆ 2029-01-01      ┆ 14.21  │
│ NTN-F  ┆ 2031-01-01      ┆ 21.61  │
│ NTN-F  ┆ 2033-01-01      ┆ 11.51  │
│ NTN-F  ┆ 2035-01-01      ┆ 22.0   │
└────────┴─────────────────┴────────┘
```

## `taxas(data, titulo=None)`

Busca taxas e preços indicativos de TPFs.

Fonte: ANBIMA. Primeiro consulta o cache local de dados históricos; se a data não estiver no cache, busca diretamente na fonte da ANBIMA.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data` | `DateLike` | Data de referência. | *required* |
| `titulo` | `TipoTPF \| None` | Tipo do título público federal. Aceita `LFT`, `NTN-B`, `NTN-C`, `LTN`, `NTN-F` ou `PRE`. | `None` |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | DataFrame Polars com taxas e preços indicativos. Retorna DataFrame |
| `DataFrame` | vazio, com as colunas abaixo, se não houver dados para a data. |

**Output Columns**

- titulo (String): tipo do título público.
- data\_referencia (Date): data de referência dos dados.
- codigo\_selic (Int64): código do título no SELIC.
- data\_base (Date): data base ou de emissão do título.
- data\_vencimento (Date): data de vencimento do título.
- pu (Float64): preço unitário para liquidação em D0.
- taxa\_compra (Float64): taxa de compra em D0.
- taxa\_venda (Float64): taxa de venda em D0.
- taxa\_indicativa (Float64): taxa indicativa em D0.

Examples:

```pycon
>>> df = yd.tpf.taxas(data="06-02-2026")
```

## `taxas_historicas(inicio=None, fim=None, titulo=None)`

Consulta o histórico de taxas e preços indicativos de TPFs.

Fonte: ANBIMA, em painel histórico publicado pela PYield. Os filtros de período são inclusivos. Sem argumentos, retorna todo o histórico disponível.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `inicio` | `DateLike \| None` | Data inicial do período. Se omitida, não limita o início. | `None` |
| `fim` | `DateLike \| None` | Data final do período. Se omitida, não limita o fim. | `None` |
| `titulo` | `TipoTPF \| None` | Tipo do título público federal. Aceita `LFT`, `NTN-B`, `NTN-C`, `LTN`, `NTN-F` ou `PRE`. | `None` |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | DataFrame Polars com o histórico de taxas e preços indicativos. Retorna |
| `DataFrame` | DataFrame vazio se não houver dados para os filtros informados. |

**Output Columns**

- titulo (String): tipo do título público.
- data\_referencia (Date): data de referência dos dados.
- codigo\_selic (Int64): código do título no SELIC.
- data\_base (Date): data base ou de emissão do título.
- data\_vencimento (Date): data de vencimento do título.
- pu (Float64): preço unitário para liquidação em D0.
- taxa\_compra (Float64): taxa de compra em D0.
- taxa\_venda (Float64): taxa de venda em D0.
- taxa\_indicativa (Float64): taxa indicativa em D0.

Raises:

| Type | Description |
| --- | --- |
| `ValueError` | Se `inicio` for posterior a `fim`. |

Examples:

```pycon
>>> df = yd.tpf.taxas_historicas(
...     inicio="01-01-2025", fim="31-01-2025", titulo="PRE"
... )
```

## `vencimentos(data, titulo)`

Busca vencimentos de TPFs disponíveis nas taxas indicativas.

Fonte: ANBIMA, mesma base usada por `yd.tpf.taxas`.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data` | `DateLike` | Data de referência. | *required* |
| `titulo` | `TipoTPF` | Tipo do título público federal. Aceita `LFT`, `NTN-B`, `NTN-C`, `LTN`, `NTN-F` ou `PRE`. | *required* |

Returns:

| Type | Description |
| --- | --- |
| `Series` | Series ordenada com os vencimentos disponíveis. |

Examples:

```pycon
>>> yd.tpf.vencimentos(data="22-08-2025", titulo="PRE")
shape: (18,)
Series: 'data_vencimento' [date]
[
    2025-10-01
    2026-01-01
    2026-04-01
    2026-07-01
    2026-10-01
    …
    2030-01-01
    2031-01-01
    2032-01-01
    2033-01-01
    2035-01-01
]
```

## Acesso técnico à fonte ANBIMA

O módulo `pyield.anbima.taxas` permite baixar ou ler o arquivo da ANBIMA com todas as colunas processadas da fonte. Essa camada é indicada para integração com a fonte; para análises de TPF, prefira a visão estável de `yd.tpf`.

Taxas de Títulos Públicos Federais (TPF) da ANBIMA.

**Fonte**

https://www.anbima.com.br/pt\_br/informar/taxas-de-titulos-publicos.htm

**Exemplo de URL**

https://www.anbima.com.br/informacoes/merc-sec/arqs/ms240614.txt

Exemplo de dado bruto (CSV separado por @, encoding latin1): ANBIMA - Associação Brasileira das Entidades dos Mercados Financeiro e de Capitais

```text
Titulo@Data Referencia@Codigo SELIC@Data Base/Emissao@Data Vencimento@Tx. Compra@Tx. Venda@Tx. Indicativas@PU@Desvio padrao@Interv. Ind. Inf. (D0)@Interv. Ind. Sup. (D0)@Interv. Ind. Inf. (D+1)@Interv. Ind. Sup. (D+1)@Criterio
LTN@20250924@100000@20230707@20251001@14,9483@14,9263@14,9375@997,241543@0,00433039162894@14,7341@15,2612@14,7316@15,2689@Calculado
LTN@20250924@100000@20200206@20260101@14,7741@14,7485@14,7616@963,001853@0,00729826731971@14,7008@14,9986@14,7021@14,9975@Calculado
```

## `baixar_arquivo(data)`

Baixa o arquivo bruto de taxas de TPF publicado pela ANBIMA.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data` | `DateLike` | Data de referência do arquivo. | *required* |

Returns:

| Type | Description |
| --- | --- |
| `bytes` | Bytes do arquivo publicado pela ANBIMA, sem decodificação ou parsing. |

Raises:

| Type | Description |
| --- | --- |
| `HTTPError` | Se o arquivo não estiver disponível ou a resposta HTTP indicar erro. |
| `ValueError` | Se `data` não for uma data escalar válida. |

## `buscar(data)`

Busca e processa taxas de TPF diretamente na ANBIMA.

Fonte: arquivo de taxas de títulos públicos da ANBIMA.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data` | `DateLike` | Data de referência do arquivo. | *required* |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | DataFrame com todas as colunas processadas da fonte. Retorna DataFrame |
| `DataFrame` | vazio para datas válidas sem dados disponíveis. |

**Output Columns**

- titulo (String): tipo do título público.
- data\_referencia (Date): data de referência dos dados.
- codigo\_selic (Int64): código do título no SELIC.
- data\_base (Date): data base ou de emissão do título.
- data\_vencimento (Date): data de vencimento do título.
- taxa\_compra (Float64): taxa de compra em D0.
- taxa\_venda (Float64): taxa de venda em D0.
- taxa\_indicativa (Float64): taxa indicativa em D0.
- pu (Float64): preço unitário para liquidação em D0.
- desvio\_padrao (Float64): desvio padrão das taxas observadas.
- taxa\_intervalo\_inf\_d0 (Float64): limite inferior indicativo em D0.
- taxa\_intervalo\_sup\_d0 (Float64): limite superior indicativo em D0.
- taxa\_intervalo\_inf\_d1 (Float64): limite inferior indicativo em D+1.
- taxa\_intervalo\_sup\_d1 (Float64): limite superior indicativo em D+1.
- criterio (String): critério usado pela ANBIMA para o título.

Raises:

| Type | Description |
| --- | --- |
| `ValueError` | Se `data` não for uma data escalar válida. |

## `ler(fonte)`

Lê taxas de TPF da ANBIMA a partir de bytes ou arquivo local.

Fonte: arquivo de taxas de títulos públicos da ANBIMA.

Arquivos atuais `.txt` são lidos diretamente. Arquivos históricos `.exe` são tratados como ZIPs e o arquivo interno é lido.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `fonte` | `bytes \| _CaminhoArquivo` | Bytes do arquivo bruto ou caminho do arquivo salvo localmente. | *required* |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | DataFrame Polars com todas as colunas processadas da fonte. |

**Output Columns**

- titulo (String): tipo do título público.
- data\_referencia (Date): data de referência dos dados.
- codigo\_selic (Int64): código do título no SELIC.
- data\_base (Date): data base ou de emissão do título.
- data\_vencimento (Date): data de vencimento do título.
- taxa\_compra (Float64): taxa de compra em D0.
- taxa\_venda (Float64): taxa de venda em D0.
- taxa\_indicativa (Float64): taxa indicativa em D0.
- pu (Float64): preço unitário para liquidação em D0.
- desvio\_padrao (Float64): desvio padrão das taxas observadas.
- taxa\_intervalo\_inf\_d0 (Float64): limite inferior indicativo em D0.
- taxa\_intervalo\_sup\_d0 (Float64): limite superior indicativo em D0.
- taxa\_intervalo\_inf\_d1 (Float64): limite inferior indicativo em D+1.
- taxa\_intervalo\_sup\_d1 (Float64): limite superior indicativo em D+1.
- criterio (String): critério usado pela ANBIMA para o título.

## secundario

Negociações do mercado secundário de TPFs no sistema Selic do BCB.

## `baixar_zip(data, extragrupo=False)`

Baixa o ZIP bruto mensal de negociações secundárias de TPFs.

Fonte: Banco Central do Brasil, sistema SELIC. A fonte publica um arquivo por mês; por isso, apenas o ano e o mês de `data` são usados.

A função valida a estrutura mínima do ZIP antes de retornar os bytes, para evitar que pipelines de ingestão salvem arquivo vazio, corrompido ou sem CSV plausível. Use :func:`ler` para processar os bytes ou o arquivo salvo.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data` | `DateLike` | Data de referência. Apenas ano e mês definem o arquivo. | *required* |
| `extragrupo` | `bool` | Se verdadeiro, baixa o arquivo extragrupo. | `False` |

Returns:

| Type | Description |
| --- | --- |
| `bytes` | Bytes validados do arquivo ZIP mensal publicado pelo BCB. |

Raises:

| Type | Description |
| --- | --- |
| `HTTPError` | Se o arquivo não estiver disponível no BCB ou a resposta HTTP indicar erro. |
| `ValueError` | Se o conteúdo baixado não for um ZIP bruto plausível. |

Examples:

```pycon
>>> conteudo = yd.tpf.secundario.baixar_zip("07-01-2025")
```

## `intradia()`

Busca dados intradia do mercado secundário de TPFs.

Fonte: Banco Central do Brasil, sistema SELIC. Os dados ficam disponíveis apenas durante o horário do SELIC (09:00-22:00 BRT) em dias úteis.

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | DataFrame Polars com negociações intradia do mercado secundário. |
| `DataFrame` | Retorna DataFrame vazio fora do horário do SELIC. |

**Output Columns**

- data\_hora\_consulta (Datetime): data e hora da consulta.
- data\_liquidacao (Date): data de liquidação à vista.
- titulo (String): sigla do título público.
- codigo\_selic (Int64): código SELIC do título.
- data\_vencimento (Date): data de vencimento do título.
- pu\_minimo (Float64): menor preço negociado.
- pu\_medio (Float64): preço médio negociado.
- pu\_maximo (Float64): maior preço negociado.
- pu\_ultimo (Float64): último preço negociado.
- taxa\_minima (Float64): menor taxa negociada.
- taxa\_media (Float64): taxa média negociada.
- taxa\_maxima (Float64): maior taxa negociada.
- taxa\_ultima (Float64): última taxa negociada.
- operacoes (Int64): total de operações liquidadas.
- quantidade (Int64): quantidade total de títulos negociados.
- financeiro (Float64): valor financeiro total negociado.
- operacoes\_corretagem (Int64): operações via corretagem.
- quantidade\_corretagem (Int64): títulos via corretagem.
- termo\_pu\_minimo (Float64): menor preço a termo negociado.
- termo\_pu\_medio (Float64): preço médio a termo negociado.
- termo\_pu\_ultimo (Float64): último preço a termo negociado.
- termo\_pu\_maximo (Float64): maior preço a termo negociado.
- termo\_taxa\_ultima (Float64): última taxa a termo negociada.
- termo\_taxa\_minima (Float64): menor taxa a termo negociada.
- termo\_taxa\_media (Float64): taxa média a termo negociada.
- termo\_taxa\_maxima (Float64): maior taxa a termo negociada.
- termo\_operacoes (Int64): total de operações a termo.
- termo\_quantidade (Int64): total de títulos a termo negociados.
- termo\_financeiro (Float64): valor financeiro total a termo.
- termo\_operacoes\_corretagem (Int64): operações a termo via corretagem.
- termo\_quantidade\_corretagem (Int64): títulos a termo via corretagem.

Examples:

```pycon
>>> df = yd.tpf.secundario.intradia()
```

## `ler(fonte)`

Lê o ZIP mensal bruto do secundário de TPFs.

Fonte: Banco Central do Brasil, sistema SELIC. Mesma saída de :func:`mensal`, mas recebe o ZIP bruto em vez de baixá-lo. Útil para processar arquivos salvos com :func:`baixar_zip`.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `fonte` | `bytes \| CaminhoArquivo` | ZIP bruto em bytes ou caminho para o arquivo ZIP no disco. | *required* |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | DataFrame Polars com as colunas documentadas em :func:`mensal`. |

Raises:

| Type | Description |
| --- | --- |
| `ValueError` | Se o conteúdo não for um ZIP válido. |

Examples:

```pycon
>>> conteudo = yd.tpf.secundario.baixar_zip("07-01-2025")
>>> df = yd.tpf.secundario.ler(conteudo)
>>> df = yd.tpf.secundario.ler("NegT202501.ZIP")
```

## `mensal(data, extragrupo=False)`

Busca dados mensais do mercado secundário de TPFs.

Fonte: Banco Central do Brasil, sistema SELIC. Baixa e valida o ZIP mensal de negociações secundárias e retorna os dados processados. Apenas o ano e o mês de `data` são usados para identificar o arquivo.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data` | `DateLike` | Data de referência. Apenas ano e mês definem o arquivo. | *required* |
| `extragrupo` | `bool` | Se verdadeiro, busca apenas negociações extragrupo. | `False` |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | DataFrame Polars com dados mensais do mercado secundário. |

**Output Columns**

- data\_liquidacao (Date): data de liquidação da negociação.
- titulo (String): sigla do título público.
- codigo\_selic (Int64): código único no sistema SELIC.
- isin (String): código ISIN.
- data\_emissao (Date): data de emissão do título.
- data\_vencimento (Date): data de vencimento do título.
- operacoes (Int64): número total de operações.
- quantidade (Int64): quantidade total negociada.
- pu\_minimo (Float64): preço unitário mínimo.
- pu\_medio (Float64): preço unitário médio.
- pu\_maximo (Float64): preço unitário máximo.
- pu\_lastro (Float64): preço unitário de lastro.
- valor\_par (Float64): valor par do título.
- taxa\_minima (Float64): taxa mínima.
- taxa\_media (Float64): taxa média.
- taxa\_maxima (Float64): taxa máxima.
- operacoes\_corretagem (Int64): operações com corretagem.
- quantidade\_corretagem (Int64): quantidade com corretagem.
- financeiro (Float64): valor financeiro negociado (`quantidade * pu_medio`).

**Notes**

O schema é estável para concatenação entre meses. Em layouts antigos da fonte que não trazem corretagem, `operacoes_corretagem` e `quantidade_corretagem` são retornadas como nulas.

Examples:

```pycon
>>> df = yd.tpf.secundario.mensal("07-01-2025", extragrupo=True)
```

## leiloes

Busca resultados de leilões de TPFs.

Fonte: Tesouro Nacional. Dados disponíveis a partir de 04/01/2018. Retorna dados de quantidades, financeiros, taxas de colocação, duration e DV01 dos leilões no período informado.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data` | `DateLike \| DatesLike \| None` | Data ou sequência de datas do leilão. Padrão é `None`. | `None` |
| `inicio` | `DateLike \| None` | Data inicial da consulta. Padrão é `None`. | `None` |
| `fim` | `DateLike \| None` | Data final da consulta. Padrão é `None`. | `None` |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | DataFrame Polars com os dados processados dos leilões. Se `data` for |
| `DataFrame` | uma sequência, concatena os resultados das datas informadas. Se |
| `DataFrame` | `inicio` for informado, retorna os leilões a partir dessa data. Se |
| `DataFrame` | nenhum filtro temporal for informado, retorna o histórico completo. |
| `DataFrame` | Retorna DataFrame vazio se não houver dados para o período. |

**Output Columns**

- data\_1v (Date): data de realização do leilão.
- data\_liquidacao\_1v (Date): data de liquidação financeira da 1ª volta.
- data\_liquidacao\_2v (Date): data de liquidação financeira da 2ª volta.
- numero\_edital (Int64): número do edital do leilão.
- tipo\_leilao (String): tipo da operação.
- tipo\_ocorrencia (String): classificação da ocorrência do leilão.
- titulo (String): código do título público leiloado.
- benchmark (String): descrição de referência do título.
- data\_vencimento (Date): data de vencimento do título.
- dias\_uteis (Int64): dias úteis entre liquidação e vencimento.
- dias\_corridos (Int64): dias corridos entre liquidação e vencimento.
- duration (Float64): duration de Macaulay em anos.
- prazo\_medio (Float64): maturidade média em anos.
- quantidade\_ofertada\_1v (Int64): quantidade ofertada na 1ª volta.
- quantidade\_ofertada\_2v (Int64): quantidade ofertada na 2ª volta.
- quantidade\_aceita\_1v (Int64): quantidade aceita na 1ª volta.
- quantidade\_aceita\_2v (Int64): quantidade aceita na 2ª volta.
- quantidade\_aceita\_total (Int64): quantidade aceita total.
- quantidade\_liquidada\_1v (Int64): quantidade liquidada na 1ª volta.
- quantidade\_liquidada\_2v (Int64): quantidade liquidada na 2ª volta.
- financeiro\_ofertado\_1v (Float64): financeiro ofertado na 1ª volta.
- financeiro\_ofertado\_2v (Float64): financeiro ofertado na 2ª volta.
- financeiro\_ofertado\_total (Float64): financeiro ofertado total.
- financeiro\_aceito\_1v (Float64): financeiro aceito na 1ª volta.
- financeiro\_aceito\_2v (Float64): financeiro aceito na 2ª volta.
- financeiro\_aceito\_total (Float64): financeiro aceito total.
- quantidade\_bcb (Int64): quantidade adquirida pelo Banco Central.
- financeiro\_bcb (Int64): financeiro adquirido pelo Banco Central.
- colocacao\_1v (Float64): taxa de colocação da 1ª volta.
- colocacao\_2v (Float64): taxa de colocação da 2ª volta.
- colocacao\_total (Float64): taxa de colocação total.
- dv01\_1v (Float64): DV01 da 1ª volta em reais.
- dv01\_2v (Float64): DV01 da 2ª volta em reais.
- dv01\_total (Float64): DV01 total em reais.
- ptax (Float64): PTAX usada na conversão para dólar.
- dv01\_1v\_usd (Float64): DV01 da 1ª volta em dólar.
- dv01\_2v\_usd (Float64): DV01 da 2ª volta em dólar.
- dv01\_total\_usd (Float64): DV01 total em dólar.
- pu\_minimo (Float64): preço unitário mínimo aceito.
- pu\_medio (Float64): preço unitário médio ponderado aceito.
- tipo\_pu\_medio (String): origem do PU médio.
- taxa\_media (Float64): taxa média aceita.
- taxa\_maxima (Float64): taxa máxima aceita.

**Notes**

`data` não pode ser combinado com `inicio` ou `fim`. `fim` só pode ser usado junto com `inicio`. Quando o Tesouro estiver indisponível ou incompleto (por exemplo, sem a 2ª volta), use `yd.tpf.leiloes_bcb`, que usa os mesmos nomes de colunas.

Busca resultados de leilões de TPFs no BCB.

Fonte: Banco Central do Brasil, disponível desde 12/11/2012. Alternativa a `yd.tpf.leiloes` (Tesouro Nacional) para quando o Tesouro estiver indisponível ou ainda não trouxer a 2ª volta. Recebe os mesmos parâmetros e usa os mesmos nomes de colunas; colunas exclusivas de cada fonte ficam ausentes na outra. Os resultados podem ser unidos com `pl.concat(..., how="diagonal")`.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data` | `DateLike \| DatesLike \| None` | Data ou sequência de datas do leilão. Padrão é `None`. | `None` |
| `inicio` | `DateLike \| None` | Data inicial da consulta. Padrão é `None`. | `None` |
| `fim` | `DateLike \| None` | Data final da consulta. Padrão é `None`. | `None` |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | DataFrame Polars com os leilões no período, ordenado por data, título e |
| `DataFrame` | vencimento. Sem filtros temporais, retorna o histórico completo. |
| `DataFrame` | Retorna DataFrame vazio se não houver dados. |

**Output Columns**

- data\_1v (Date): data de realização do leilão.
- data\_liquidacao\_1v (Date): data de liquidação financeira da 1ª volta.
- numero\_edital (Int64): número do edital do leilão.
- tipo\_leilao (String): `"Venda"` ou `"Compra"`.
- tipo\_publico (String): público do leilão (exclusiva do BCB).
- titulo (String): LTN, LFT, NTN-B ou NTN-F.
- codigo\_selic (Int64): código do título no Selic (exclusiva do BCB).
- data\_vencimento (Date): data de vencimento do título.
- dias\_uteis (Int64): dias úteis entre liquidação e vencimento.
- dias\_corridos (Int64): dias corridos entre liquidação e vencimento.
- duration (Float64): duration de Macaulay em anos.
- prazo\_medio (Float64): maturidade média em anos.
- quantidade\_ofertada\_1v (Int64): quantidade ofertada na 1ª volta.
- quantidade\_ofertada\_2v (Int64): quantidade ofertada na 2ª volta.
- quantidade\_aceita\_1v (Int64): quantidade aceita na 1ª volta.
- quantidade\_aceita\_2v (Int64): quantidade aceita na 2ª volta.
- quantidade\_aceita\_total (Int64): quantidade aceita total.
- quantidade\_liquidada\_1v (Int64): quantidade liquidada na 1ª volta.
- quantidade\_liquidada\_2v (Int64): quantidade liquidada na 2ª volta.
- financeiro\_ofertado\_1v (Float64): quantidade ofertada na 1ª volta vezes `pu_medio`.
- financeiro\_ofertado\_2v (Float64): quantidade ofertada na 2ª volta vezes `pu_medio`.
- financeiro\_ofertado\_total (Float64): financeiro ofertado total.
- financeiro\_aceito\_1v (Float64): quantidade aceita na 1ª volta vezes `pu_medio`.
- financeiro\_aceito\_2v (Float64): quantidade aceita na 2ª volta vezes `pu_medio`.
- financeiro\_aceito\_total (Float64): financeiro aceito total.
- colocacao\_1v (Float64): taxa de colocação da 1ª volta.
- colocacao\_2v (Float64): taxa de colocação da 2ª volta.
- colocacao\_total (Float64): taxa de colocação total.
- dv01\_1v (Float64): DV01 da 1ª volta em reais.
- dv01\_2v (Float64): DV01 da 2ª volta em reais.
- dv01\_total (Float64): DV01 total em reais.
- ptax (Float64): PTAX usada na conversão para dólar.
- dv01\_1v\_usd (Float64): DV01 da 1ª volta em dólar.
- dv01\_2v\_usd (Float64): DV01 da 2ª volta em dólar.
- dv01\_total\_usd (Float64): DV01 total em dólar.
- pu\_minimo (Float64): PU de corte.
- pu\_medio (Float64): PU médio aceito.
- taxa\_media (Float64): taxa média aceita.
- taxa\_maxima (Float64): taxa de corte.

**Notes**

Diferenças em relação ao Tesouro Nacional:

- O BCB não publica `data_liquidacao_2v`, `tipo_ocorrencia`, `benchmark`, `quantidade_bcb`, `financeiro_bcb` nem `tipo_pu_medio`.
- O financeiro publicado pelo BCB é arredondado a R$ 100 mil. Por isso, os financeiros são calculados como quantidade vezes `pu_medio`, o que reproduz o Tesouro na 1ª volta; na 2ª volta, a diferença típica é inferior a 0,1%.
- Na 2ª volta, a quantidade ofertada do BCB é a nominal do edital, enquanto o Tesouro pode informar valores ligeiramente menores.
- Até 10/06/2024, o BCB publica cotação, e não PU, para LFT e NTN-B. Nesses casos, `pu_medio` é estimado pelo financeiro dividido pela quantidade aceita (sujeito ao arredondamento acima) e `pu_minimo` é nulo.

`data` não pode ser combinado com `inicio` ou `fim`. `fim` só pode ser usado junto com `inicio`.

Examples:

```pycon
>>> df = yd.tpf.leiloes_bcb(data="19-08-2025")
>>> df.select("titulo", "quantidade_aceita_total", "pu_medio", "taxa_media")
shape: (5, 4)
┌────────┬─────────────────────────┬──────────────┬────────────┐
│ titulo ┆ quantidade_aceita_total ┆ pu_medio     ┆ taxa_media │
│ ---    ┆ ---                     ┆ ---          ┆ ---        │
│ str    ┆ i64                     ┆ f64          ┆ f64        │
╞════════╪═════════════════════════╪══════════════╪════════════╡
│ LFT    ┆ 150000                  ┆ 17149.069465 ┆ 0.000636   │
│ LFT    ┆ 751003                  ┆ 17072.592044 ┆ 0.001068   │
│ NTN-B  ┆ 300759                  ┆ 4299.806542  ┆ 0.08167    │
│ NTN-B  ┆ 500542                  ┆ 4143.249763  ┆ 0.07748    │
│ NTN-B  ┆ 500000                  ┆ 4021.3613    ┆ 0.07323    │
└────────┴─────────────────────────┴──────────────┴────────────┘
```

## dealers

Busca os dealers do Tesouro Nacional vigentes em uma data.

Dealers são instituições financeiras credenciadas pelo Tesouro Nacional para atuar nas emissões primárias e no mercado secundário de títulos públicos federais. A composição é revista periodicamente.

Fonte: API de Leilões da Dívida Pública do Tesouro Nacional.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data` | `DateLike \| None` | Data de referência do credenciamento. Se `None`, usa a data atual no Brasil. | `None` |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | DataFrame Polars com as instituições cujo período de credenciamento |
| `DataFrame` | contém a data informada. Retorna DataFrame vazio, com esquema estável, |
| `DataFrame` | se a fonte não possuir registros para a data. |

**Output Columns**

- inicio\_periodo (Date): início do período de credenciamento.
- fim\_periodo (Date): fim do período de credenciamento.
- cnpj (String): CNPJ da instituição credenciada.
- instituicao (String): nome da instituição credenciada como dealer.

**Notes**

A função preserva os nomes e períodos publicados pela fonte. O endpoint não informa o tipo da instituição, o conglomerado financeiro nem os objetos de negociação escolhidos pelo dealer.

Examples:

```pycon
>>> df = yd.tpf.dealers("15-03-2026")
>>> df.is_empty() or df["inicio_periodo"].unique().to_list()
[datetime.date(2026, 2, 10)]
```

## Relatório Mensal da Dívida (RMD)

Relatório Mensal da Dívida (RMD) do Tesouro Nacional.

## `rmd(aba)`

Retorna dados do Relatório Mensal da Dívida (RMD) do Tesouro Nacional.

Baixa e processa a planilha do RMD, extraindo dados da aba solicitada. A publicação mais recente é descoberta automaticamente via parse HTML da página oficial do Tesouro Transparente.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `aba` | `str` | Número da aba a processar. Abas implementadas: `"1.3"` e `"2.1"`. | *required* |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | DataFrame Polars no schema específico da aba solicitada. |

**Output Columns**

Aba `"1.3"`: \* periodo (Date): primeiro dia do mês de referência. \* grupo (String): seção principal — `"Emissões"` ou `"Resgates"`. \* subgrupo (String): categoria dentro do grupo. \* titulo (String): tipo de título ou `null` para subgrupos sem detalhamento por título. \* valor (Float64): valor em R$. Aba `"2.1"`: \* periodo (Date): primeiro dia do mês de referência. \* detentor (String): quem detém o estoque — `"Público"` ou `"Banco Central"`. \* tipo (String): classificação da dívida — `"DPMFi"` (interna) ou `"DPFe"` (externa). \* categoria (String): subdivisão dentro do tipo, quando houver — `"Tesouro Nacional"`, `"Banco Central"` (emitente dentro da DPMFi pública), `"Mobiliária"`, `"Contratual"`; `null` quando não há subdivisão (ex.: DPMFi em poder do Banco Central). \* titulo (String): título ou instrumento de dívida. \* valor (Float64): valor em R$. Somente registros folha; subtotais devem ser calculados pelo usuário via agregação.

Raises:

| Type | Description |
| --- | --- |
| `ValueError` | Se `aba` não estiver entre as abas implementadas. |

**Notes**

- A publicação mais recente é descoberta automaticamente via parse HTML do Tesouro Transparente.
- A aba `"1.3"` traz emissões e resgates da DPMFi.
- A aba `"2.1"` traz a série histórica de estoque da DPF.

Erros de download e processamento são propagados ao chamador; não são convertidos em DataFrame vazio.

Examples:

```pycon
>>> df = yd.rmd(aba="1.3")
>>> df = yd.rmd(aba="2.1")
```
