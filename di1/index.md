Curva de DI Futuro (DI1).

**Fonte**

Dados históricos de contratos futuros da B3, processados a partir do Price Report e usados como curva de ajuste do DI1.

## `dados(datas, inicio_mes=False, filtrar_pre=False)`

Obtém dados de contratos de futuros de DI para datas de negociação específicas.

Fornece acesso aos dados de futuros de DI, permitindo ajustes nas datas de vencimento (para início do mês) e filtragem opcional com base nos vencimentos de títulos públicos prefixados (LTN e NTN-F).

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `datas` | `DateLike \| DatesLike` | Datas de negociação para as quais obter dados de contratos DI. | *required* |
| `inicio_mes` | `bool` | Se True, ajusta todas as datas de vencimento para o primeiro dia de seus respectivos meses (ex: 2025-02-03 vira 2025-02-01). Padrão: False. | `False` |
| `filtrar_pre` | `bool` | Se True, filtra contratos DI para incluir apenas aqueles cujas datas de vencimento coincidem com vencimentos conhecidos de títulos públicos prefixados (LTN, NTN-F) do dataset TPF mais próximo da data de negociação fornecida. Padrão: False. | `False` |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | DataFrame contendo dados de contratos de futuros de DI para as datas |
| `DataFrame` | especificadas, ordenados por datas de negociação e vencimento. Retorna |
| `DataFrame` | DataFrame vazio se nenhum dado for encontrado. |

Examples:

```pycon
>>> from pyield import di1
>>> import polars.selectors as cs
>>> df = di1.dados(datas="16-10-2024", inicio_mes=True)
>>> df.head(3).select(
...     "codigo_negociacao", "data_vencimento", "dias_uteis", "taxa_ajuste"
... ).with_columns(cs.starts_with("taxa_") * 100)
shape: (3, 4)
┌───────────────────┬─────────────────┬────────────┬─────────────┐
│ codigo_negociacao ┆ data_vencimento ┆ dias_uteis ┆ taxa_ajuste │
│ ---               ┆ ---             ┆ ---        ┆ ---         │
│ str               ┆ date            ┆ i64        ┆ f64         │
╞═══════════════════╪═════════════════╪════════════╪═════════════╡
│ DI1X24            ┆ 2024-11-01      ┆ 12         ┆ 10.653      │
│ DI1Z24            ┆ 2024-12-01      ┆ 31         ┆ 10.91       │
│ DI1F25            ┆ 2025-01-01      ┆ 52         ┆ 11.164      │
└───────────────────┴─────────────────┴────────────┴─────────────┘
```

## `datas_disponiveis()`

Retorna as datas de negociação disponíveis para DI1.

Obtém valores distintos de 'data\_referencia' para DI1, com base no dataset histórico PR da B3 (mesmo utilizado por `futuro`). Inclui apenas datas com preço e taxa de ajuste já definidos; o pregão do dia corrente não está incluído.

Returns:

| Type | Description |
| --- | --- |
| `Series` | Series ordenada de datas de negociação (dt.date) para as quais dados de |
| `Series` | ajuste de DI estão disponíveis. |

Examples:

```pycon
>>> from pyield import di1
>>> # Série disponível no dataset PR começa em 2018-01-02
>>> di1.datas_disponiveis().head(5)
shape: (5,)
Series: 'data_referencia' [date]
[
    2018-01-02
    2018-01-03
    2018-01-04
    2018-01-05
    2018-01-08
]
```

## `interpolar_taxa(data_referencia, data_vencimento, extrapolar=False, tipo_taxa='ajuste')`

Interpola ou obtém a taxa DI para uma única data de vencimento.

Busca dados de contratos DI para a data de negociação especificada e determina a taxa de liquidação para o vencimento fornecido. Se existir uma correspondência exata para a data de vencimento, sua taxa é retornada. Caso contrário, a taxa é interpolada usando o método flat-forward baseado nas taxas dos contratos adjacentes.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data_referencia` | `DateLike` | Data de negociação para a qual obter dados de DI. | *required* |
| `data_vencimento` | `DateLike` | Data de vencimento alvo para a taxa. | *required* |
| `extrapolar` | `bool` | Se True, permite extrapolação se o `data_vencimento` estiver fora do intervalo de vencimentos de contratos disponíveis para a `data_referencia`. Padrão: False. | `False` |
| `tipo_taxa` | `TipoTaxaDI1` | Tipo de taxa usada como curva de referência. Use `"ajuste"` para a taxa de ajuste ou `"fechamento"` para a última taxa negociada. Padrão: `"ajuste"`. | `'ajuste'` |

Returns:

<table>       <thead>         <tr>           <th>Type</th>           <th>Description</th>         </tr>       </thead>       <tbody>           <tr class="doc-section-item">             <td>                   <code><autoref backlink-anchor="pyield.futuro.di1.interpolar_taxa" backlink-type="returned-by" identifier="float" optional="">float</autoref></code>             </td>             <td>               <div class="doc-md-description">                 <p>Taxa de liquidação DI exata ou interpolada para a data e vencimento</p>               </div>             </td>           </tr>           <tr class="doc-section-item">             <td>                   <code><autoref backlink-anchor="pyield.futuro.di1.interpolar_taxa" backlink-type="returned-by" identifier="float" optional="">float</autoref></code>             </td>             <td>               <div class="doc-md-description">                 <p>especificados. Retorna <code>float(&quot;nan&quot;)</code> se:</p>               </div>             </td>           </tr>           <tr class="doc-section-item">             <td>                   <code><autoref backlink-anchor="pyield.futuro.di1.interpolar_taxa" backlink-type="returned-by" identifier="float" optional="">float</autoref></code>             </td>             <td>               <div class="doc-md-description">                 <ul> <li><code>data_referencia</code> ou <code>data_vencimento</code> for nulo.</li> </ul>               </div>             </td>           </tr>           <tr class="doc-section-item">             <td>                   <code><autoref backlink-anchor="pyield.futuro.di1.interpolar_taxa" backlink-type="returned-by" identifier="float" optional="">float</autoref></code>             </td>             <td>               <div class="doc-md-description">                 <ul> <li>Não há dados DI para a <code>data_referencia</code>.</li> </ul>               </div>             </td>           </tr>           <tr class="doc-section-item">             <td>                   <code><autoref backlink-anchor="pyield.futuro.di1.interpolar_taxa" backlink-type="returned-by" identifier="float" optional="">float</autoref></code>             </td>             <td>               <div class="doc-md-description">                 <ul> <li>O <code>data_vencimento</code> está fora do intervalo e <code>extrapolar</code> é False.</li> </ul>               </div>             </td>           </tr>           <tr class="doc-section-item">             <td>                   <code><autoref backlink-anchor="pyield.futuro.di1.interpolar_taxa" backlink-type="returned-by" identifier="float" optional="">float</autoref></code>             </td>             <td>               <div class="doc-md-description">                 <ul> <li>O cálculo de interpolação falhou.</li> </ul>               </div>             </td>           </tr>       </tbody>     </table>

Examples:

```pycon
>>> from pyield import di1
>>> # Obtém taxa para um vencimento de contrato existente
>>> taxa = di1.interpolar_taxa("25-04-2025", "01-01-2027")
>>> f"{taxa:.3%}"
'13.901%'
```

```pycon
>>> # Obtém taxa para um vencimento não existente
>>> di1.interpolar_taxa("25-04-2025", "01-11-2027")
0.13576348733268917
```

```pycon
>>> # Extrapola taxa para uma data de vencimento futura
>>> taxa = di1.interpolar_taxa("25-04-2025", "01-01-2050", extrapolar=True)
>>> f"{taxa:.3%}"
'13.881%'
```

## `interpolar_taxas(datas_referencia, datas_vencimento, extrapolar=True, tipo_taxa='ajuste')`

Interpola taxas de DI para datas de negociação e vencimentos especificados.

Calcula taxas de DI interpoladas usando o método **flat-forward** para conjuntos de datas de negociação e vencimentos. Esta função é adequada para cálculos vetorizados com múltiplos pares de datas.

Se taxas de DI não estiverem disponíveis para uma data de negociação, as taxas interpoladas correspondentes serão NaN.

Trata broadcasting: Se um argumento for escalar e o outro for array, o valor escalar é aplicado a todos os elementos do array.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `datas_referencia` | `DateLike \| DatesLike` | Data(s) de negociação para as taxas. | *required* |
| `datas_vencimento` | `DateLike \| DatesLike` | Data(s) de vencimento correspondentes. Deve ser compatível em tamanho com `datas_referencia` se ambos forem arrays. | *required* |
| `extrapolar` | `bool` | Se permite extrapolação além do intervalo de taxas DI conhecidas para uma data de negociação. Padrão: True. | `True` |
| `tipo_taxa` | `TipoTaxaDI1` | Tipo de taxa usada como curva de referência. Use `"ajuste"` para a taxa de ajuste ou `"fechamento"` para a última taxa negociada. Padrão: `"ajuste"`. | `'ajuste'` |

Returns:

| Type | Description |
| --- | --- |
| `Series` | Series contendo as taxas DI interpoladas (como floats). Valores serão |
| `Series` | NaN onde interpolação não for possível (ex: sem dados DI para a data |
| `Series` | de negociação). As taxas são retornadas em formato decimal; os exemplos |
| `Series` | abaixo as multiplicam por 100 apenas para facilitar a leitura. |

Raises:

| Type | Description |
| --- | --- |
| `ValueError` | Se `datas_referencia` e `datas_vencimento` forem ambos array-like mas tiverem tamanhos diferentes. |
| `ValueError` | Se `tipo_taxa` não for `"ajuste"` ou `"fechamento"`. |

**Notes**

- Todas as taxas disponíveis do tipo escolhido são usadas para interpolação flat-forward.
- `tipo_taxa="fechamento"` usa a última taxa negociada (last trade), que pode ter menos vértices disponíveis que a curva de ajuste.
- A função trata broadcasting de entradas escalares e array-like.

Examples:

Interpola taxas para múltiplas datas de negociação e vencimento:

```pycon
>>> # Para contrato com vencimento 01-01-2027 em 08-05-2025
>>> # A taxa não é interpolada (taxa de liquidação é usada)
>>> # Não há contrato com vencimento 25-11-2027 em 09-05-2025
>>> # A taxa é interpolada (método flat-forward)
>>> # Não há dados para 10-05-2025 (sábado) -> NaN
>>> from pyield import di1
>>> di1.interpolar_taxas(
...     datas_referencia=["08-05-2025", "09-05-2025", "10-05-2025"],
...     datas_vencimento=["01-01-2027", "25-11-2027", "01-01-2030"],
... ) * 100
shape: (3,)
Series: 'taxa_interpolada' [f64]
[
    13.972
    13.461282
    null
]
```

Interpola taxas para uma data de negociação e múltiplos vencimentos:

```pycon
>>> di1.interpolar_taxas(
...     datas_referencia="25-04-2025",
...     datas_vencimento=["01-01-2027", "01-01-2050"],
...     extrapolar=True,
... ) * 100
shape: (2,)
Series: 'taxa_interpolada' [f64]
[
    13.901
    13.881
]
```

```pycon
>>> # Com extrapolação desabilitada, vencimentos fora do intervalo retornam null
>>> di1.interpolar_taxas(
...     datas_referencia="25-04-2025",
...     datas_vencimento=["01-11-2027", "01-01-2050"],
...     extrapolar=False,
... ) * 100
shape: (2,)
Series: 'taxa_interpolada' [f64]
[
    13.576349
    null
]
```
