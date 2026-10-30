## `forward(du1, du2, taxa1, taxa2)`

Calcula a taxa a termo entre dois prazos (dias úteis).

Definindo \\(F\_i = 1 + tx\_i\\) e \\(t\_i = du\_i/252\\), onde 252 é o número de dias úteis no ano, a fórmula é:

\\\[ f\_{1 \\rightarrow 2} = \\left( \\frac{F\_2^{t\_2}}{F\_1^{t\_1}} \\right)^{\\frac{1}{t\_2 - t\_1}} - 1 \\\]

Aqui, `F₁` e `F₂` são os fatores de capitalização dos prazos `du1` e `du2`, e `tx1` e `tx2` são as respectivas taxas zero.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `du1` | `int` | Número de dias úteis do primeiro ponto (prazo menor). | *required* |
| `du2` | `int` | Número de dias úteis do segundo ponto (prazo maior). | *required* |
| `taxa1` | `float` | Taxa zero para o prazo `du1`. | *required* |
| `taxa2` | `float` | Taxa zero para o prazo `du2`. | *required* |

Returns:

| Name | Type | Description |
| --- | --- | --- |
| `float` | `float` | A taxa a termo calculada entre `du₁` e `du₂`. Retorna `nan` se `du₁ >= du₂` ou se qualquer um dos argumentos de entrada for `float("nan")` ou `None`. |

Examples:

```pycon
>>> # Exemplo válido: du₂ > du₁
>>> yd.forward(10, 20, 0.05, 0.06)
0.0700952380952371
>>> # Exemplo inválido: du₁ >= du₂
>>> print(yd.forward(20, 10, 0.06, 0.05))
nan
```

**Notes**

`du₂` precisa ser necessariamente maior que `du₁` para que o cálculo da taxa a termo seja matematicamente válido.

## `forwards(dias_uteis, taxas, agrupar_por=None)`

Calcula taxas a termo a partir de taxas zero.

A taxa a termo em cada vértice é calculada aplicando sucessivamente a fórmula documentada em :func:`forward` aos vértices consecutivos. A primeira taxa de cada grupo é a taxa zero do primeiro vértice, pois não há um vértice anterior para calcular a taxa a termo.

A função preserva a ordem original dos dados de entrada. Nulos em `dias_uteis` ou `taxas` produzem nulo na linha correspondente; NaN produz NaN (nulos e NaN não se misturam). Duplicatas em `(agrupar_por, dias_uteis)` tornam o vértice ambíguo e produzem nulo nas linhas duplicadas. Em todos esses casos a linha imediatamente posterior em ordem de `dias_uteis` dentro do mesmo grupo também fica inválida (nula ou NaN), pois seu vértice anterior é inválido; a seguinte volta ao normal. Este contrato é alinhado com :func:`forwards_expr`.

A primeira taxa a termo de cada grupo é definida como a taxa zero desse primeiro vértice (fwd₁ = tx₁), dado que não existe um vértice anterior a tx₁ para se calcular a taxa a termo no primeiro ponto.

A função também lida com agrupamentos opcionais, permitindo calcular taxas a termo para diferentes grupos de dados. O agrupamento é feito com base em `agrupar_por`. Se este argumento for None, todos os dados serão tratados como um único grupo.

A função calcula as taxas a termo para todos os pontos, exceto o primeiro de cada grupo, que é tratado separadamente.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `dias_uteis` | `ArrayLike` | Número de dias úteis (du) para cada taxa zero. | *required* |
| `taxas` | `ArrayLike` | Taxas zero (tx) correspondentes aos dias úteis. | *required* |
| `agrupar_por` | `Sequence[str \| int \| date] \| Series \| None` | Critério de agrupamento para os cálculos (ex: datas de referência, tickers de títulos). Pode ser uma lista/série de strings, inteiros ou datas. Se None, todos os dados são tratados como um único grupo. Padrão None. | `None` |

Returns:

| Type | Description |
| --- | --- |
| `Series` | pl.Series: Série contendo as taxas a termo calculadas (tipo Float64), em formato decimal. A primeira taxa de cada grupo corresponde à taxa zero inicial. |

Raises:

| Type | Description |
| --- | --- |
| `ShapeError` | Se os comprimentos de `dias_uteis`, `taxas` e `agrupar_por` (quando fornecido) não forem iguais. |

Examples:

```pycon
>>> dias_uteis = [10, 20, 30]
>>> taxas = [0.05, 0.06, 0.07]
>>> yd.forwards(dias_uteis, taxas)
shape: (3,)
Series: 'taxa_forward' [f64]
[
    0.05
    0.070095
    0.090284
]
```

```pycon
>>> # Exemplo com agrupamento (a última está isolada em outro grupo)
>>> agrupar_por = ["LTN", "LTN", "NTN-F"]
>>> yd.forwards(dias_uteis, taxas, agrupar_por)
shape: (3,)
Series: 'taxa_forward' [f64]
[
    0.05
    0.070095
    0.07
]
```

```pycon
>>> # Exemplo com taxas indicativas de NTN-B em 16-09-2025
>>> from pyield import ntnb
>>> df = ntnb.dados("16-09-2025")
>>> yd.forwards(df["dias_uteis"], df["taxa_indicativa"]) * 100
shape: (13,)
Series: 'taxa_forward' [f64]
[
    9.43
    7.154918
    7.243863
    6.955808
    7.661395
    …
    6.810503
    7.127822
    6.911658
    7.037305
    7.32861
]
```

```pycon
>>> # Valores nulos em ``dias_uteis`` produzem nulo na própria linha;
>>> # como nulos vão para o fim em ordem de ``dias_uteis``, a linha
>>> # com du=914 vê 730 como vértice anterior e calcula normalmente.
>>> du = [230, 415, 730, None, 914]
>>> tx = [0.0943, 0.084099, 0.079052, 0.1, 0.077134]
>>> yd.forwards(du, tx)
shape: (5,)
Series: 'taxa_forward' [f64]
[
    0.0943
    0.071549
    0.072439
    null
    0.069558
]
```

```pycon
>>> # Já um nulo em ``taxas`` propaga em cascata: a própria linha fica
>>> # nula e a próxima em ordem de ``dias_uteis`` também, pois seu
>>> # vértice anterior é nulo. A linha seguinte volta ao normal.
>>> du = [230, 415, 730, 914]
>>> tx = [0.0943, None, 0.079052, 0.077134]
>>> yd.forwards(du, tx)
shape: (4,)
Series: 'taxa_forward' [f64]
[
    0.0943
    null
    null
    0.069558
]
```

```pycon
>>> # NaN se comporta como nulo, mas mantém o dtype NaN no resultado
>>> # (nulos e NaN não se misturam).
>>> du = [230, 415, 730, 914]
>>> tx = [0.0943, float("nan"), 0.079052, 0.077134]
>>> yd.forwards(du, tx)
shape: (4,)
Series: 'taxa_forward' [f64]
[
    0.0943
    NaN
    NaN
    0.069558
]
```

```pycon
>>> # Duplicatas em ``dias_uteis`` dentro de um grupo tornam o
>>> # vértice ambíguo: ambas as linhas duplicadas ficam nulas e a
>>> # linha imediatamente posterior em ordem de ``dias_uteis`` também
>>> # (cascata local de nulos). A linha seguinte volta ao normal.
>>> du = [230, 730, 415, 230]
>>> tx = [0.1, 0.079052, 0.084099, 0.0943]
>>> yd.forwards(du, tx)
shape: (4,)
Series: 'taxa_forward' [f64]
[
    null
    0.072439
    null
    null
]
```

**Notes**

- A função ordena os dados de entrada primeiro por `agrupar_por`, se for fornecido, e depois por `dias_uteis` para garantir a ordem cronológica correta no cálculo das taxas a termo.
- Nulos em `dias_uteis` ou `taxas` produzem nulo na linha correspondente; NaN produz NaN. Duplicatas em `(agrupar_por,   dias_uteis)` tornam o vértice ambíguo e produzem nulo nas linhas duplicadas. Em todos esses casos, a linha imediatamente posterior em ordem de `dias_uteis` dentro do mesmo grupo também fica inválida; a seguinte volta ao normal. Mesmo contrato de :func:`forwards_expr`.
- Os resultados são retornados na mesma ordem dos dados de entrada.

## `forwards_expr(dias_uteis, taxas, agrupar_por=None)`

Cria uma expressão Polars para calcular taxas a termo dentro de DataFrames.

Versão expressiva de :func:`forwards`, no mesmo estilo de `du.contar_expr`. Pensada para uso dentro de `with_columns()` ou `select()`, especialmente em DataFrames com múltiplas datas de referência (ex.: séries históricas de DI1), onde `agrupar_por` define janelas independentes de cálculo sem que seja necessário extrair colunas, calcular fora e juntar de volta.

A taxa a termo entre os vértices `j` (anterior) e `k` (atual) segue a fórmula documentada em :func:`forward`. A primeira linha de cada grupo (menor `dias_uteis`) é tratada como spot: `f = tx`.

**Ordenação cronológica**

A expressão usa `shift(1).over(agrupar_por, order_by=dias_uteis)`, calculando a taxa do vértice anterior em ordem de `dias_uteis` dentro de cada grupo **sem reordenar o DataFrame** de origem.

**Propagação de nulos e NaN**

Se `dias_uteis` ou `taxas` for nulo em uma linha, o resultado é nulo nessa linha; se for NaN, o resultado é NaN (nulos e NaN não se misturam). Em ambos os casos, a linha imediatamente posterior em ordem de `dias_uteis` dentro do mesmo grupo também fica nula/NaN, pois seu vértice anterior é inválido. Como nulos são ordenados ao fim em `order_by=dias_uteis`, um null em `dias_uteis` afeta apenas a própria linha. Mesmo contrato de :func:`forwards`.

Duplicatas em `(agrupar_por, dias_uteis)`: Duplicatas tornam o vértice ambíguo. A função invalida a taxa com nulo nas próprias linhas duplicadas; a partir daí o cascateamento natural de nulos vale (a linha imediatamente posterior em ordem de `dias_uteis` dentro do grupo também fica nula; a seguinte volta ao normal). Mesmo contrato de :func:`forwards`.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `dias_uteis` | `Expr \| str` | Nome de coluna ou expressão Polars com o prazo em dias úteis para cada taxa zero. | *required* |
| `taxas` | `Expr \| str` | Nome de coluna ou expressão Polars com a taxa zero correspondente a cada `dias_uteis`. | *required* |
| `agrupar_por` | `Expr \| str \| None` | Nome de coluna, expressão ou `None`. Define janelas independentes de cálculo (ex.: `"data_referencia"`, `"ticker"`). Quando `None`, todas as linhas formam um único grupo. Padrão `None`. | `None` |

Returns:

| Type | Description |
| --- | --- |
| `Expr` | pl.Expr: Expressão Float64 com a taxa a termo de cada linha. A |
| `Expr` | expressão não recebe alias; nomeie no momento do uso, por exemplo |
| `Expr` | via `with_columns(taxa_forward=forwards_expr(...))`. |

Examples:

```pycon
>>> df = pl.DataFrame(
...     {
...         "data_referencia": ["2025-01-02"] * 3 + ["2025-01-03"] * 3,
...         "dias_uteis": [10, 20, 30, 10, 20, 30],
...         "taxa": [0.05, 0.06, 0.07, 0.06, 0.07, 0.08],
...     }
... )
>>> df.with_columns(
...     taxa_forward=yd.forwards_expr(
...         "dias_uteis", "taxa", agrupar_por="data_referencia"
...     )
... )
shape: (6, 4)
┌─────────────────┬────────────┬──────┬──────────────┐
│ data_referencia ┆ dias_uteis ┆ taxa ┆ taxa_forward │
│ ---             ┆ ---        ┆ ---  ┆ ---          │
│ str             ┆ i64        ┆ f64  ┆ f64          │
╞═════════════════╪════════════╪══════╪══════════════╡
│ 2025-01-02      ┆ 10         ┆ 0.05 ┆ 0.05         │
│ 2025-01-02      ┆ 20         ┆ 0.06 ┆ 0.070095     │
│ 2025-01-02      ┆ 30         ┆ 0.07 ┆ 0.090284     │
│ 2025-01-03      ┆ 10         ┆ 0.06 ┆ 0.06         │
│ 2025-01-03      ┆ 20         ┆ 0.07 ┆ 0.080094     │
│ 2025-01-03      ┆ 30         ┆ 0.08 ┆ 0.100281     │
└─────────────────┴────────────┴──────┴──────────────┘
```

Sem agrupamento, todas as linhas formam um único grupo:

```pycon
>>> df = pl.DataFrame({"du": [10, 20, 30], "tx": [0.05, 0.06, 0.07]})
>>> df.with_columns(fwd=yd.forwards_expr("du", "tx"))
shape: (3, 3)
┌─────┬──────┬──────────┐
│ du  ┆ tx   ┆ fwd      │
│ --- ┆ ---  ┆ ---      │
│ i64 ┆ f64  ┆ f64      │
╞═════╪══════╪══════════╡
│ 10  ┆ 0.05 ┆ 0.05     │
│ 20  ┆ 0.06 ┆ 0.070095 │
│ 30  ┆ 0.07 ┆ 0.090284 │
└─────┴──────┴──────────┘
```

Duplicatas em `(grupo, du)` invalidam o vértice. Aqui o segundo grupo (`2025-01-03`) tem duplicata em `du=20`: ambas as linhas com `du=20` ficam nulas e a linha `du=30` também (cascata local); o primeiro grupo permanece intacto.

```pycon
>>> df = pl.DataFrame(
...     {
...         "dr": ["2025-01-02"] * 3 + ["2025-01-03"] * 4,
...         "du": [10, 20, 30, 10, 20, 20, 30],
...         "tx": [0.05, 0.06, 0.07, 0.05, 0.06, 0.061, 0.07],
...     }
... )
>>> df.with_columns(fwd=yd.forwards_expr("du", "tx", agrupar_por="dr"))
shape: (7, 4)
┌────────────┬─────┬───────┬──────────┐
│ dr         ┆ du  ┆ tx    ┆ fwd      │
│ ---        ┆ --- ┆ ---   ┆ ---      │
│ str        ┆ i64 ┆ f64   ┆ f64      │
╞════════════╪═════╪═══════╪══════════╡
│ 2025-01-02 ┆ 10  ┆ 0.05  ┆ 0.05     │
│ 2025-01-02 ┆ 20  ┆ 0.06  ┆ 0.070095 │
│ 2025-01-02 ┆ 30  ┆ 0.07  ┆ 0.090284 │
│ 2025-01-03 ┆ 10  ┆ 0.05  ┆ 0.05     │
│ 2025-01-03 ┆ 20  ┆ 0.06  ┆ null     │
│ 2025-01-03 ┆ 20  ┆ 0.061 ┆ null     │
│ 2025-01-03 ┆ 30  ┆ 0.07  ┆ null     │
└────────────┴─────┴───────┴──────────┘
```

**Notes**

- A ordem original do DataFrame é preservada.
- Para uso fora de DataFrames (arrays, Series soltas), use :func:`forwards`.
