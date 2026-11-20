Classe interpoladora para interpolação de taxas de juros.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `dias_uteis` | `ArrayLike` | Sequência de dias úteis (DU) conhecidos, de tipo inteiro. Vértices com DU negativo são ignorados, assim como os de DU nulo ou de taxa nula ou NaN. | *required* |
| `taxas` | `ArrayLike` | Sequência de taxas de juros conhecidas. | *required* |
| `metodo` | `Literal['flat_forward', 'linear']` | Método de interpolação a usar. Opções: "flat\_forward" ou "linear". | *required* |
| `extrapolar` | `bool` | Controla apenas o comportamento na ponta longa (DU acima do maior vértice conhecido). Se True, retorna a última taxa conhecida; se False (padrão), retorna NaN. A ponta curta (DU abaixo do menor vértice, inclusive zero) sempre retorna a primeira taxa conhecida, independentemente desta flag. | `False` |

Raises:

| Type | Description |
| --- | --- |
| `ValueError` | Se `metodo` não for `"flat_forward"` nem `"linear"`, se não houver ao menos um vértice válido na curva ou se a curva tiver o mesmo prazo com taxas diferentes. |
| `TypeError` | Se `dias_uteis` não for de tipo inteiro. |

**Notes**

- Esta classe usa convenção de 252 dias úteis por ano.
- Instâncias desta classe são **imutáveis**. Para modificar as configurações de interpolação, crie uma nova instância.

Para um prazo alvo `du`, a classe encontra automaticamente os dois vértices conhecidos que o cercam: `j` é o anterior e `k` é o seguinte. O usuário informa apenas `du`; os índices `j` e `k` são detalhes internos.

No método `"linear"`, a taxa é calculada por:

\\\[ tx\_{du} = tx\_j + \\frac{(du - du\_j)(tx\_k - tx\_j)}{du\_k - du\_j} \\\]

No método `"flat_forward"`, definindo \\(t = du/252\\), \\(t\_x = du\_x/252\\), \\(F\_x = 1 + tx\_x\\), \\(C\_x = F\_x^{t\_x}\\) e \\(w = (t - t\_j)/(t\_k - t\_j)\\), a taxa é:

\\\[ tx\_{du} = \\left(C\_j\\left(\\frac{C\_k}{C\_j}\\right)^w\\right)^{1/t} - 1 \\\]

A interpolação flat-forward interpola o fator acumulado entre os dois vértices e converte o resultado de volta para uma taxa no prazo alvo.

Examples:

```pycon
>>> from pyield import Interpolador
>>> dus = [30, 60, 90]
>>> txs = [0.045, 0.05, 0.055]
```

Interpolação linear:

```pycon
>>> linear = Interpolador(dus, txs, "linear")
>>> linear(45)
0.0475
```

Interpolação flat forward:

```pycon
>>> flat_fwd = Interpolador(dus, txs, "flat_forward")
>>> flat_fwd(45)
0.04833068080970859
```

```pycon
>>> print(flat_fwd(100))  # Extrapolação desabilitada por padrão
nan
```

```pycon
>>> print(flat_fwd(-10))  # Entrada inválida retorna NaN
nan
```

Vários prazos de uma vez; os casos sem taxa ficam nulos:

```pycon
>>> flat_fwd([45, 100, -10])
shape: (3,)
Series: 'taxa_interpolada' [f64]
[
    0.048331
    null
    null
]
```

Se extrapolação estiver habilitada, a última taxa conhecida é usada:

```pycon
>>> flat_fwd_extrap = Interpolador(dus, txs, "flat_forward", extrapolar=True)
>>> flat_fwd_extrap(100)
0.055
```

## `interpolar(du)`

Interpola a taxa para um dia útil ou para uma sequência de dias úteis.

A instância pode ser chamada diretamente: `flat_forward(du)` é o atalho de `flat_forward.interpolar(du)`.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `du` | `int \| ArrayLike` | DU inteiro ou sequência de DUs inteiros (lista, tupla ou `pl.Series`). | *required* |

Returns:

| Type | Description |
| --- | --- |
| `float \| Series` | Para DU escalar, a taxa interpolada como float; `float("nan")` |
| `float \| Series` | quando o DU for negativo ou estiver acima do maior vértice |
| `float \| Series` | conhecido com `extrapolar=False`. Para uma sequência, Series |
| `float \| Series` | Float64 `taxa_interpolada` na mesma ordem da entrada, com |
| `float \| Series` | `null` nesses mesmos casos e quando o DU for nulo. DU abaixo |
| `float \| Series` | do menor vértice, inclusive zero, sempre retorna a primeira taxa |
| `float \| Series` | conhecida. |

Raises:

| Type | Description |
| --- | --- |
| `TypeError` | Se `du` não for um inteiro nem uma sequência de tipo inteiro. Float é rejeitado, mesmo com valor inteiro. |

Examples:

```pycon
>>> from pyield import Interpolador
>>> flat_forward = Interpolador(
...     [30, 60, 90], [0.045, 0.05, 0.055], "flat_forward"
... )
>>> flat_forward.interpolar(45)
0.04833068080970859
```

A chamada direta da instância dá o mesmo resultado:

```pycon
>>> flat_forward(45)
0.04833068080970859
```

```pycon
>>> flat_forward([15, 45, 100])
shape: (3,)
Series: 'taxa_interpolada' [f64]
[
    0.045
    0.048331
    null
]
```

## `interpolar_expr(du)`

Cria expressão Polars que interpola taxas para uma coluna de DU.

Útil para adicionar uma coluna de taxas interpoladas a um DataFrame via `with_columns`, compondo livremente com outras expressões Polars (ex.: cálculo de inflação implícita na mesma chamada). A curva e o método são os configurados na instância.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `du` | `str \| Expr` | Nome de coluna ou expressão Polars com os DUs alvo. A coluna deve ser de tipo inteiro; outro tipo gera `TypeError` na execução da expressão. | *required* |

Returns:

| Type | Description |
| --- | --- |
| `Expr` | pl.Expr: Expressão Float64 sem alias com as taxas interpoladas. `null` quando o DU for nulo, negativo, ou estiver acima do maior vértice conhecido com `extrapolar=False`. |

Examples:

```pycon
>>> from pyield import Interpolador
>>> flat_forward = Interpolador(
...     [30, 60, 90], [0.045, 0.05, 0.055], "flat_forward"
... )
>>> df = pl.DataFrame({"du": [15, 45, 75]})
>>> df.with_columns(taxa=flat_forward.interpolar_expr("du"))
shape: (3, 2)
┌─────┬──────────┐
│ du  ┆ taxa     │
│ --- ┆ ---      │
│ i64 ┆ f64      │
╞═════╪══════════╡
│ 15  ┆ 0.045    │
│ 45  ┆ 0.048331 │
│ 75  ┆ 0.052997 │
└─────┴──────────┘
```

## interpolar

Interpola taxas em lote, com uma curva por data de referência.

Cada ponto alvo é interpolado contra a curva da sua data de referência (multi-curva, sem loop em Python). Todos os parâmetros são nomeados.

O resultado pode ser adicionado a um DataFrame via `with_columns`, preservando a ordem original de `dus_alvo`.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `datas_alvo` | `Series` | Series Date com a data de referência de cada ponto alvo. | *required* |
| `dus_alvo` | `Series` | Series Int com os dias úteis dos pontos a interpolar (mesma ordem de `datas_alvo`). | *required* |
| `datas_curva` | `Series` | Series Date com a data de referência de cada vértice da curva. | *required* |
| `dus_curva` | `Series` | Series Int com os dias úteis dos vértices conhecidos (mesma ordem de `datas_curva`). Vértices com DU negativo são ignorados, assim como os de DU nulo ou de taxa nula ou NaN. | *required* |
| `taxas_curva` | `Series` | Series Float com as taxas dos vértices conhecidos (mesma ordem de `dus_curva`). | *required* |
| `metodo` | `Literal['flat_forward', 'linear']` | Método de interpolação a usar. Opções: "flat\_forward" ou "linear". | *required* |
| `extrapolar` | `bool` | Controla apenas o comportamento na ponta longa (DU acima do maior vértice de cada grupo). Se True, retorna a última taxa conhecida; se False (padrão), retorna `null`. A ponta curta (DU abaixo do menor vértice, inclusive zero) sempre retorna a primeira taxa do grupo, independentemente desta flag. O default casa com o da classe `Interpolador`. | `False` |

Returns:

| Type | Description |
| --- | --- |
| `Series` | Series Float64 `taxa_interpolada` na mesma ordem de |
| `Series` | `dus_alvo`. `null` quando o DU é nulo ou negativo, a data alvo |
| `Series` | não existe na curva, ou o DU está acima do maior vértice e |
| `Series` | `extrapolar` é False. |

Raises:

| Type | Description |
| --- | --- |
| `ValueError` | Se `metodo` não for `"flat_forward"` nem `"linear"`, ou se uma curva tiver o mesmo prazo com taxas diferentes na mesma data. |
| `TypeError` | Se `dus_alvo` ou `dus_curva` não for de tipo inteiro. |

**Notes**

Usa as mesmas fórmulas descritas na documentação de `Interpolador`. Os vértices que cercam cada prazo alvo são encontrados automaticamente.

A função existe para o processamento em lote: interpolar de uma vez os pontos de muitas datas de referência, como no reprocessamento de uma base histórica inteira, em que criar um `Interpolador` por data seria muito mais lento. Para uma única curva, `Interpolador` é o caminho indicado e permite reutilizar a curva em várias consultas.

Examples:

Caso típico: adicionar uma coluna de taxas interpoladas a um DataFrame com múltiplas datas de referência.

```pycon
>>> df = pl.DataFrame(
...     {
...         "data_referencia": ["2025-01-02", "2025-01-02"],
...         "dias_uteis": [10, 25],
...     }
... )
>>> df_curva = pl.DataFrame(
...     {
...         "data_referencia": ["2025-01-02"] * 3,
...         "dias_uteis": [5, 20, 50],
...         "taxa": [0.10, 0.12, 0.13],
...     }
... )
>>> df.with_columns(
...     taxa=yd.interpolar(
...         datas_alvo=df["data_referencia"],
...         dus_alvo=df["dias_uteis"],
...         datas_curva=df_curva["data_referencia"],
...         dus_curva=df_curva["dias_uteis"],
...         taxas_curva=df_curva["taxa"],
...         metodo="flat_forward",
...     )
... )
shape: (2, 3)
┌─────────────────┬────────────┬──────────┐
│ data_referencia ┆ dias_uteis ┆ taxa     │
│ ---             ┆ ---        ┆ ---      │
│ str             ┆ i64        ┆ f64      │
╞═════════════════╪════════════╪══════════╡
│ 2025-01-02      ┆ 10         ┆ 0.113293 │
│ 2025-01-02      ┆ 25         ┆ 0.123323 │
└─────────────────┴────────────┴──────────┘
```
