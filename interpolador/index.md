Classe interpoladora para interpolação de taxas de juros.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `dias_uteis` | `ArrayLike` | Sequência de dias úteis (DU) conhecidos. | *required* |
| `taxas` | `ArrayLike` | Sequência de taxas de juros conhecidas. | *required* |
| `metodo` | `Literal['flat_forward', 'linear']` | Método de interpolação a usar. Opções: "flat\_forward" ou "linear". | *required* |
| `extrapolar` | `bool` | Controla apenas o comportamento na ponta longa (DU acima do maior vértice conhecido). Se True, retorna a última taxa conhecida; se False (padrão), retorna NaN. A ponta curta (DU abaixo do menor vértice) sempre retorna a primeira taxa conhecida, independentemente desta flag. | `False` |

Raises:

| Type | Description |
| --- | --- |
| `ValueError` | Se `metodo` não for `"flat_forward"` nem `"linear"`, se não houver ao menos um vértice válido na curva ou se a curva tiver o mesmo prazo com taxas diferentes. |

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

Se extrapolação estiver habilitada, a última taxa conhecida é usada:

```pycon
>>> flat_fwd_extrap = Interpolador(dus, txs, "flat_forward", extrapolar=True)
>>> flat_fwd_extrap(100)
0.055
```

## `interpolar(du)`

Interpola a taxa para um único dia útil.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `du` | `int` | DU escalar inteiro para interpolação. | *required* |

Returns:

| Type | Description |
| --- | --- |
| `float` | Taxa interpolada como float. Retorna `float("nan")` quando o DU |
| `float` | for negativo ou estiver acima do maior vértice conhecido com |
| `float` | `extrapolar=False`. DU abaixo do menor vértice sempre retorna |
| `float` | a primeira taxa conhecida. |

Raises:

| Type | Description |
| --- | --- |
| `TypeError` | Se `du` não for um inteiro. |

## `interpolar_expr(du)`

Cria expressão Polars que interpola taxas para uma coluna de DU.

Útil para adicionar uma coluna de taxas interpoladas a um DataFrame via `with_columns`, compondo livremente com outras expressões Polars (ex.: cálculo de inflação implícita na mesma chamada). A curva e o método são os configurados na instância.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `du` | `str \| Expr` | Nome de coluna ou expressão Polars com os DUs alvo. A coluna deve ser inteira (será convertida para Int64 internamente). | *required* |

Returns:

| Type | Description |
| --- | --- |
| `Expr` | pl.Expr: Expressão Float64 sem alias com as taxas interpoladas. `null` quando o DU for nulo, negativo, ou estiver acima do maior vértice conhecido com `extrapolar=False`. |

Examples:

```pycon
>>> from pyield import Interpolador
>>> interp = Interpolador(
...     [30, 60, 90], [0.045, 0.05, 0.055], "flat_forward"
... )
>>> df = pl.DataFrame({"du": [15, 45, 75]})
>>> df.with_columns(taxa=interp.interpolar_expr("du"))
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

Interpola taxas flat-forward para uma série de pontos alvo.

Versão vetorizada de `Interpolador`. Quando `datas_alvo` e `datas_curva` são fornecidas, cada ponto é interpolado contra a curva da sua data correspondente (multi-curva, sem loop em Python). Quando ambas são `None`, todos os pontos usam a mesma curva.

O resultado pode ser adicionado a um DataFrame via `with_columns`, preservando a ordem original de `dus_alvo`.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `dus_alvo` | `Series` | Series Int com os dias úteis dos pontos a interpolar. | *required* |
| `dus_curva` | `Series` | Series Int com os dias úteis dos vértices conhecidos. | *required* |
| `taxas_curva` | `Series` | Series Float com as taxas dos vértices conhecidos (mesma ordem de `dus_curva`). | *required* |
| `datas_alvo` | `Series \| None` | Series Date com a data de referência de cada ponto alvo. Quando fornecida, `datas_curva` também deve ser. Padrão: `None` (curva única). | `None` |
| `datas_curva` | `Series \| None` | Series Date com a data de referência de cada vértice da curva. Padrão: `None` (curva única). | `None` |
| `extrapolar` | `bool` | Controla apenas o comportamento na ponta longa (DU acima do maior vértice de cada grupo). Se True, retorna a última taxa conhecida; se False (padrão), retorna `null`. A ponta curta (DU abaixo do menor vértice) sempre retorna a primeira taxa do grupo, independentemente desta flag. O default casa com o da classe `Interpolador`. | `False` |

Returns:

| Type | Description |
| --- | --- |
| `Series` | Series Float64 `taxa_interpolada` na mesma ordem de |
| `Series` | `dus_alvo`. `null` quando o DU é nulo, a data alvo não existe |
| `Series` | na curva, ou o DU está acima do maior vértice e `extrapolar` é |
| `Series` | False. |

Raises:

| Type | Description |
| --- | --- |
| `ValueError` | Se apenas uma de `datas_alvo` ou `datas_curva` for fornecida, ou se a curva tiver o mesmo prazo com taxas diferentes (na mesma data, quando há várias curvas). |

**Notes**

Usa a mesma fórmula flat-forward descrita na documentação de `Interpolador`. Os vértices que cercam cada prazo alvo são encontrados automaticamente.

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
...         dus_alvo=df["dias_uteis"],
...         dus_curva=df_curva["dias_uteis"],
...         taxas_curva=df_curva["taxa"],
...         datas_alvo=df["data_referencia"],
...         datas_curva=df_curva["data_referencia"],
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

Curva única (sem datas):

```pycon
>>> yd.interpolar(
...     dus_alvo=pl.Series([10, 25]),
...     dus_curva=pl.Series([5, 20, 50]),
...     taxas_curva=pl.Series([0.10, 0.12, 0.13]),
... )
shape: (2,)
Series: 'taxa_interpolada' [f64]
[
    0.113293
    0.123323
]
```
