import bisect
import numbers
from typing import Any, Literal, overload

import polars as pl

from pyield._internal.types import ArrayLike, is_array_like


def _rejeitar_vertices_conflitantes(
    df: pl.DataFrame, du: str, tx: str, grupo: str | None = None
) -> pl.DataFrame:
    """Remove vértices idênticos e rejeita o mesmo DU com taxas diferentes.

    Vértices repetidos com a mesma taxa não são ambíguos e ficam uma única
    vez. O mesmo DU com taxas diferentes não tem taxa correta a escolher e
    gera erro, que informa o DU, as taxas e, se houver, a data da curva.
    """
    chaves = [du] if grupo is None else [grupo, du]
    df = df.unique(maintain_order=True)
    conflitos = (
        df.filter(pl.len().over(chaves) > 1)
        .group_by(chaves, maintain_order=True)
        .agg(pl.col(tx))
    )
    if conflitos.is_empty():
        return df
    descricoes = [
        (f"{linha[grupo]}, " if grupo is not None else "")
        + f"DU {linha[du]}: taxas {linha[tx]}"
        for linha in conflitos.head(5).iter_rows(named=True)
    ]
    restantes = conflitos.height - len(descricoes)
    sufixo = f" (e mais {restantes})" if restantes > 0 else ""
    raise ValueError(
        "Curva tem o mesmo prazo com taxas diferentes; informe uma única "
        f"taxa por prazo. Conflitos: {'; '.join(descricoes)}{sufixo}."
    )


def _dus_inteiros(dus: pl.Series, nome: str) -> pl.Series:
    """Exige DUs de tipo inteiro e os devolve como Int64.

    Série vazia ou só com nulos não tem tipo definido e é aceita. O prazo em
    dias úteis é inteiro por definição: uma coluna float, texto ou decimal
    indica erro anterior no pipeline e não é convertida em silêncio.
    """
    if dus.dtype != pl.Null and not dus.dtype.is_integer():
        raise TypeError(
            f"{nome} deve ter tipo inteiro, mas tem {dus.dtype}. "
            "Converta os prazos para inteiro antes de interpolar."
        )
    return dus.cast(pl.Int64)


def _linear(du: Any, du_j: Any, tx_j: Any, du_k: Any, tx_k: Any) -> Any:
    """Interpola linearmente a taxa entre os vértices j e k."""
    return tx_j + (du - du_j) * (tx_k - tx_j) / (du_k - du_j)


def _flat_forward(du: Any, du_j: Any, tx_j: Any, du_k: Any, tx_k: Any) -> Any:
    """Interpola o fator acumulado entre os vértices j e k (flat-forward)."""
    au, au_j, au_k = du / 252, du_j / 252, du_k / 252
    # Fator acumulado de cada vértice e peso temporal da interpolação.
    fa_j = (1 + tx_j) ** au_j
    fa_k = (1 + tx_k) ** au_k
    peso = (au - au_j) / (au_k - au_j)
    return (fa_j * (fa_k / fa_j) ** peso) ** (1 / au) - 1


# As fórmulas usam apenas operadores aritméticos e por isso servem tanto à
# classe (float) quanto à função ``interpolar`` (pl.Expr); daí os argumentos Any.
_FORMULAS = {"flat_forward": _flat_forward, "linear": _linear}


class Interpolador:
    r"""Classe interpoladora para interpolação de taxas de juros.

    Args:
        dias_uteis: Sequência de dias úteis (DU) conhecidos, de tipo inteiro.
            Vértices com DU negativo são ignorados, assim como os de DU nulo
            ou de taxa nula ou NaN.
        taxas: Sequência de taxas de juros conhecidas.
        metodo: Método de interpolação a usar. Opções: "flat_forward" ou "linear".
        extrapolar: Controla apenas o comportamento na ponta longa (DU acima
            do maior vértice conhecido). Se True, retorna a última taxa
            conhecida; se False (padrão), retorna NaN. A ponta curta (DU
            abaixo do menor vértice, inclusive zero) sempre retorna a primeira
            taxa conhecida, independentemente desta flag.

    Raises:
        ValueError: Se ``metodo`` não for ``"flat_forward"`` nem ``"linear"``,
            se não houver ao menos um vértice válido na curva ou se a curva
            tiver o mesmo prazo com taxas diferentes.
        TypeError: Se ``dias_uteis`` não for de tipo inteiro.

    Notes:
        - Esta classe usa convenção de 252 dias úteis por ano.
        - Instâncias desta classe são **imutáveis**. Para modificar as
          configurações de interpolação, crie uma nova instância.

        Para um prazo alvo ``du``, a classe encontra automaticamente os dois
        vértices conhecidos que o cercam: ``j`` é o anterior e ``k`` é o
        seguinte. O usuário informa apenas ``du``; os índices ``j`` e ``k``
        são detalhes internos.

        No método ``"linear"``, a taxa é calculada por:

        \[
        tx_{du} = tx_j + \frac{(du - du_j)(tx_k - tx_j)}{du_k - du_j}
        \]

        No método ``"flat_forward"``, definindo
        \(t = du/252\), \(t_x = du_x/252\), \(F_x = 1 + tx_x\),
        \(C_x = F_x^{t_x}\) e \(w = (t - t_j)/(t_k - t_j)\), a taxa é:

        \[
        tx_{du} = \left(C_j\left(\frac{C_k}{C_j}\right)^w\right)^{1/t} - 1
        \]

        A interpolação flat-forward interpola o fator acumulado entre os dois
        vértices e converte o resultado de volta para uma taxa no prazo alvo.

    Examples:
        >>> from pyield import Interpolador
        >>> dus = [30, 60, 90]
        >>> txs = [0.045, 0.05, 0.055]

        Interpolação linear:
        >>> linear = Interpolador(dus, txs, "linear")
        >>> linear(45)
        0.0475

        Interpolação flat forward:
        >>> flat_fwd = Interpolador(dus, txs, "flat_forward")
        >>> flat_fwd(45)
        0.04833068080970859

        >>> print(flat_fwd(100))  # Extrapolação desabilitada por padrão
        nan

        >>> print(flat_fwd(-10))  # Entrada inválida retorna NaN
        nan

        Vários prazos de uma vez; os casos sem taxa ficam nulos:
        >>> flat_fwd([45, 100, -10])
        shape: (3,)
        Series: 'taxa_interpolada' [f64]
        [
            0.048331
            null
            null
        ]

        Se extrapolação estiver habilitada, a última taxa conhecida é usada:
        >>> flat_fwd_extrap = Interpolador(dus, txs, "flat_forward", extrapolar=True)
        >>> flat_fwd_extrap(100)
        0.055
    """

    def __init__(
        self,
        dias_uteis: ArrayLike,
        taxas: ArrayLike,
        metodo: Literal["flat_forward", "linear"],
        extrapolar: bool = False,
    ):
        if metodo not in _FORMULAS:
            raise ValueError(f"Método de interpolação '{metodo}' não reconhecido.")
        dus = _dus_inteiros(pl.Series("dus", dias_uteis), "dias_uteis")
        df = (
            pl.DataFrame({"dus": dus, "txs": taxas})
            .with_columns(txs=pl.col("txs").cast(pl.Float64))
            .drop_nulls()
            .drop_nans()
            .filter(pl.col("dus") >= 0)
        )
        df = _rejeitar_vertices_conflitantes(df, "dus", "txs").sort("dus")
        if df.is_empty():
            raise ValueError("A curva deve conter ao menos um vértice válido.")
        self._df = df
        self._metodo = str(metodo)
        self._dus = tuple(df.get_column("dus"))
        self._txs = tuple(df.get_column("txs"))
        self._extrapolar = bool(extrapolar)

    @overload
    def interpolar(self, du: int) -> float: ...
    @overload
    def interpolar(self, du: ArrayLike) -> pl.Series: ...
    def interpolar(self, du: int | ArrayLike) -> float | pl.Series:
        """Interpola a taxa para um dia útil ou para uma sequência de dias úteis.

        A instância pode ser chamada diretamente: ``flat_forward(du)`` é o
        atalho de ``flat_forward.interpolar(du)``.

        Args:
            du: DU inteiro ou sequência de DUs inteiros (lista, tupla ou
                ``pl.Series``).

        Returns:
            Para DU escalar, a taxa interpolada como float; ``float("nan")``
            quando o DU for negativo ou estiver acima do maior vértice
            conhecido com ``extrapolar=False``. Para uma sequência, Series
            Float64 ``taxa_interpolada`` na mesma ordem da entrada, com
            ``null`` nesses mesmos casos e quando o DU for nulo. DU abaixo
            do menor vértice, inclusive zero, sempre retorna a primeira taxa
            conhecida.

        Raises:
            TypeError: Se ``du`` não for um inteiro nem uma sequência de tipo
                inteiro. Float é rejeitado, mesmo com valor inteiro.

        Examples:
            >>> from pyield import Interpolador
            >>> flat_forward = Interpolador(
            ...     [30, 60, 90], [0.045, 0.05, 0.055], "flat_forward"
            ... )
            >>> flat_forward.interpolar(45)
            0.04833068080970859

            A chamada direta da instância dá o mesmo resultado:
            >>> flat_forward(45)
            0.04833068080970859

            >>> flat_forward([15, 45, 100])
            shape: (3,)
            Series: 'taxa_interpolada' [f64]
            [
                0.045
                0.048331
                null
            ]
        """
        if is_array_like(du):
            return self._interpolar_serie(du)
        # Aceita qualquer tipo integral (int, np.int64, etc) e rejeita float/string.
        if not isinstance(du, numbers.Integral):
            raise TypeError(
                "du deve ser int ou sequência de inteiros. "
                "Use interpolar_expr para coluna Polars."
            )
        return self._taxa_interpolada(int(du))

    # Chamar a instância é o atalho de ``interpolar``: ``flat_forward(du)``.
    __call__ = interpolar

    def _interpolar_serie(self, du: ArrayLike) -> pl.Series:
        """Interpola uma sequência de DUs; também serve a ``interpolar_expr``."""
        return (
            _dus_inteiros(pl.Series("taxa_interpolada", du), "du")
            .map_elements(self._taxa_interpolada, return_dtype=pl.Float64)
            .fill_nan(None)
        )

    def interpolar_expr(self, du: str | pl.Expr) -> pl.Expr:
        """Cria expressão Polars que interpola taxas para uma coluna de DU.

        Útil para adicionar uma coluna de taxas interpoladas a um DataFrame via
        ``with_columns``, compondo livremente com outras expressões Polars (ex.:
        cálculo de inflação implícita na mesma chamada). A curva e o método são
        os configurados na instância.

        Args:
            du: Nome de coluna ou expressão Polars com os DUs alvo. A coluna
                deve ser de tipo inteiro; outro tipo gera ``TypeError`` na
                execução da expressão.

        Returns:
            pl.Expr: Expressão Float64 sem alias com as taxas interpoladas.
                ``null`` quando o DU for nulo, negativo, ou estiver acima do
                maior vértice conhecido com ``extrapolar=False``.

        Examples:
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
        """
        expr = pl.col(du) if isinstance(du, str) else du
        return expr.map_batches(
            self._interpolar_serie, return_dtype=pl.Float64, is_elementwise=True
        )

    def _taxa_interpolada(self, du: int) -> float:
        """Encontra o ponto de interpolação apropriado e retorna a taxa de juros.

        A taxa é interpolada pelo método especificado a partir desse ponto.

        Args:
            du: Número de dias úteis (DU) para os quais a taxa será calculada.

        Returns:
            Taxa de juros interpolada pelo método especificado para o número de
            dias úteis fornecido. Retorna float("nan") se o DU for negativo ou
            estiver acima do maior vértice com a extrapolação desabilitada.
        """
        if du < 0:
            return float("nan")

        dus = self._dus
        txs = self._txs

        # Extrapolação na ponta curta sempre retorna a primeira taxa conhecida.
        if du < dus[0]:
            return txs[0]
        # Extrapolação na ponta longa depende da flag de extrapolação.
        if du > dus[-1]:
            return txs[-1] if self._extrapolar else float("nan")

        # Encontra k tal que dus[k-1] < du <= dus[k].
        k = bisect.bisect_left(dus, du)

        # Se du for exatamente um ponto conhecido, retorna a taxa desse ponto.
        if dus[k] == du:
            return txs[k]

        formula = _FORMULAS[self._metodo]
        return formula(du, dus[k - 1], txs[k - 1], dus[k], txs[k])

    def __repr__(self) -> str:
        """Representação textual, usada em terminal ou scripts."""
        return repr(self._df)

    def __len__(self) -> int:
        """Retorna o número de dias úteis conhecidos."""
        return len(self._df)


def interpolar(
    *,
    datas_alvo: pl.Series,
    dus_alvo: pl.Series,
    datas_curva: pl.Series,
    dus_curva: pl.Series,
    taxas_curva: pl.Series,
    metodo: Literal["flat_forward", "linear"],
    extrapolar: bool = False,
) -> pl.Series:
    r"""Interpola taxas em lote, com uma curva por data de referência.

    Cada ponto alvo é interpolado contra a curva da sua data de referência
    (multi-curva, sem loop em Python). Todos os parâmetros são nomeados.

    O resultado pode ser adicionado a um DataFrame via ``with_columns``,
    preservando a ordem original de ``dus_alvo``.

    Args:
        datas_alvo: Series Date com a data de referência de cada ponto
            alvo.
        dus_alvo: Series Int com os dias úteis dos pontos a interpolar
            (mesma ordem de ``datas_alvo``).
        datas_curva: Series Date com a data de referência de cada
            vértice da curva.
        dus_curva: Series Int com os dias úteis dos vértices conhecidos
            (mesma ordem de ``datas_curva``). Vértices com DU negativo são
            ignorados, assim como os de DU nulo ou de taxa nula ou NaN.
        taxas_curva: Series Float com as taxas dos vértices conhecidos
            (mesma ordem de ``dus_curva``).
        metodo: Método de interpolação a usar. Opções: "flat_forward" ou
            "linear".
        extrapolar: Controla apenas o comportamento na ponta longa (DU acima
            do maior vértice de cada grupo). Se True, retorna a última taxa
            conhecida; se False (padrão), retorna ``null``. A ponta curta
            (DU abaixo do menor vértice, inclusive zero) sempre retorna a
            primeira taxa do grupo, independentemente desta flag. O default
            casa com o da classe ``Interpolador``.

    Returns:
        Series Float64 ``taxa_interpolada`` na mesma ordem de
        ``dus_alvo``. ``null`` quando o DU é nulo ou negativo, a data alvo
        não existe na curva, ou o DU está acima do maior vértice e
        ``extrapolar`` é False.

    Raises:
        ValueError: Se ``metodo`` não for ``"flat_forward"`` nem
            ``"linear"``, ou se uma curva tiver o mesmo prazo com taxas
            diferentes na mesma data.
        TypeError: Se ``dus_alvo`` ou ``dus_curva`` não for de tipo inteiro.

    Notes:
        Usa as mesmas fórmulas descritas na documentação de
        ``Interpolador``. Os vértices que cercam cada prazo alvo são
        encontrados automaticamente.

        A função existe para o processamento em lote: interpolar de uma vez
        os pontos de muitas datas de referência, como no reprocessamento de
        uma base histórica inteira, em que criar um ``Interpolador`` por data
        seria muito mais lento. Para uma única curva, ``Interpolador`` é o
        caminho indicado e permite reutilizar a curva em várias consultas.

    Examples:
        Caso típico: adicionar uma coluna de taxas interpoladas a um
        DataFrame com múltiplas datas de referência.

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
    """
    if metodo not in _FORMULAS:
        raise ValueError(f"Método de interpolação '{metodo}' não reconhecido.")
    df_alvo = pl.DataFrame(
        {"grupo": datas_alvo, "du_alvo": _dus_inteiros(dus_alvo, "dus_alvo")}
    )
    df_curva = (
        pl.DataFrame(
            {
                "grupo": datas_curva,
                "dus": _dus_inteiros(dus_curva, "dus_curva"),
                "txs": taxas_curva,
            }
        )
        .with_columns(txs=pl.col("txs").cast(pl.Float64, strict=False))
        .drop_nulls()
        .drop_nans()
        .filter(pl.col("dus") >= 0)
    )
    df_curva = _rejeitar_vertices_conflitantes(df_curva, "dus", "txs", "grupo").sort(
        "grupo", "dus"
    )

    # j = último vértice com du <= du_alvo; k = primeiro com du >= du_alvo.
    # Sem j, o alvo está antes do primeiro vértice; sem k, depois do último.
    # Alvo nulo ou grupo sem curva deixa j e k nulos e resulta em nulo.
    # As duas tabelas estão ordenadas por (grupo, du); com isso podemos
    # desligar check_sortedness, que também silencia o aviso do Polars quando
    # se usa `by=`.
    df = (
        df_alvo.with_row_index("_idx")
        .sort("grupo", "du_alvo")
        .join_asof(
            df_curva.rename({"dus": "du_j", "txs": "tx_j"}),
            by="grupo",
            left_on="du_alvo",
            right_on="du_j",
            strategy="backward",
            check_sortedness=False,
        )
        .join_asof(
            df_curva.rename({"dus": "du_k", "txs": "tx_k"}),
            by="grupo",
            left_on="du_alvo",
            right_on="du_k",
            strategy="forward",
            check_sortedness=False,
        )
    )

    du, du_j, du_k = pl.col("du_alvo"), pl.col("du_j"), pl.col("du_k")
    tx_j, tx_k = pl.col("tx_j"), pl.col("tx_k")
    nulo = pl.lit(None, dtype=pl.Float64)
    taxa = (
        pl.when(du < 0)
        .then(nulo)
        .when(du_j == du)
        .then(tx_j)
        .when(du_j.is_null())
        .then(tx_k)
        .when(du_k.is_null())
        .then(tx_j if extrapolar else nulo)
        .otherwise(_FORMULAS[metodo](du, du_j, tx_j, du_k, tx_k))
    )
    return (
        df.with_columns(taxa_interpolada=taxa)
        .sort("_idx")["taxa_interpolada"]
        .fill_nan(None)
    )
