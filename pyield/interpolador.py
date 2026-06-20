import bisect
import numbers
from typing import Literal

import polars as pl

from pyield._internal.types import ArrayLike


class Interpolador:
    """Classe interpoladora para interpolação de taxas de juros.

    Args:
        dias_uteis: Sequência de dias úteis (DU) conhecidos.
        taxas: Sequência de taxas de juros conhecidas.
        metodo: Método de interpolação a usar. Opções: "flat_forward" ou "linear".
        extrapolar: Controla apenas o comportamento na ponta longa (DU acima
            do maior vértice conhecido). Se True, retorna a última taxa
            conhecida; se False (padrão), retorna NaN. A ponta curta (DU
            abaixo do menor vértice) sempre retorna a primeira taxa conhecida,
            independentemente desta flag.

    Notes:
        - Esta classe usa convenção de 252 dias úteis por ano.
        - Instâncias desta classe são **imutáveis**. Para modificar as
          configurações de interpolação, crie uma nova instância.

    Examples:
        >>> from pyield import Interpolador
        >>> dus = [30, 60, 90]
        >>> txs = [0.045, 0.05, 0.055]

        Interpolação linear:
        >>> linear = Interpolador(dus, txs, "linear")
        >>> linear(45)
        0.0475

        Interpolação flat forward:
        >>> fforward = Interpolador(dus, txs, "flat_forward")
        >>> fforward(45)
        0.04833068080970859

        >>> print(fforward(100))  # Extrapolação desabilitada por padrão
        nan

        >>> print(fforward(-10))  # Entrada inválida retorna NaN
        nan

        Se extrapolação estiver habilitada, a última taxa conhecida é usada:
        >>> fforward_extrap = Interpolador(dus, txs, "flat_forward", extrapolar=True)
        >>> print(fforward_extrap(100))
        0.055
    """

    def __init__(
        self,
        dias_uteis: ArrayLike,
        taxas: ArrayLike,
        metodo: Literal["flat_forward", "linear"],
        extrapolar: bool = False,
    ):
        df = (
            pl.DataFrame({"dus": dias_uteis, "txs": taxas})
            .with_columns(pl.col("dus").cast(pl.Int64))
            .with_columns(pl.col("txs").cast(pl.Float64))
            .drop_nulls()
            .drop_nans()
            .unique(subset="dus", keep="last")
            .sort("dus")
        )
        self._df = df
        self._method = str(metodo)
        self._dus = tuple(df.get_column("dus"))
        self._txs = tuple(df.get_column("txs"))
        self._extrapolate = bool(extrapolar)

    def linear(self, du: int, k: int) -> float:
        """Realiza interpolação de taxa de juros usando o método linear.

        A taxa interpolada é dada pela fórmula:
        y = y1 + (x - x1) * (y2 - y1) / (x2 - x1)

        Onde:
        - (x, y) é o ponto a ser interpolado (du, tx_interpolada).
        - (x1, y1) é o ponto conhecido anterior (du_j, tx_j).
        - (x2, y2) é o próximo ponto conhecido (du_k, tx_k).

        Args:
            du: Número de dias úteis (DU) para os quais a taxa será interpolada.
            k: O índice tal que dus[k-1] < du < dus[k].

        Returns:
            Taxa de juros interpolada em forma decimal.
        """
        # Obtém os pontos imediatamente anterior e posterior ao DU desejado.
        du_j, tx_j = self._dus[k - 1], self._txs[k - 1]
        du_k, tx_k = self._dus[k], self._txs[k]

        return tx_j + (du - du_j) * (tx_k - tx_j) / (du_k - du_j)

    def flat_forward(self, du: int, k: int) -> float:
        r"""Realiza interpolação de taxa de juros usando o método flat forward.

        Este método calcula a taxa de juros interpolada para um dado número de
        dias úteis (``du``) usando a metodologia flat forward, baseada em dois
        pontos conhecidos: o ponto atual (``k``) e o ponto anterior (``j``).

        Assumindo taxas de juros em forma decimal, a taxa interpolada é calculada.
        O tempo é medido em anos baseado em 252 dias úteis por ano.

        Definindo os fatores simples:
        - ``fⱼ = 1 + txⱼ``
        - ``fₖ = 1 + txₖ``

        A taxa interpolada é dada pela fórmula:

        \[
        \left(F_j*\left(\frac{F_k}{F_j}\right)^{f_t}\right)^{\frac{1}{au}}-1
        \]

        Onde os fatores usados na fórmula são definidos como:
        - ``Fⱼ = fⱼ^auⱼ`` é o fator acumulado no ponto ``j``.
        - ``Fₖ = fₖ^auₖ`` é o fator acumulado no ponto ``k``.
        - ``fₜ = (au - auⱼ)/(auₖ - auⱼ)`` é o fator de tempo.

        E as variáveis são definidas como:
        - ``au = du/252`` é o tempo em anos para o ponto interpolado. ``du``
          é o número de dias úteis para o ponto interpolado (entrada deste método).
        - ``k`` é o índice do ponto conhecido atual.
        - ``auₖ = duₖ/252`` é o tempo em anos do ponto ``k``.
        - ``txₖ`` é a taxa de juros (decimal) no ponto ``k``.
        - ``j`` é o índice do ponto conhecido anterior (``k - 1``).
        - ``auⱼ = duⱼ/252`` é o tempo em anos do ponto ``j``.
        - ``txⱼ`` é a taxa de juros (decimal) no ponto ``j``.

        Args:
            du: Número de dias úteis (DU) para os quais a taxa será interpolada.
            k: Índice tal que ``dus[k-1] < du < dus[k]``. Esse ``k``
                corresponde ao próximo vértice conhecido após ``du``.

        Returns:
            Taxa de juros interpolada em forma decimal.
        """
        tx_j = self._txs[k - 1]
        au_j = self._dus[k - 1] / 252
        tx_k = self._txs[k]
        au_k = self._dus[k] / 252
        au = du / 252

        # Siglas: fs = fator simples; fa = fator acumulado; ft = fator de tempo.
        fs_j = 1 + tx_j
        fs_k = 1 + tx_k
        fa_j = fs_j**au_j
        fa_k = fs_k**au_k
        ft = (au - au_j) / (au_k - au_j)
        return (fa_j * (fa_k / fa_j) ** ft) ** (1 / au) - 1

    def interpolar(self, du: int) -> float:
        """Interpola a taxa para um único dia útil.

        Args:
            du: DU escalar inteiro para interpolação.

        Returns:
            Taxa interpolada como float. Retorna ``float("nan")`` quando o DU
            for negativo ou estiver acima do maior vértice conhecido com
            ``extrapolar=False``. DU abaixo do menor vértice sempre retorna
            a primeira taxa conhecida.

        Raises:
            TypeError: Se ``du`` não for um inteiro.
        """
        # Aceita qualquer tipo integral (int, np.int64, etc) e rejeita float/string.
        if not isinstance(du, numbers.Integral):
            raise TypeError("du deve ser int. Use interpolar_expr para coluna Polars.")
        return self._taxa_interpolada(int(du))

    def _interpolar_serie(self, du: ArrayLike) -> pl.Series:
        """Interpola taxas para uma sequência de dias úteis.

        Helper interno usado por ``interpolar_expr`` via ``map_batches``.
        A API pública para uso vetorizado é ``interpolar_expr`` (composição
        com expressões Polars) ou a função top-level ``interpolar`` (curva
        única ou multi-curva sem Polars).

        Args:
            du: Sequência de DUs (lista, tupla, ``pl.Series``, ``np.ndarray``,
                etc.) com os pontos a interpolar.

        Returns:
            ``pl.Series`` Float64 chamada ``taxa_interpolada`` na mesma ordem
            da entrada. ``null`` quando o DU for nulo, negativo, ou estiver
            acima do maior vértice conhecido com ``extrapolar=False``.
        """
        s_dus = pl.Series(name="taxa_interpolada", values=du, dtype=pl.Int64)
        return s_dus.map_elements(
            self._taxa_interpolada, return_dtype=pl.Float64
        ).fill_nan(None)

    def interpolar_expr(self, du: str | pl.Expr) -> pl.Expr:
        """Cria expressão Polars que interpola taxas para uma coluna de DU.

        Útil para adicionar uma coluna de taxas interpoladas a um DataFrame via
        ``with_columns``, compondo livremente com outras expressões Polars (ex.:
        cálculo de inflação implícita na mesma chamada). A curva e o método são
        os configurados na instância.

        Args:
            du: Nome de coluna ou expressão Polars com os DUs alvo. A coluna
                deve ser inteira (será convertida para Int64 internamente).

        Returns:
            pl.Expr: Expressão Float64 sem alias com as taxas interpoladas.
                ``null`` quando o DU for nulo, negativo, ou estiver acima do
                maior vértice conhecido com ``extrapolar=False``.

        Examples:
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
        """
        expr = pl.col(du) if isinstance(du, str) else du
        return expr.map_batches(self._interpolar_serie, return_dtype=pl.Float64)

    def _taxa_interpolada(self, du: int) -> float:
        """Encontra o ponto de interpolação apropriado e retorna a taxa de juros.

        A taxa é interpolada pelo método especificado a partir desse ponto.

        Args:
            du: Número de dias úteis (DU) para os quais a taxa será calculada.

        Returns:
            Taxa de juros interpolada pelo método especificado para o número de
            dias úteis fornecido. Se a entrada estiver fora do intervalo e
            extrapolação estiver desabilitada, retorna float("nan").
        """
        # Validação de entrada.
        if not isinstance(du, int) or du < 0:
            return float("nan")

        # Referências locais para facilitar legibilidade.
        dus = self._dus
        txs = self._txs
        extrapolate = self._extrapolate
        method = self._method

        # Extrapolação na ponta curta sempre retorna a primeira taxa conhecida.
        if du < dus[0]:
            return txs[0]
        # Extrapolação na ponta longa depende da flag de extrapolação.
        elif du > dus[-1]:
            return txs[-1] if extrapolate else float("nan")

        # Encontra k tal que dus[k-1] < du < dus[k].
        k = bisect.bisect_left(dus, du)

        # Se du for exatamente um ponto conhecido, retorna a taxa desse ponto.
        if k < len(dus) and dus[k] == du:
            return txs[k]

        if method == "linear":
            return self.linear(du, k)
        elif method == "flat_forward":
            return self.flat_forward(du, k)

        raise ValueError(f"Método de interpolação '{method}' não reconhecido.")

    def __call__(self, du: int) -> float:
        """Atalho escalar para ``interpolar``.

        Para interpolação vetorizada, use ``interpolar_expr`` (em pipelines
        Polars) ou a função top-level ``pyield.interpolar`` (curva única ou
        multi-curva sem Polars).

        Args:
            du: DU escalar inteiro.

        Returns:
            Taxa interpolada como float.
        """
        return self.interpolar(du)

    def __repr__(self) -> str:
        """Representação textual, usada em terminal ou scripts."""
        return repr(self._df)

    def __len__(self) -> int:
        """Retorna o número de dias úteis conhecidos."""
        return len(self._df)


def interpolar(  # noqa: PLR0913
    dus_alvo: pl.Series,
    dus_curva: pl.Series,
    taxas_curva: pl.Series,
    *,
    datas_alvo: pl.Series | None = None,
    datas_curva: pl.Series | None = None,
    extrapolar: bool = False,
) -> pl.Series:
    r"""Interpola taxas flat-forward para uma série de pontos alvo.

    Versão vetorizada de :class:`Interpolador`. Quando ``datas_alvo`` e
    ``datas_curva`` são fornecidas, cada ponto é interpolado contra a
    curva da sua data correspondente (multi-curva, sem loop em Python).
    Quando ambas são ``None``, todos os pontos usam a mesma curva.

    O resultado pode ser adicionado a um DataFrame via ``with_columns``,
    preservando a ordem original de ``dus_alvo``.

    Args:
        dus_alvo: Series Int com os dias úteis dos pontos a interpolar.
        dus_curva: Series Int com os dias úteis dos vértices conhecidos.
        taxas_curva: Series Float com as taxas dos vértices conhecidos
            (mesma ordem de ``dus_curva``).
        datas_alvo: Series Date com a data de referência de cada ponto
            alvo. Quando fornecida, ``datas_curva`` também deve ser.
            Padrão: ``None`` (curva única).
        datas_curva: Series Date com a data de referência de cada
            vértice da curva. Padrão: ``None`` (curva única).
        extrapolar: Controla apenas o comportamento na ponta longa (DU acima
            do maior vértice de cada grupo). Se True, retorna a última taxa
            conhecida; se False (padrão), retorna ``null``. A ponta curta
            (DU abaixo do menor vértice) sempre retorna a primeira taxa do
            grupo, independentemente desta flag. O default casa com o da
            classe :class:`Interpolador`.

    Returns:
        Series Float64 ``taxa_interpolada`` na mesma ordem de
        ``dus_alvo``. ``null`` quando o DU é nulo, a data alvo não existe
        na curva, ou o ponto está fora do intervalo e ``extrapolar`` é
        False.

    Raises:
        ValueError: Se apenas uma de ``datas_alvo`` ou ``datas_curva``
            for fornecida.

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

        Curva única (sem datas):

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
    """
    if (datas_alvo is None) != (datas_curva is None):
        raise ValueError(
            "datas_alvo e datas_curva devem ser fornecidas juntas ou omitidas juntas."
        )

    if datas_alvo is not None and datas_curva is not None:
        df_alvo = pl.DataFrame({"grupo": datas_alvo, "du_alvo": dus_alvo})
        df_curva = pl.DataFrame(
            {"grupo": datas_curva, "du": dus_curva, "tx": taxas_curva}
        )
    else:
        df_alvo = pl.DataFrame({"du_alvo": dus_alvo}).with_columns(
            grupo=pl.lit(0, dtype=pl.Int32)
        )
        df_curva = pl.DataFrame({"du": dus_curva, "tx": taxas_curva}).with_columns(
            grupo=pl.lit(0, dtype=pl.Int32)
        )

    df_alvo = df_alvo.with_columns(
        du_alvo=pl.col("du_alvo").cast(pl.Int64, strict=False),
    ).with_row_index("_idx")

    df_curva = (
        df_curva.with_columns(
            du=pl.col("du").cast(pl.Int64, strict=False),
            tx=pl.col("tx").cast(pl.Float64, strict=False),
        )
        .drop_nulls()
        .drop_nans()
        .unique(subset=["grupo", "du"], keep="last")
        .sort("grupo", "du")
    )

    nulo = pl.lit(None, dtype=pl.Float64)
    if df_curva.is_empty():
        return df_alvo.sort("_idx").select(taxa_interpolada=nulo)["taxa_interpolada"]

    # Vértices das pontas por grupo (em ordem de DU, não de chegada).
    df_extremos = df_curva.group_by("grupo").agg(
        du_min=pl.col("du").min(),
        du_max=pl.col("du").max(),
        tx_min=pl.col("tx").sort_by("du").first(),
        tx_max=pl.col("tx").sort_by("du").last(),
    )

    # j = último vértice com du <= du_alvo; k = primeiro com du >= du_alvo.
    # df_alvo é ordenado por (grupo, du_alvo) e df_curva já vem ordenado por
    # (grupo, du); com isso podemos desligar check_sortedness, que também
    # silencia o aviso do Polars quando se usa `by=`.
    df = (
        df_alvo.sort("grupo", "du_alvo")
        .join_asof(
            df_curva.rename({"du": "du_j", "tx": "tx_j"}),
            by="grupo",
            left_on="du_alvo",
            right_on="du_j",
            strategy="backward",
            check_sortedness=False,
        )
        .join_asof(
            df_curva.rename({"du": "du_k", "tx": "tx_k"}),
            by="grupo",
            left_on="du_alvo",
            right_on="du_k",
            strategy="forward",
            check_sortedness=False,
        )
        .join(df_extremos, on="grupo", how="left")
    )

    # Flat-forward: tx = (fⱼ^auⱼ * (fₖ^auₖ / fⱼ^auⱼ)^ft)^(1/au) - 1
    au = pl.col("du_alvo") / 252
    au_j = pl.col("du_j") / 252
    au_k = pl.col("du_k") / 252
    fa_j = (1 + pl.col("tx_j")).pow(au_j)
    fa_k = (1 + pl.col("tx_k")).pow(au_k)
    ft = (au - au_j) / (au_k - au_j)
    expr_meio = (fa_j * (fa_k / fa_j).pow(ft)).pow(1 / au) - 1

    taxa = (
        pl.when(pl.col("du_alvo").is_null() | pl.col("du_min").is_null())
        .then(nulo)
        .when(pl.col("du_j") == pl.col("du_alvo"))
        .then(pl.col("tx_j"))
        .when(pl.col("du_alvo") < pl.col("du_min"))
        .then(pl.col("tx_min"))
        .when(pl.col("du_alvo") > pl.col("du_max"))
        .then(pl.col("tx_max") if extrapolar else nulo)
        .otherwise(expr_meio)
    )

    return (
        df.with_columns(taxa_interpolada=taxa)
        .sort("_idx")["taxa_interpolada"]
        .fill_nan(None)
    )
