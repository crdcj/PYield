import bisect
import numbers
from typing import Literal, overload

import polars as pl

from pyield._internal.types import ArrayLike, is_collection


class Interpolator:
    """Classe interpoladora para interpolação de taxas de juros.

    Args:
        method: Método de interpolação a usar. Opções: "flat_forward" ou "linear".
        known_bdays: Sequência de dias úteis (DU) conhecidos.
        known_rates: Sequência de taxas de juros conhecidas.
        extrapolate: Se True, extrapola além dos dias úteis conhecidos usando a
            última taxa disponível. Padrão: False, retornando NaN para valores
            fora do intervalo.

    Raises:
        ValueError: Se known_bdays e known_rates não tiverem o mesmo tamanho.
        ValueError: Se o método de interpolação não for reconhecido.

    Notes:
        - Esta classe usa convenção de 252 dias úteis por ano.
        - Na API pública, os parâmetros mantêm o nome ``bday``/``bdays`` por
          compatibilidade, mas o conceito de negócio é DU (dias úteis).
        - Instâncias desta classe são **imutáveis**. Para modificar as
          configurações de interpolação, crie uma nova instância.

    Examples:
        >>> from pyield import Interpolator
        >>> dus = [30, 60, 90]
        >>> txs = [0.045, 0.05, 0.055]

        Interpolação linear:
        >>> linear = Interpolator("linear", dus, txs)
        >>> linear(45)
        0.0475

        Interpolação flat forward:
        >>> fforward = Interpolator("flat_forward", dus, txs)
        >>> fforward(45)
        0.04833068080970859

        Interpolação de array (polars mostra 6 casas decimais por padrão):
        >>> fforward([15, 45, 75, 100])
        shape: (4,)
        Series: 'interpolated_rate' [f64]
        [
            0.045
            0.048331
            0.052997
            null
        ]

        >>> print(fforward(100))  # Extrapolação desabilitada por padrão
        nan

        >>> print(fforward(-10))  # Entrada inválida retorna NaN
        nan

        Se extrapolação estiver habilitada, a última taxa conhecida é usada:
        >>> fforward_extrap = Interpolator("flat_forward", dus, txs, extrapolate=True)
        >>> print(fforward_extrap(100))
        0.055
    """

    def __init__(
        self,
        method: Literal["flat_forward", "linear"],
        known_bdays: ArrayLike,
        known_rates: ArrayLike,
        extrapolate: bool = False,
    ):
        df = (
            pl.DataFrame({"dus": known_bdays, "txs": known_rates})
            .with_columns(pl.col("dus").cast(pl.Int64))
            .with_columns(pl.col("txs").cast(pl.Float64))
            .drop_nulls()
            .drop_nans()
            .unique(subset="dus", keep="last")
            .sort("dus")
        )
        self._df = df
        self._method = str(method)
        self._dus = tuple(df.get_column("dus"))
        self._txs = tuple(df.get_column("txs"))
        self._extrapolate = bool(extrapolate)

    def linear(self, bday: int, k: int) -> float:
        """Realiza interpolação de taxa de juros usando o método linear.

        A taxa interpolada é dada pela fórmula:
        y = y1 + (x - x1) * (y2 - y1) / (x2 - x1)

        Onde:
        - (x, y) é o ponto a ser interpolado (du, tx_interpolada).
        - (x1, y1) é o ponto conhecido anterior (du_j, tx_j).
        - (x2, y2) é o próximo ponto conhecido (du_k, tx_k).

        Args:
            bday: Número de dias úteis (DU) para os quais a taxa será interpolada.
            k: O índice tal que dus[k-1] < bday < dus[k].

        Returns:
            Taxa de juros interpolada em forma decimal.
        """
        du = bday
        # Obtém os pontos imediatamente anterior e posterior ao DU desejado.
        du_j, tx_j = self._dus[k - 1], self._txs[k - 1]
        du_k, tx_k = self._dus[k], self._txs[k]

        return tx_j + (du - du_j) * (tx_k - tx_j) / (du_k - du_j)

    def flat_forward(self, bday: int, k: int) -> float:
        r"""Realiza interpolação de taxa de juros usando o método flat forward.

        Este método calcula a taxa de juros interpolada para um dado número de
        dias úteis (``bday``) usando a metodologia flat forward, baseada em dois
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
        - ``au = du/252`` é o tempo em anos para o ponto interpolado. ``bday``
          é o número de dias úteis para o ponto interpolado (entrada deste método).
        - ``k`` é o índice do ponto conhecido atual.
        - ``auₖ = duₖ/252`` é o tempo em anos do ponto ``k``.
        - ``txₖ`` é a taxa de juros (decimal) no ponto ``k``.
        - ``j`` é o índice do ponto conhecido anterior (``k - 1``).
        - ``auⱼ = duⱼ/252`` é o tempo em anos do ponto ``j``.
        - ``txⱼ`` é a taxa de juros (decimal) no ponto ``j``.

        Args:
            bday: Número de dias úteis (DU) para os quais a taxa será interpolada.
            k: Índice tal que ``dus[k-1] < bday < dus[k]``. Esse ``k``
                corresponde ao próximo vértice conhecido após ``bday``.

        Returns:
            Taxa de juros interpolada em forma decimal.
        """
        du = bday
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

    def interpolate(self, bdays: int | ArrayLike) -> float | pl.Series:
        """Interpola taxas para dia(s) útil(eis) fornecido(s).

        Args:
            bdays: DU(s) para interpolação. Aceita int ou ArrayLike.

        Returns:
            Taxa(s) interpolada(s). Float para entrada escalar, pl.Series para array.
        """
        if is_collection(bdays):
            s_dus = pl.Series(name="interpolated_rate", values=bdays, dtype=pl.Int64)
            result = s_dus.map_elements(
                self._interpolated_rate, return_dtype=pl.Float64
            )
            return result.fill_nan(None)

        # Aceita qualquer tipo integral (int, np.int64, etc) e rejeita float/string.
        elif isinstance(bdays, numbers.Integral):
            return self._interpolated_rate(int(bdays))

        else:
            raise TypeError("bdays deve ser int ou uma estrutura array-like.")

    def _interpolated_rate(self, bday: int) -> float:
        """Encontra o ponto de interpolação apropriado e retorna a taxa de juros.

        A taxa é interpolada pelo método especificado a partir desse ponto.

        Args:
            bday: Número de dias úteis (DU) para os quais a taxa será calculada.

        Returns:
            Taxa de juros interpolada pelo método especificado para o número de
            dias úteis fornecido. Se a entrada estiver fora do intervalo e
            extrapolação estiver desabilitada, retorna float("nan").
        """
        du = bday

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

    @overload
    def __call__(self, bday: int) -> float: ...
    @overload
    def __call__(self, bday: ArrayLike) -> pl.Series: ...
    def __call__(self, bday: int | ArrayLike) -> float | pl.Series:
        """Permite que a instância seja chamada como função para realizar interpolação.

        Args:
            bday: Número de dias úteis (DU) para os quais a taxa será calculada.

        Returns:
            Taxa de juros interpolada pelo método especificado para o número de
            dias úteis fornecido. Se a entrada estiver fora do intervalo e
            extrapolação estiver desabilitada, retorna float("nan").
        """
        return self.interpolate(bday)

    def __repr__(self) -> str:
        """Representação textual, usada em terminal ou scripts."""
        return repr(self._df)

    def __len__(self) -> int:
        """Retorna o número de dias úteis conhecidos."""
        return len(self._df)
