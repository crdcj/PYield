import bisect
import numbers
from typing import Literal, overload

import polars as pl

from pyield.types import ArrayLike, is_array_like


class Interpolator:
    """Classe interpoladora para interpolação de taxas de juros.

    Args:
        method: Método de interpolação a usar. Opções: "flat_forward" ou "linear".
        known_bdays: Sequência de dias úteis conhecidos.
        known_rates: Sequência de taxas de juros conhecidas.
        extrapolate: Se True, extrapola além dos dias úteis conhecidos usando a
            última taxa disponível. Padrão: False, retornando NaN para valores
            fora do intervalo.

    Raises:
        ValueError: Se known_bdays e known_rates não tiverem o mesmo tamanho.
        ValueError: Se o método de interpolação não for reconhecido.

    Notes:
        - Esta classe usa convenção de 252 dias úteis por ano.
        - Instâncias desta classe são **imutáveis**. Para modificar as
          configurações de interpolação, crie uma nova instância.

    Examples:
        >>> from pyield import Interpolator
        >>> known_bdays = [30, 60, 90]
        >>> known_rates = [0.045, 0.05, 0.055]

        Interpolação linear:
        >>> linear = Interpolator("linear", known_bdays, known_rates)
        >>> linear(45)
        0.0475

        Interpolação flat forward:
        >>> fforward = Interpolator("flat_forward", known_bdays, known_rates)
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
        >>> fforward_extrap = Interpolator(
        ...     "flat_forward", known_bdays, known_rates, extrapolate=True
        ... )
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
            pl.DataFrame({"bday": known_bdays, "rate": known_rates})
            .with_columns(pl.col("bday").cast(pl.Int64))
            .with_columns(pl.col("rate").cast(pl.Float64))
            .drop_nulls()
            .drop_nans()
            .unique(subset="bday", keep="last")
            .sort("bday")
        )
        self._df = df
        self._method = str(method)
        self._known_bdays = tuple(df.get_column("bday"))
        self._known_rates = tuple(df.get_column("rate"))
        self._extrapolate = bool(extrapolate)

    def linear(self, bday: int, k: int) -> float:
        """Realiza interpolação de taxa de juros usando o método linear.

        A taxa interpolada é dada pela fórmula:
        y = y1 + (x - x1) * (y2 - y1) / (x2 - x1)

        Onde:
        - (x, y) é o ponto a ser interpolado (bday, taxa_interpolada).
        - (x1, y1) é o ponto conhecido anterior (bday_j, rate_j).
        - (x2, y2) é o próximo ponto conhecido (bday_k, rate_k).

        Args:
            bday: Número de dias úteis para os quais a taxa será interpolada.
            k: O índice tal que known_bdays[k-1] < bday < known_bdays[k].

        Returns:
            Taxa de juros interpolada em forma decimal.
        """
        # Get the bracketing points for interpolation
        bday_j, rate_j = self._known_bdays[k - 1], self._known_rates[k - 1]
        bday_k, rate_k = self._known_bdays[k], self._known_rates[k]

        # Perform linear interpolation
        return rate_j + (bday - bday_j) * (rate_k - rate_j) / (bday_k - bday_j)

    def flat_forward(self, bday: int, k: int) -> float:
        r"""Realiza interpolação de taxa de juros usando o método flat forward.

        Este método calcula a taxa de juros interpolada para um dado número de
        dias úteis (``bday``) usando a metodologia flat forward, baseada em dois
        pontos conhecidos: o ponto atual (``k``) e o ponto anterior (``j``).

        Assumindo taxas de juros em forma decimal, a taxa interpolada é calculada.
        O tempo é medido em anos baseado em 252 dias úteis por ano.

        A taxa interpolada é dada pela fórmula:

        $$
        \left(f_j*\left(\frac{f_k}{f_j}\right)^{f_t}\right)^{\frac{1}{time}}-1
        $$

        Onde os fatores usados na fórmula são definidos como:

        * ``fⱼ = (1 + rateⱼ)^timeⱼ`` é o fator de composição no ponto ``j``.
        * ``fₖ = (1 + rateₖ)^timeₖ`` é o fator de composição no ponto ``k``.
        * ``fₜ = (time - timeⱼ)/(timeₖ - timeⱼ)`` é o fator de tempo.

        E as variáveis são definidas como:

        * ``time = bday/252`` é o tempo em anos para o ponto interpolado. ``bday``
          é o número de dias úteis para o ponto interpolado (entrada deste método).
        * ``k`` é o índice do ponto conhecido atual.
        * ``timeₖ = bdayₖ/252`` é o tempo em anos do ponto ``k``.
        * ``rateₖ`` é a taxa de juros (decimal) no ponto ``k``.
        * ``j`` é o índice do ponto conhecido anterior (``k - 1``).
        * ``timeⱼ = bdayⱼ/252`` é o tempo em anos do ponto ``j``.
        * ``rateⱼ`` é a taxa de juros (decimal) no ponto ``j``.

        Args:
            bday: Número de dias úteis para os quais a taxa será interpolada.
            k: O índice nos arrays known_bdays e known_rates tal que
                known_bdays[k-1] < bday < known_bdays[k]. Este ``k`` corresponde
                ao índice do próximo ponto conhecido após ``bday``.

        Returns:
            Taxa de juros interpolada em forma decimal.
        """
        rate_j = self._known_rates[k - 1]
        time_j = self._known_bdays[k - 1] / 252
        rate_k = self._known_rates[k]
        time_k = self._known_bdays[k] / 252
        time = bday / 252

        # Perform flat forward interpolation
        f_j = (1 + rate_j) ** time_j
        f_k = (1 + rate_k) ** time_k
        f_t = (time - time_j) / (time_k - time_j)
        return (f_j * (f_k / f_j) ** f_t) ** (1 / time) - 1

    def interpolate(self, bdays: int | ArrayLike) -> float | pl.Series:
        """Interpola taxas para dia(s) útil(eis) fornecido(s).

        Args:
            bdays: Dia(s) útil(eis) para interpolação. Aceita int ou ArrayLike.

        Returns:
            Taxa(s) interpolada(s). Float para entrada escalar, pl.Series para array.
        """
        if is_array_like(bdays):
            s_bdays = pl.Series(name="interpolated_rate", values=bdays, dtype=pl.Int64)
            result = s_bdays.map_elements(
                self._interpolated_rate, return_dtype=pl.Float64
            )
            return result.fill_nan(None)

        # Aceita QUALQUER coisa que se comporte como inteiro (int, np.int64, etc)
        # Mas REJEITA floats (30.5) e Strings
        elif isinstance(bdays, numbers.Integral):
            return self._interpolated_rate(int(bdays))

        else:
            raise TypeError("bdays must be an int or an array-like structure.")

    def _interpolated_rate(self, bday: int) -> float:
        """Encontra o ponto de interpolação apropriado e retorna a taxa de juros.

        A taxa é interpolada pelo método especificado a partir desse ponto.

        Args:
            bday: Número de dias úteis para os quais a taxa de juros será calculada.

        Returns:
            Taxa de juros interpolada pelo método especificado para o número de
            dias úteis fornecido. Se a entrada estiver fora do intervalo e
            extrapolação estiver desabilitada, retorna float("nan").
        """
        # Validate input
        if not isinstance(bday, int) or bday < 0:
            return float("nan")

        # Create local references to facilitate code readability
        known_bdays = self._known_bdays
        known_rates = self._known_rates
        extrapolate = self._extrapolate
        method = self._method

        # Lower bound extrapolation is always the first known rate
        if bday < known_bdays[0]:
            return known_rates[0]
        # Upper bound extrapolation depends on the extrapolate flag
        elif bday > known_bdays[-1]:
            return known_rates[-1] if extrapolate else float("nan")

        # Find k such that known_bdays[k-1] < bday < known_bdays[k]
        k = bisect.bisect_left(known_bdays, bday)

        # If bday is one of the known points, return its rate directly
        if k < len(known_bdays) and known_bdays[k] == bday:
            return known_rates[k]

        if method == "linear":
            return self.linear(bday, k)
        elif method == "flat_forward":
            return self.flat_forward(bday, k)

        raise ValueError(f"Interpolation method '{method}' not recognized.")

    @overload
    def __call__(self, bday: int) -> float: ...
    @overload
    def __call__(self, bday: ArrayLike) -> pl.Series: ...
    def __call__(self, bday: int | ArrayLike) -> float | pl.Series:
        """Permite que a instância seja chamada como função para realizar interpolação.

        Args:
            bday: Número de dias úteis para os quais a taxa de juros será calculada.

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
