import bisect
from typing import Literal

import numpy as np
import pandas as pd


class Interpolator:
    """
    Interpolator class for interest rate interpolation.

    Args:
        method (Literal["flat_forward", "linear"]): The interpolation method to use.
        known_bdays (pd.Series | list[int]): The known business days sequence.
        known_rates (pd.Series | list[float]): The known interest rates sequence.
        extrapolate (bool, optional): If True, extrapolates beyond known business days
            using the last available rate. Defaults to False, returning NaN for
            out-of-range values.

    Raises:
        ValueError: If known_bdays and known_rates do not have the same length.
        ValueError: If the interpolation method is not recognized

    Note:
        - This class uses a 252 business days per year convention.
        - Instances of this class are **immutable**. To modify the interpolation
          settings, create a new instance.

    Examples:
        >>> from pyield import Interpolator
        >>> known_bdays = [30, 60, 90]
        >>> known_rates = [0.045, 0.05, 0.055]

        Linear interpolation example:
        >>> lin_interp = Interpolator("linear", known_bdays, known_rates)
        >>> lin_interp(45)
        0.0475

        Flat forward interpolation example:
        >>> ffo_interp = Interpolator("flat_forward", known_bdays, known_rates)
        >>> ffo_interp(45)
        0.04833068080970859
    """

    def __init__(
        self,
        method: Literal["flat_forward", "linear"],
        known_bdays: pd.Series | np.ndarray | tuple[int] | list[int],
        known_rates: pd.Series | np.ndarray | tuple[float] | list[float],
        extrapolate: bool = False,
    ):
        df = (
            pd.DataFrame({"bday": known_bdays, "rate": known_rates})
            .dropna()
            .drop_duplicates(subset="bday")
            .sort_values("bday", ignore_index=True)
        )
        self._df = df
        self._method = method
        self._known_bdays = tuple(df["bday"])
        self._known_rates = tuple(df["rate"])
        self._extrapolate = bool(extrapolate)

    def _flat_forward(self, bday: int) -> float:
        """Performs the interest rate interpolation using the flat forward method."""

        # Find i such that known_bdays[i-1] < bday < known_bdays[i]
        i = bisect.bisect_left(self._known_bdays, bday)

        # Get previous and next known rates and business days
        prev_rate = self._known_rates[i - 1]
        prev_bday = self._known_bdays[i - 1]
        next_rate = self._known_rates[i]
        next_bday = self._known_bdays[i]

        # Perform flat forward interpolation
        a = (1 + prev_rate) ** (prev_bday / 252)
        b = (1 + next_rate) ** (next_bday / 252)
        c = (bday - prev_bday) / (next_bday - prev_bday)
        return (a * (b / a) ** c) ** (252 / bday) - 1

    def interpolate(self, bday: int) -> float:
        """
        Finds the appropriate interpolation point and returns the interest rate
        interpolated by the specified method from that point.

        Args:
            bday (int): Number of business days for which the interest rate is to be
                calculated.

        Returns:
            float: The interest rate interpolated by the specified method for the given
                number of business days.
        """
        # Check for cases where interpolation is not needed
        if bday < self._known_bdays[0]:
            return self._known_rates[0]
        elif bday in self._known_bdays:
            return self._known_rates[self._known_bdays.index(bday)]
        elif bday > self._known_bdays[-1]:
            return self._known_rates[-1] if self._extrapolate else float("NaN")

        if self._method == "flat_forward":
            return self._flat_forward(bday)
        elif self._method == "linear":
            return float(np.interp(bday, self._known_bdays, self._known_rates))
        else:
            raise ValueError(f"Unknown interpolation method: {self._method}.")

    def __call__(self, bday: int) -> float:
        """
        Allows the instance to be called as a function to perform interpolation.

        Args:
            bday (int): Number of business days for which the interest rate is to be
                calculated.

        Returns:
            float: The interest rate interpolated by the specified method for the given
                number of business days.
        """
        return self.interpolate(bday)

    def __repr__(self) -> str:
        """
        Textual representation, used in terminal or scripts.
        """

        return repr(self._df)
