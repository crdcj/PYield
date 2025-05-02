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
        >>> linear = Interpolator("linear", known_bdays, known_rates)
        >>> linear(45)
        0.0475

        Flat forward interpolation example:
        >>> fforward = Interpolator("flat_forward", known_bdays, known_rates)
        >>> fforward(45)
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
        self._method = str(method)
        self._known_bdays = tuple(df["bday"])
        self._known_rates = tuple(df["rate"])
        self._extrapolate = bool(extrapolate)

    def flat_forward(self, bday: int, k: int) -> float:
        r"""
        Performs the interest rate interpolation using the flat forward method.

        This method calculates the interpolated interest rate for a given
        number of business days (`bday`) using the flat forward methodology,
        based on two known points: the current point (`k`) and the previous point (`j`).

        Assuming interest rates are in decimal form, the interpolated rate
        is calculated. Time is measured in years based on a 252-business-day year.

        The interpolated rate is given by the formula:

        $$
        \left(f_j*\left(\frac{f_k}{f_j}\right)^{f_t}\right)^{\frac{1}{time}}-1
        $$

        Where the factors used in the formula are defined as:

        * `fⱼ = (1 + rateⱼ)^timeⱼ` is the compounding factor at point `j`.
        * `fₖ = (1 + rateₖ)^timeₖ` is the compounding factor at point `k`.
        * `fₜ = (time - timeⱼ)/(timeₖ - timeⱼ)` is the time factor.

        And the variables are defined as:

        * `time = bday/252` is the time in years for the interpolated point. `bday` is
         the number of business days for the interpolated point (input to this method).
        * `k` is the index of the current known point.
        * `timeₖ = bdayₖ/252` is the time in years of point `k`.
        * `rateₖ` is the interest rate (decimal) at point `k`.
        * `j` is the index of the previous known point (`k - 1`).
        * `timeⱼ = bdayⱼ/252` is the time in years of point `j`.
        * `rateⱼ` is the interest rate (decimal) at point `j`.

        Args:
            bday (int): Number of bus. days for which the rate is to be interpolated.
            k (int): The index in the known_bdays and known_rates arrays such that
                     known_bdays[k-1] < bday < known_bdays[k]. This `k` corresponds
                     to the index of the next known point after `bday`.

        Returns:
            float: The interpolated interest rate in decimal form.
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
        # Create local references to facilitate code readability
        known_bdays = self._known_bdays
        known_rates = self._known_rates
        extrapolate = self._extrapolate
        method = self._method

        # Lower bound extrapolation is always the first known rate
        if bday < known_bdays[0]:
            return float(known_rates[0])
        # Upper bound extrapolation depends on the extrapolate flag
        elif bday > known_bdays[-1]:
            return float(known_rates[-1]) if extrapolate else float("NaN")

        # Early return for linear interpolation
        if method == "linear":
            return float(np.interp(bday, known_bdays, known_rates))

        # Find k such that known_bdays[k-1] < bday < known_bdays[k]
        k = bisect.bisect_left(known_bdays, bday)

        # Check if the interpolation point is known
        if k < len(known_bdays) and known_bdays[k] == bday:
            return float(known_rates[k])

        # Perform flat forward interpolation
        return float(self.flat_forward(bday, k))

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
        """Textual representation, used in terminal or scripts."""
        return repr(self._df)

    def __len__(self) -> int:
        """Returns the number of known business days."""
        return len(self._df)
