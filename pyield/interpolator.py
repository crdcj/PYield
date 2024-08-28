import bisect
from typing import Literal

import numpy as np
import pandas as pd


class Interpolator:
    def __init__(
        self,
        method: Literal["flat_forward", "linear"],
        known_bdays: pd.Series | list,
        known_rates: pd.Series | list,
        extrapolate: bool = True,
    ):
        """
        Initialize the Interpolator with given atributes.

        Args:
            method (Literal["flat_forward", "linear"]): Interpolation method.
            known_bdays (pd.Series | pd.Index | list): Series of known business days.
            known_rates (pd.Series | pd.Index | list): Series of known interest rates.
            extrapolate (bool, optional): Whether to extrapolate beyond the known data.

        Raises:
            ValueError: If known_bdays and known_rates do not have the same length.
            ValueError: If the interpolation method is not recognized

        Returns:
            Interpolator: An instance of the Interpolator

        Note:
            This class uses a 252 business days per year convention.
        Examples:
            >>> known_bdays = [30, 60, 90]
            >>> known_rates = [0.045, 0.05, 0.055]
            >>> lin_interp = Interpolator("linear", known_bdays, known_rates)
            >>> lin_interp(45)
            0.0475
            >>> ffo_interp = Interpolator("flat_forward", known_bdays, known_rates)
            >>> ffo_interp(45)
            0.04833068080970859

        """
        self.method = method
        self.known_bdays = known_bdays
        self.known_rates = known_rates
        self.extrapolate = extrapolate
        self._set_known_bdays_and_rates()

    def _set_known_bdays_and_rates(self) -> None:
        """Validate and process the inputs of the Interpolator."""
        if self.method not in {"flat_forward", "linear"}:
            raise ValueError(f"Unknown interpolation method: {self.method}.")

        known_bdays = self.known_bdays
        known_rates = self.known_rates
        if isinstance(known_bdays, pd.Series):
            known_bdays = known_bdays.to_list()
        if isinstance(known_rates, pd.Series):
            known_rates = known_rates.to_list()

        if len(known_bdays) != len(known_rates):
            raise ValueError("known_bdays and known_rates must have the same length.")

        df = pd.DataFrame({"bday": known_bdays, "rate": known_rates})
        df = df.dropna().drop_duplicates(subset="bday").sort_values("bday")

        self._known_bdays = df["bday"].to_list()
        self._known_rates = df["rate"].to_list()

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

    def _linear(self, bday: int) -> float:
        """Performs linear interpolation."""
        np_float = np.interp(bday, self._known_bdays, self._known_rates)
        return float(np_float)

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
        # Check for edge cases
        if bday < self._known_bdays[0]:
            return self._known_rates[0]
        elif bday in self._known_bdays:
            return self._known_rates[self._known_bdays.index(bday)]
        elif bday > self._known_bdays[-1]:
            if self.extrapolate:
                return self._known_rates[-1]
            else:
                return float("NaN")

        if self.method == "flat_forward":
            return self._flat_forward(bday)
        elif self.method == "linear":
            return self._linear(bday)

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
