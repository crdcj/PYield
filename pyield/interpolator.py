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
    ):
        """
        Initialize the Interpolator with given atributes.

        Args:
            method (Literal["flat_forward", "linear"]): Interpolation method.
            known_bdays (pd.Series | list): Series of known business days.
            known_rates (pd.Series | list): Series of known interest rates.

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
            >>> interpolator = Interpolator("linear", known_bdays, known_rates)
        """
        self.method = method
        self.known_bdays = known_bdays
        self.known_rates = known_rates
        self._validate_and_process_inputs()

    def _validate_and_process_inputs(self) -> None:
        """Validate and process the inputs of the Interpolator."""
        if self.method not in {"flat_forward", "linear"}:
            raise ValueError(f"Unknown interpolation method: {self.method}.")

        if len(self.known_bdays) != len(self.known_rates):
            raise ValueError("known_bdays and known_rates must have the same length.")

        df = pd.DataFrame({"bday": self.known_bdays, "rate": self.known_rates})
        df = df.dropna().drop_duplicates(subset="bday").sort_values("bday")

        self.known_bdays = df["bday"].to_list()
        self.known_rates = df["rate"].to_list()

    def _flat_forward(self, bday: int) -> float:
        """Performs the interest rate interpolation using the flat forward method."""

        # Find i such that known_bdays[i-1] < bday < known_bdays[i]
        i = bisect.bisect_left(self.known_bdays, bday)

        # Get previous and next known rates and business days
        prev_rate = self.known_rates[i - 1]
        prev_bday = self.known_bdays[i - 1]
        next_rate = self.known_rates[i]
        next_bday = self.known_bdays[i]

        # Perform flat forward interpolation
        a = (1 + prev_rate) ** (prev_bday / 252)
        b = (1 + next_rate) ** (next_bday / 252)
        c = (bday - prev_bday) / (next_bday - prev_bday)
        return (a * (b / a) ** c) ** (252 / bday) - 1

    def _linear(self, bday: int) -> float:
        """Performs linear interpolation."""
        np_float = np.interp(bday, self.known_bdays, self.known_rates)
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

        Examples:
                >>> known_bdays = [30, 60, 90]
                >>> known_rates = [0.045, 0.05, 0.055]
                >>> linear = Interpolator("linear", known_bdays, known_rates)
                >>> linear.interpolate(45)
                0.0475
                >>> fforward = Interpolator("flat_forward", known_bdays, known_rates)
                >>> fforward.interpolate(45)
                0.04833068080970859
        """
        # Check for edge cases
        if bday < self.known_bdays[0]:
            return self.known_rates[0]
        elif bday > self.known_bdays[-1]:
            return self.known_rates[-1]
        elif bday in self.known_bdays:
            return self.known_rates[self.known_bdays.index(bday)]

        if self.method == "flat_forward":
            return self._flat_forward(bday)
        elif self.method == "linear":
            return self._linear(bday)
        else:
            raise ValueError(f"Unknown interpolation method: {self.method}.")
