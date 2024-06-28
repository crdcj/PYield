import bisect
from typing import Literal

import numpy as np
import pandas as pd


class Interpolator:
    def __init__(
        self,
        method: Literal["flat_forward", "linear"],
        known_bdays: pd.Series,
        known_rates: pd.Series,
    ):
        """
        Initialize the Interpolator with given atributes.

        Args:
            method (Literal["flat_forward", "linear"]): Interpolation method.
            known_bdays (pd.Series): Series of known business days.
            known_rates (pd.Series): Series of known interest rates.

        Raises:
            ValueError: If known_bdays and known_rates do not have the same length.
            ValueError: If the interpolation method is not recognized

        Returns:
            Interpolator: An instance of the Interpolator

        Examples:
            >>> known_bdays = pd.Series([1, 5, 10, 15])
            >>> known_rates = pd.Series([0.01, 0.015, 0.02, 0.025])
            >>> interpolator = Interpolator("linear", known_bdays, known_rates)
        """
        self.known_bdays, self.known_rates = self._process_known_data(
            known_bdays, known_rates
        )
        self.method = self._validate_method(method)

    @staticmethod
    def _validate_method(method: str) -> str:
        """
        Validate the interpolation method.

        Args:
            method (str): Interpolation method to validate.

        Returns:
            str: Validated interpolation method.
        """
        valid_methods = ["flat_forward", "linear"]
        if method not in valid_methods:
            raise ValueError(f"Unknown interpolation method: {method}.")
        return method

    @staticmethod
    def _process_known_data(
        known_bdays: pd.Series,
        known_rates: pd.Series,
    ) -> tuple:
        """
        Process and validate known business days and interest rates.

        Args:
            known_bdays (pd.Series): Series of known business days.
            known_rates (pd.Series): Series of known interest rates.

        Returns:
            tuple: Processed lists of business days and interest rates.
        """
        if len(known_bdays) != len(known_rates):
            raise ValueError("known_bdays and known_rates must have the same length.")

        df = pd.DataFrame({"bday": known_bdays, "rate": known_rates})
        df = df.dropna().drop_duplicates(subset="bday").sort_values("bday")

        return df["bday"].to_list(), df["rate"].to_list()

    @staticmethod
    def _flat_forward(
        bday: int,
        known_bdays: list,
        known_rates: list,
    ) -> float:
        """Performs the interest rate interpolation using the flat forward method."""

        # Find i such that known_bdays[i-1] < bday < known_bdays[i]
        i = bisect.bisect_left(known_bdays, bday)

        # Get previous and next known rates and business days
        prev_rate = known_rates[i - 1]
        prev_bday = known_bdays[i - 1]
        next_rate = known_rates[i]
        next_bday = known_bdays[i]

        # Perform flat forward interpolation
        a = (1 + prev_rate) ** (prev_bday / 252)
        b = (1 + next_rate) ** (next_bday / 252)
        c = (bday - prev_bday) / (next_bday - prev_bday)
        return (a * (b / a) ** c) ** (252 / bday) - 1

    @staticmethod
    def _linear(
        bday: int,
        known_bdays: list,
        known_rates: list,
    ) -> float:
        """Performs linear interpolation."""
        np_float = np.interp(bday, known_bdays, known_rates)
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
                >>> known_bdays = pd.Series([1, 5, 10, 15])
                >>> known_rates = pd.Series([0.1, 0.2, 0.3, 0.4])
                >>> linear_interp = Interpolator("linear", known_bdays, known_rates)
                >>> linear_interp.interpolate(7)
                0.25
                >>> ff_interp = Interpolator("flat_forward", known_bdays, known_rates)
                >>> ff_interp.interpolate(7)
                0.25
        """
        known_bdays = self.known_bdays
        known_rates = self.known_rates

        # Special cases
        if bday < known_bdays[0]:
            return known_rates[0]
        elif bday > known_bdays[-1]:
            return known_rates[-1]
        elif bday in known_bdays:
            return known_rates[known_bdays.index(bday)]

        if self.method == "flat_forward":
            return self._flat_forward(bday, known_bdays, known_rates)
        elif self.method == "linear":
            return self._linear(bday, known_bdays, known_rates)
