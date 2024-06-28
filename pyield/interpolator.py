import bisect
from typing import Literal

import pandas as pd


class Interpolator:
    def __init__(
        self,
        interpolation_method: Literal["flat_forward"],
        known_bdays: pd.Series,
        known_rates: pd.Series,
    ):
        """
        Initialize the Interpolator with known business days and corresponding interest
        rates.

        Args:
            known_bdays (pd.Series): Series of business days where interest rates are
                known.
            known_rates (pd.Series): Series of known interest rates.
        """
        self.interpolation_type = interpolation_method

        if len(known_bdays) != len(known_rates):
            raise ValueError("known_bdays and known_rates must have the same length.")

        df = pd.DataFrame({"bday": known_bdays, "rate": known_rates})
        df.dropna(inplace=True)
        df.drop_duplicates(subset="bday", inplace=True)
        df.sort_values("bday", inplace=True)

        self.known_bdays = df["bday"].to_list()
        self.known_rates = df["rate"].to_list()

    @staticmethod
    def _flat_forward(
        prev_rate: float,
        prev_bday: int,
        next_rate: float,
        next_bday: int,
        bday: int,
    ) -> float:
        """
        Performs interest rate interpolation using the flat forward interpolation method
        considering a base of 252 business days.

        Args:
            prev_rate (float): Interest rate of the previous vertex.
            prev_bday (int):  Number of business days of the previous vertex.
            next_rate (float): Interest rate of the next vertex.
            next_bday (int): Number of business days of the next vertex.
            bday (int): Number of business days for which the interest
                rate is to be interpolated.

        Returns:
            float: The interpolated interest rate at the given `business_days`.
        """
        a = (1 + prev_rate) ** (prev_bday / 252)
        b = (1 + next_rate) ** (next_bday / 252)
        c = (bday - prev_bday) / (next_bday - prev_bday)

        return (a * (b / a) ** c) ** (252 / bday) - 1

    def find_and_interpolate(self, bday: int) -> float:
        """
        Finds the appropriate interpolation point and returns the interest rate
        interpolated by the flat forward method from that point.

        Args:
            bdays (int): Number of business days for which the flat forward interest
                rate is to be calculated.

        Returns:
            float: The interest rate interpolated by the flat forward method for the
                given number of business days.
        """
        known_bdays = self.known_bdays
        known_rates = self.known_rates

        # Special cases
        if bday < known_bdays[0]:
            return known_rates[0]
        elif bday > known_bdays[-1]:
            return known_rates[-1]
        # Do not interpolate vertex whose rate is known
        elif bday in known_bdays:
            return known_rates[known_bdays.index(bday)]

        # Find i such that known_bdays[i-1] < bdays < known_bdays[i]
        i = bisect.bisect_left(known_bdays, bday)

        return self._flat_forward(
            known_rates[i - 1],
            known_bdays[i - 1],
            known_rates[i],
            known_bdays[i],
            bday,
        )
