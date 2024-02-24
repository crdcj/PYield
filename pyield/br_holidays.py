from pathlib import Path
from typing import Literal

import pandas as pd
import numpy as np


class Holidays:
    # The date (inclusive) when the new list of holidays starts to be valid
    TRANSITION_DATE = np.datetime64("2023-12-26", "D")
    CURRENT_DIR = Path(__file__).parent
    NEW_HOLIDAYS_PATH = CURRENT_DIR / "br_holidays_new.txt"
    OLD_HOLIDAYS_PATH = CURRENT_DIR / "br_holidays_old.txt"

    def __init__(self):
        self.new_holidays = self._load_holidays(Holidays.NEW_HOLIDAYS_PATH)
        self.old_holidays = self._load_holidays(Holidays.OLD_HOLIDAYS_PATH)

    def _load_holidays(self, file_path: Path) -> np.array:
        df = pd.read_csv(file_path, header=None, names=["date"], comment="#")
        df["date"] = pd.to_datetime(df["date"], format="%d/%m/%Y")
        return df["date"].values.astype("datetime64[D]")

    def get_applicable_holidays(
        self,
        dates: np.datetime64 | np.ndarray,
        holiday_list: Literal["old", "new", "infer"] = "infer",
    ) -> np.array:
        """
        Returns the correct list of holidays to use based on the most recent date in the input.

        Args:
            dates (pd.Timestamp): A single date or a Series of dates.
            select (str): The list of holidays to use. Valid options are 'old', 'new' or
                'infer'. If 'infer' is used, the list of holidays is selected based on the
                most recent (minimum) date in the input.

        Returns:
            np.array: The list of holidays to use.
        """

        match holiday_list:
            case "old":
                return self.old_holidays
            case "new":
                return self.new_holidays
            case "infer":
                if np.any(dates < Holidays.TRANSITION_DATE):
                    return self.old_holidays
                else:
                    return self.new_holidays
            case _:
                raise ValueError("Invalid holiday list option.")
