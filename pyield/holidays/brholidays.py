from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd


class BrHolidays:
    # The date (inclusive) when the new holidays list starts to be valid
    TRANSITION_DATE = pd.to_datetime("2023-12-26")

    def __init__(self):
        current_dir = Path(__file__).parent
        new_holidays_path = current_dir / "br_holidays_new.txt"
        old_holidays_path = current_dir / "br_holidays_old.txt"
        self.new_holidays = self._load_holidays(new_holidays_path)
        self.old_holidays = self._load_holidays(old_holidays_path)

    @staticmethod
    def _load_holidays(file_path: Path) -> pd.Series:
        """Loads the holidays from a file and returns it as a Series of Timestamps."""
        df = pd.read_csv(file_path, header=None, names=["date"], comment="#")
        holidays = pd.to_datetime(df["date"], format="%d/%m/%Y")
        return holidays.astype("datetime64[ns]")

    def get_holiday_series(
        self,
        dates: pd.Timestamp | pd.Series | None = None,
        holiday_option: Literal["old", "new", "infer"] = "infer",
    ) -> pd.Series:
        """
        Returns the correct list of holidays to use based on the most recent date in
        the input.

        Args:
            dates (pd.Timestamp | pd.Series | None): The dates to use for inferring
                the holiday list. If a single date is provided, it is used directly.
                If a Series of dates is provided, the earliest date is used.

            holiday_option (Literal): The holidays list to use. Valid options are
                'old', 'new' or 'infer'. If 'infer' is used, the list of holidays is
                selected based on the earliest (minimum) date in the input.

        Returns:
            pd.Series: The list of holidays as a Series of Timestamps.
        """
        match holiday_option:
            case "old":
                holidays = self.old_holidays

            case "new":
                return self.new_holidays

            case "infer":
                if dates is None:
                    msg = "Dates must be provided when using 'infer' option."
                    raise ValueError(msg)
                if isinstance(dates, pd.Timestamp):
                    earliest_date = dates
                else:
                    earliest_date = dates.min()

                if earliest_date < BrHolidays.TRANSITION_DATE:
                    holidays = self.old_holidays
                else:
                    holidays = self.new_holidays
            case _:
                raise ValueError("Invalid holiday list option.")

        return holidays

    def get_holiday_array(
        self,
        dates: pd.Timestamp | pd.Series | None = None,
        holiday_option: Literal["old", "new", "infer"] = "infer",
    ) -> np.ndarray:
        """
        Returns the correct list of holidays to use based on the most recent date in
        the input.

        Args:
            dates (pd.Timestamp | pd.Series | None): The dates to use for inferring
                the holiday list. If a single date is provided, it is used directly.
                If a Series of dates is provided, the earliest date is used.

            holiday_option (Literal): The holidays list to use. Valid options are
                'old', 'new' or 'infer'. If 'infer' is used, the list of holidays is
                selected based on the earliest (minimum) date in the input.

        Returns:
            np.ndarray: The list of holidays as a NumPy array of datetime64[D].
        """
        holidays = self.get_holiday_series(dates, holiday_option)
        return holidays.to_numpy().astype("datetime64[D]")
