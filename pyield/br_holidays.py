from pathlib import Path
from typing import Literal

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

    def _load_holidays(self, file_path: Path) -> pd.Series:
        """Loads the list of holidays from a text file and returns it as a pd.Series of pd.Timestamp."""
        df = pd.read_csv(file_path, header=None, names=["date"], comment="#")
        # Convert the dates to a pd.Series of pd.Timestamp
        return pd.to_datetime(df["date"], format="%d/%m/%Y")

    def get_applicable_holidays(
        self,
        dates: pd.Timestamp | pd.Series,
        holiday_list: Literal["old", "new", "infer"] = "infer",
    ) -> pd.Series:
        """
        Returns the correct list of holidays to use based on the most recent date in the input.

        Args:
            dates (pd.Timestamp | pd.Series): The date(s) to use to infer the holidays.
            holiday_list (str): The holidays list to use. Valid options are 'old', 'new' or
                'infer'. If 'infer' is used, the list of holidays is selected based on the
                earliest (minimum) date in the input.

        Returns:
            pd.Series: The list of holidays to use.
        """
        if isinstance(dates, pd.Timestamp):
            earliest_date = dates
        else:
            earliest_date = dates.min()

        match holiday_list:
            case "old":
                return self.old_holidays
            case "new":
                return self.new_holidays
            case "infer":
                if earliest_date < BrHolidays.TRANSITION_DATE:
                    return self.old_holidays
                else:
                    return self.new_holidays
            case _:
                raise ValueError("Invalid holiday list option.")
