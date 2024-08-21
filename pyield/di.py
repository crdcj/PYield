from typing import Literal

import pandas as pd

from . import bday
from . import date_converter as dc

DI_URL = "https://raw.githubusercontent.com/crdcj/pyield-data/main/di_data.parquet"


class DIData:
    _df = pd.DataFrame()

    def __init__(self):
        """Initialize the DIData class and load the data."""
        if self._df.empty or not self._is_data_up_to_date():
            self._load_data()

    @classmethod
    def _load_data(cls):
        print("Loading DI dataset...")
        cls._df = pd.read_parquet(DI_URL)
        cls._last_update = pd.Timestamp.today().normalize()

    @classmethod
    def _is_data_up_to_date(cls) -> bool:
        """Check if the last date in the file is the last available ANBIMA date."""
        if cls._df.empty:
            return False
        today = pd.Timestamp.today().normalize()
        last_di_date = bday.offset(today, -1)
        last_file_date = cls._df["TradeDate"].max()
        return last_di_date == last_file_date

    @classmethod
    def _check_for_updates(cls):
        """Check if the data is up to date. If not, load the latest data."""
        if cls._df.empty or not cls._is_data_up_to_date():
            cls._load_data()

    @classmethod
    def _get_dataframe(cls):
        cls._load_data()
        return cls._df.copy()

    @staticmethod
    def _get_rate_column(rate_type: str) -> str:
        rate_map = {
            "settlement": "SettlementRate",
            "min": "MinRate",
            "max": "MaxRate",
            "close": "CloseRate",
        }
        if rate_type not in rate_map:
            raise ValueError(
                "Invalid rate type. Use 'settlement', 'min', 'max', or 'close'."
            )
        return rate_map[rate_type]

    @classmethod
    def rates(
        cls,
        trade_date: str | pd.Timestamp | None = None,
        expiration_date: str | pd.Timestamp | None = None,
        rate_type: Literal["settlement", "min", "max", "close"] = "settlement",
        adjust_exp_date: bool = False,
    ) -> pd.DataFrame | float:
        rate_col = cls._get_rate_column(rate_type)
        cls._check_for_updates()
        df = cls._df[["TradeDate", "ExpirationDate", f"{rate_col}"]].copy()

        if expiration_date:
            expiration_date = dc.convert_date(expiration_date)
            # Force the expiration date to be a business day
            expiration_date = bday.offset(expiration_date, 0)
            df.query("ExpirationDate == @expiration_date", inplace=True)

        if trade_date:
            trade_date = dc.convert_date(trade_date)
            df.query("TradeDate == @trade_date", inplace=True)

        if adjust_exp_date:
            df["ExpirationDate"] = df["ExpirationDate"].dt.to_period("M")
            df["ExpirationDate"] = df["ExpirationDate"].dt.to_timestamp()

        df.sort_values(["TradeDate", "ExpirationDate"]).reset_index(drop=True)
        if len(df.index) == 1:
            return float(df[f"{rate_col}"].values[0])
        else:
            return df
