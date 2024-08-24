from typing import Literal

import pandas as pd

from . import bday
from . import date_converter as dc
from .data_manager import get_anbima_dataframe, get_di_dataframe
from .fetchers import futures


class DIData:
    def __init__(self):
        """Initialize the DIData class."""
        pass  # A inicialização não precisa carregar os dados, o cache cuidará disso.

    @staticmethod
    def _get_rate_column(rate_type: str) -> str:
        """Map the rate type to the corresponding column name."""
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

    def rates(
        self,
        trade_date: str | pd.Timestamp,
        rate_type: Literal["settlement", "min", "max", "close", "last"] = "settlement",
        adj_expirations: bool = False,
        prefixed_filter: bool = False,
    ) -> pd.DataFrame:
        """Retrieve the rates for the specified trade date and rate type."""
        rate_col = self._get_rate_column(rate_type)
        bz_last_bday = bday.offset(trade_date, 0, roll="backward")
        trade_date = dc.convert_date(trade_date)

        if trade_date == bz_last_bday:
            # There is no historical data for the last business day.
            df = futures(contract_code="DI1", trade_date=trade_date)
        else:
            df = get_di_dataframe()
            df = df[["TradeDate", "ExpirationDate", f"{rate_col}"]].copy()
            df.query("TradeDate == @trade_date", inplace=True)

        df.drop(columns=["TradeDate"], inplace=True)

        if prefixed_filter:
            df_anb = get_anbima_dataframe()
            df_anb.query("ReferenceDate == @trade_date", inplace=True)
            df_pre = df_anb.query("BondType in ['LTN', 'NTN-F']").copy()
            pre_maturities = df_pre["MaturityDate"].drop_duplicates(ignore_index=True)
            adj_pre_maturities = bday.offset(pre_maturities, 0)  # noqa
            df = df.query("ExpirationDate in @adj_pre_maturities")

        if adj_expirations:
            df["ExpirationDate"] = df["ExpirationDate"].dt.to_period("M")
            df["ExpirationDate"] = df["ExpirationDate"].dt.to_timestamp()

        return df.sort_values(["ExpirationDate"], ignore_index=True)
