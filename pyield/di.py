import pandas as pd

from pyield import bday, interpolator
from pyield import date_converter as dc
from pyield.b3_futures import futures
from pyield.data_cache import get_anbima_dataset, get_di_dataset
from pyield.tools import forward_rates


class DIFutures:
    trade_dates = (
        get_di_dataset()
        .drop_duplicates(subset=["TradeDate"])["TradeDate"]
        .sort_values(ascending=True)
        .reset_index(drop=True)
    )
    """
    pd.Series: Sorted series of unique trade dates available in the DI dataset.
    It does not include the current date. It is updated when `reload_data`
    is called and can be used to check for available historical data.
    """

    def __init__(
        self,
        trade_date: str | pd.Timestamp,
        adj_expirations: bool = False,
        prefixed_filter: bool = False,
    ):
        """
        Initialize the DIFutures instance with a specific trade date.

        Args:
            trade_date (str | pd.Timestamp): The date to retrieve the contract data.
            adj_expirations (bool): If True, adjusts the expiration dates to the start
                of the month.
            prefixed_filter (bool): If True, filters the DI contracts to match LTN and
                NTN-F bond maturities.
        """
        self._trade_date = dc.convert_input_dates(trade_date)
        self._adj_expirations = adj_expirations
        self._prefixed_filter = prefixed_filter

    @property
    def data(self) -> pd.DataFrame:
        """Retrieve DI contract DataFrame for the initialized trade date."""
        df = (
            get_di_dataset()
            .query("TradeDate == @self.trade_date")
            .reset_index(drop=True)
        )

        if df.empty:
            df = futures(contract_code="DI1", trade_date=self._trade_date)

        if df.empty:
            return pd.DataFrame()

        df.drop(columns=["TradeDate"], inplace=True)
        if "DaysToExpiration" in df.columns:
            df.drop(columns=["DaysToExpiration"], inplace=True)

        if self._prefixed_filter:
            df_anbima = get_anbima_dataset()

            # Find the closest Anbima reference date to the trade date
            anbima_dates = (
                df_anbima["ReferenceDate"].drop_duplicates().reset_index(drop=True)
            )
            dates_diff = (anbima_dates - self._trade_date).abs()
            closest_date = anbima_dates[dates_diff.idxmin()]  # noqa

            df_pre = (
                df_anbima.query("ReferenceDate == @closest_date")
                .query("BondType in ['LTN', 'NTN-F']")
                .drop_duplicates(ignore_index=True)
            )
            pre_maturities = df_pre["MaturityDate"]
            adj_pre_maturities = bday.offset(pre_maturities, 0)  # noqa
            df.query("ExpirationDate in @adj_pre_maturities", inplace=True)

        if self._adj_expirations:
            df["ExpirationDate"] = (
                df["ExpirationDate"].dt.to_period("M").dt.to_timestamp()
            )

        return df.sort_values(["ExpirationDate"], ignore_index=True)

    @property
    def expirations(self) -> pd.Series:
        """
        Get the unique expiration dates for DI contracts on the initialized trade date.

        This property returns the unique expiration dates for DI contracts
        based on the instance's trade date and applied filters.

        Returns:
            pd.Series: A Series of unique expiration dates for DI contracts.
        """
        df = self.data.drop_duplicates(subset=["ExpirationDate"])
        return df["ExpirationDate"].sort_values(ignore_index=True)

    @property
    def forwards(self) -> pd.DataFrame:
        """
        Calculate the DI forward rates for the initialized trade date.

        This property returns a DataFrame with both the SettlementRate and the
        calculated ForwardRate for DI contracts, based on the instance's trade date and
        applied filters.

        Returns:
            pd.DataFrame: A DataFrame containing the following columns:
                - ExpirationDate: The expiration dates of the DI contracts.
                - SettlementRate: The zero rate (interest rate) for each expiration
                  date.
                - ForwardRate: The forward rate calculated between successive expiration
                  dates.
        """
        df = self.data
        if df.empty:
            return pd.DataFrame()

        df["ForwardRate"] = forward_rates(
            business_days=bday.count(self._trade_date, df["ExpirationDate"]),
            zero_rates=df["SettlementRate"],
        )

        return df[["ExpirationDate", "SettlementRate", "ForwardRate"]]

    def rate(
        self,
        expiration: str | pd.Timestamp,
        interpolate: bool = True,
        extrapolate: bool = False,
    ) -> float:
        """Retrieve the DI rate for a specified expiration date."""
        expiration = dc.convert_input_dates(expiration)

        if self._adj_expirations:
            # Force the expiration date to be the start of the month
            expiration = expiration.to_period("M").to_timestamp()
        else:
            # Force the expiration date to be a business day
            expiration = bday.offset(expiration, 0)

        if not interpolate and extrapolate:
            raise ValueError("Extrapolation is not allowed without interpolation.")

        df = self.data

        if df.empty:
            return float("NaN")

        df_exp = df.query("ExpirationDate == @expiration")

        if df_exp.empty and not interpolate:
            return float("NaN")

        if expiration in df_exp["ExpirationDate"].values:
            return float(df_exp["SettlementRate"].iat[0])

        if not interpolate:
            return float("NaN")

        ff_interpolator = interpolator.Interpolator(
            method="flat_forward",
            known_bdays=bday.count(self._trade_date, df["ExpirationDate"]),
            known_rates=df["SettlementRate"],
            extrapolate=extrapolate,
        )

        return ff_interpolator(bday.count(self._trade_date, expiration))

    @property
    def trade_date(self) -> pd.Timestamp:
        """
        Get or set the trade date for this DIFutures instance.

        Returns:
            pd.Timestamp: The trade date set for this instance.
        """
        return self._trade_date

    @trade_date.setter
    def trade_date(self, value: str | pd.Timestamp):
        self._trade_date = dc.convert_input_dates(value)

    @property
    def adj_expirations(self) -> bool:
        """
        Get or set the adj_expirations flag for this DIFutures instance.

        Returns:
            bool: Whether expiration dates are adjusted to the start of the month.
        """
        return self._adj_expirations

    @adj_expirations.setter
    def adj_expirations(self, value: bool):
        self._adj_expirations = value

    @property
    def prefixed_filter(self) -> bool:
        """
        Get or set the prefixed_filter flag for this DIFutures instance.

        Returns:
            bool: Whether DI Futures are filtered to match prefixed Anbima bond
                maturities.
        """
        return self._prefixed_filter

    @prefixed_filter.setter
    def prefixed_filter(self, value: bool):
        self._prefixed_filter = value
