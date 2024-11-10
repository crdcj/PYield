import pandas as pd

import pyield.b3_futures as b3
import pyield.date_converter as dc
import pyield.tools as tl
from pyield import bday, interpolator
from pyield.data_cache import get_anbima_dataset, get_di_dataset
from pyield.date_converter import DateScalar


class DIFutures:
    """
    Class to retrieve and manipulate DI futures contract data.

    This class provides access to DI futures data for a specified trade date, and
    includes options to adjust expiration dates and apply filters based on LTN and
    NTN-F bond maturities.

    Args:
        trade_date (DateScalar): The date to retrieve the contract data.
        adj_expirations (bool): If True, adjusts the expiration dates to the start
            of the month.
        prefixed_filter (bool): If True, filters the DI contracts to match only
            expirations with existing LTN and NTN-F bond maturities.

    Examples:
        To create a `DIFutures` instance and retrieve data:
        >>> di = yd.DIFutures(trade_date="16-10-2024", adj_expirations=True)
        >>> df = di.data  # Retrieve DI contract dataframe for the specified date
        >>> df.iloc[:5, :5]  # Show the first five rows and columns
          TickerSymbol ExpirationDate  BDaysToExp  OpenContracts  TradeCount
        0       DI1X24     2024-11-01          12        1744269         635
        1       DI1Z24     2024-12-01          31        1429375        1012
        2       DI1F25     2025-01-01          52        5423969        6812
        3       DI1G25     2025-02-01          74         279491          97
        4       DI1H25     2025-03-01          94         344056         221

        You can also retrieve forward rates for the DI contracts:
        >>> di.forwards.iloc[:5]  # Show the first five rows
          ExpirationDate  SettlementRate  ForwardRate
        0     2024-11-01         0.10653      0.10653
        1     2024-12-01          0.1091     0.110726
        2     2025-01-01         0.11164       0.1154
        3     2025-02-01         0.11362     0.118314
        4     2025-03-01          0.1157      0.12343
    """

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
        trade_date: DateScalar,
        adj_expirations: bool = False,
        prefixed_filter: bool = False,
    ):
        """
        Initialize the DIFutures instance with a specific trade date.
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
            df = b3.futures(contract_code="DI1", trade_date=self._trade_date)

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

        df["ForwardRate"] = tl.forward_rates(
            business_days=bday.count(self._trade_date, df["ExpirationDate"]),
            zero_rates=df["SettlementRate"],
        )

        return df[["ExpirationDate", "SettlementRate", "ForwardRate"]]

    def rate(
        self,
        expiration: DateScalar,
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
        The trade date to retrieve the DI contract data.

        This property can be both read and set. When setting a value, it automatically
        converts the input date format to a `pd.Timestamp`.

        Returns:
            pd.Timestamp: The trade date set for this instance.
        """

        return self._trade_date

    @trade_date.setter
    def trade_date(self, value: DateScalar):
        self._trade_date = dc.convert_input_dates(value)

    @property
    def adj_expirations(self) -> bool:
        """
        Adjusts the expiration dates to the start of the month.

        This property can be both read and set. When set to `True`, all expiration dates
        are adjusted to the first day of the month. For example, an expiration date of
        02/01/2025 will be adjusted to 01/01/2025.

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
        Filters DI Futures to match prefixed TN bond maturities.

        This property can be both read and set. When set to `True`, DI Futures will be
        filtered to match the maturities of LTN and NTN-F bonds from the Anbima dataset.

        Returns:
            bool: Whether DI Futures are filtered to match prefixed Anbima bond
                maturities.
        """
        return self._prefixed_filter

    @prefixed_filter.setter
    def prefixed_filter(self, value: bool):
        self._prefixed_filter = value
