import pandas as pd

import pyield.b3_futures as b3
import pyield.date_converter as dc
import pyield.tools as tl
from pyield import bday, interpolator
from pyield.data_cache import get_anbima_dataset, get_di_dataset
from pyield.date_converter import DateArray, DateScalar


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
          TickerSymbol ExpirationDate  bdays_to_mat  OpenContracts  TradeCount
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

    historical_trade_dates = (
        get_di_dataset()
        .drop_duplicates(subset=["TradeDate"])["TradeDate"]
        .sort_values(ascending=True)
        .reset_index(drop=True)
    )
    """
    pd.Series: Sorted series of unique trade dates available in the DI dataset.
    It does not include the current date. It can be used to check for available
    historical data.
    """

    def __init__(
        self,
        trade_dates: DateScalar | DateArray | None = None,
        adj_expirations: bool = False,
        prefixed_filter: bool = False,
        all_columns: bool = True,
    ):
        """
        Initialize the DIFutures instance with a specific trade date.
        """
        self.trade_dates = trade_dates
        self.adj_expirations = adj_expirations
        self.prefixed_filter = prefixed_filter
        self.all_columns = all_columns

    @property
    def data(self) -> pd.DataFrame:
        """Retrieve DI contract DataFrame for the initialized trade date."""
        df = (
            get_di_dataset()
            .query("TradeDate in @self._trade_dates")
            .reset_index(drop=True)
        )

        bz_today = pd.Timestamp.today(tz="America/Sao_Paulo").normalize()
        if bz_today in self.trade_dates:
            df_today = b3.futures(contract_code="DI1", trade_date=self._trade_dates)
            df = pd.concat([df, df_today], ignore_index=True)

        if df.empty:
            return pd.DataFrame()

        if "DaysToExpiration" in df.columns:
            df.drop(columns=["DaysToExpiration"], inplace=True)

        if self._prefixed_filter:
            df_pre = (
                get_anbima_dataset()
                .query("BondType in ['LTN', 'NTN-F']")
                .query("ReferenceDate in @self._trade_dates")[
                    ["ReferenceDate", "MaturityDate"]
                ]
                .drop_duplicates()
                .reset_index(drop=True)
            )
            df_pre["MaturityDate"] = bday.offset(df_pre["MaturityDate"], 0)
            df_pre = df_pre.rename(
                columns={
                    "ReferenceDate": "ExpirationDate",
                    "MaturityDate": "ExpirationDate",
                }
            )

            df = df.merge(df_pre, how="inner")

        if self._adj_expirations:
            df["ExpirationDate"] = (
                df["ExpirationDate"].dt.to_period("M").dt.to_timestamp()
            )

        if not self._all_columns:
            cols = [
                "TradeDate",
                "ExpirationDate",
                "TickerSymbol",
                "OpenContracts",
                "TradeVolume",
                "OpenRate",
                "MinRate",
                "MaxRate",
                "CloseRate",
                "SettlementRate",
            ]
            df = df[cols].copy()

        return df.sort_values(by=["TradeDate", "ExpirationDate"]).reset_index(drop=True)

    @property
    def forwards(self) -> pd.DataFrame:
        """
        Calculate the DI forward rates for the initialized trade dates.

        This property returns a DataFrame with both the SettlementRate and the
        calculated ForwardRate for DI contracts, based on the instance's trade dates and
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
            business_days=bday.count(self._trade_dates, df["ExpirationDate"]),
            zero_rates=df["SettlementRate"],
            groupby_dates=df["TradeDate"],
        )

        return df[["TradeDate", "ExpirationDate", "SettlementRate", "ForwardRate"]]

    @staticmethod
    def interpolate_rates(
        reference_dates: DateScalar | DateArray,
        maturities: DateScalar | DateArray,
        allow_extrapolation: bool = True,
    ) -> pd.DataFrame:
        """
        dfi: Input DataFrame with reference dates and maturities
        dfr: Rates DataFrame with DI rates for each reference date
        dfo: Ouput DataFrame with interpolated rates
        """
        reference_dates = list(dc.convert_input_dates(reference_dates))
        maturities = list(dc.convert_input_dates(maturities))

        if len(reference_dates) != len(maturities):
            raise ValueError("Dates and maturities must have the same length.")

        dfi = pd.DataFrame({"reference_date": reference_dates, "maturity": maturities})
        dfi["bdays"] = bday.count(dfi["reference_date"], dfi["maturity"])
        dfi["interpolated_rate"] = pd.NA
        dfi["interpolated_rate"] = dfi["interpolated_rate"].astype("Float64")

        df_di = (
            get_di_dataset()
            .query("TradeDate in @reference_dates")
            .reset_index(drop=True)
        )

        if df_di.empty:
            return pd.DataFrame()

        # Iterate over each unique reference date
        for date in set(reference_dates):
            dfr_subset = df_di.query("TradeDate == @date").copy()
            if not dfr_subset.empty:
                interp = interpolator.Interpolator(
                    method="flat_forward",
                    known_bdays=dfr_subset["BDaysToExp"],
                    known_rates=dfr_subset["SettlementRate"],
                    extrapolate=allow_extrapolation,
                )
                # Apply interpolation only to the subset of the DataFrame
                mask = dfi["reference_date"] == date
                dfi.loc[mask, "interpolated_rate"] = dfi.loc[mask, "bdays"].apply(
                    interp
                )

        return dfi

    @property
    def trade_dates(self) -> list:
        """
        The trade dates to retrieve the DI contract data.

        This property can be both read and set. When setting a value, it automatically
        converts the input date format to a `pd.Timestamp`.

        Returns:
            list: The trade dates set for this instance.
        """

        return self._trade_dates

    @trade_dates.setter
    def trade_dates(self, value: DateScalar | DateArray | None):
        if value is None:
            trade_dates = self.historical_trade_dates.max()
        else:
            trade_dates = dc.convert_input_dates(value)

        if isinstance(trade_dates, pd.Timestamp):
            self._trade_dates = [trade_dates]
        elif isinstance(trade_dates, pd.Series):
            self._trade_dates = trade_dates.drop_duplicates().sort_values().to_list()

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

    @property
    def all_columns(self) -> bool:
        return self._all_columns

    @all_columns.setter
    def all_columns(self, value: bool):
        self._all_columns = value
