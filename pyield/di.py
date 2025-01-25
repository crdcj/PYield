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
        trade_date (DateScalar | None): The trade date to retrieve the
            DI contract data. If None, the latest available historical trade date is
            used.
        month_start (bool): If True, adjusts the expirations to the start of the month.
        pre_filter (bool): If True, filters the DI contracts to match only
            expirations with existing prefixed TN bond maturities (LTN and NTN-F).
        all_columns (bool): If True, returns all available columns in the DI dataset.
            If False, only the most common columns are returned.

    Examples:
        To create a `DIFutures` instance and retrieve data:
        >>> dif = yd.DIFutures(trade_date="16-10-2024", month_start=True)
        >>> df = dif.df  # Retrieve DI contract dataframe for the specified date
        >>> df.iloc[:5, :5]  # Show the first five rows and columns
           TradeDate ExpirationDate TickerSymbol  BDaysToExp  OpenContracts
        0 2024-10-16     2024-11-01       DI1X24          12        1744269
        1 2024-10-16     2024-12-01       DI1Z24          31        1429375
        2 2024-10-16     2025-01-01       DI1F25          52        5423969
        3 2024-10-16     2025-02-01       DI1G25          74         279491
        4 2024-10-16     2025-03-01       DI1H25          94         344056

        You can also retrieve forward rates for the DI contracts:
        >>> dif.forwards.iloc[:5]  # Show the first five rows
           TradeDate ExpirationDate  SettlementRate  ForwardRate
        0 2024-10-16     2024-11-01         0.10653      0.10653
        1 2024-10-16     2024-12-01          0.1091     0.110726
        2 2024-10-16     2025-01-01         0.11164       0.1154
        3 2024-10-16     2025-02-01         0.11362     0.118314
        4 2024-10-16     2025-03-01          0.1157      0.12343
    """

    historical_trade_dates = (
        get_di_dataset()
        .drop_duplicates(subset=["TradeDate"])["TradeDate"]
        .sort_values(ascending=True)
        .reset_index(drop=True)
    )
    """
    pd.Series: Sorted series of unique trade dates available in the DI dataset.
    It does not include the intraday date. It can be used to check for available
    historical data.
    """

    def __init__(
        self,
        trade_date: DateScalar | None = None,
        month_start: bool = False,
        pre_filter: bool = False,
        all_columns: bool = True,
    ):
        """
        Initialize the DIFutures instance with the specified parameters.

        Note:
            The DI contract DataFrame is not loaded during initialization. It will be
            automatically loaded the first time the df() method is accessed.
        """
        self._df = pd.DataFrame()
        self._dirty = True  # Attribute to track if the df needs updating

        self.trade_date = trade_date
        self.month_start = month_start
        self.pre_filter = pre_filter
        self.all_columns = all_columns

    @property
    def df(self) -> pd.DataFrame:
        """
        Returns a copy of the DI contract DataFrame for the initialized trade date.

        If the internal state is marked as 'dirty', the DataFrame will be (re)loaded
        before returning, ensuring that the returned data reflects the latest attribute
        values.

        Returns:
            pd.DataFrame: The DI contract data.
        """
        if self._dirty:
            self._df = self._update_df()
            self._dirty = False
        return self._df.copy()

    def _update_df(self) -> pd.DataFrame:
        """Retrieve DI contract DataFrame for the initialized trade date."""
        bz_today = pd.Timestamp.today(tz="America/Sao_Paulo").normalize()
        if bz_today == self.trade_date:
            # Try to get intraday data
            df = b3.futures(contract_code="DI1", trade_date=bz_today)
        else:
            # Get historical data
            df = (
                get_di_dataset()
                .query("TradeDate == @self._trade_date")
                .reset_index(drop=True)
            )

        if df.empty:
            return pd.DataFrame()

        if "DaysToExpiration" in df.columns:
            df.drop(columns=["DaysToExpiration"], inplace=True)

        if self._pre_filter:
            df_pre = (
                get_anbima_dataset()
                .query("BondType in ['LTN', 'NTN-F']")
                .query("ReferenceDate == @self._trade_date")[
                    ["ReferenceDate", "MaturityDate"]
                ]
                .drop_duplicates()
                .reset_index(drop=True)
            )
            # Force the expiration date to be a business day as DI contracts
            df_pre["MaturityDate"] = bday.offset(df_pre["MaturityDate"], 0)
            df_pre = df_pre.rename(
                columns={
                    "ReferenceDate": "TradeDate",
                    "MaturityDate": "ExpirationDate",
                }
            )

            df = df.merge(df_pre, how="inner")

        if self._month_start:
            df["ExpirationDate"] = (
                df["ExpirationDate"].dt.to_period("M").dt.to_timestamp()
            )

        if not self._all_columns:
            cols = [
                "TradeDate",
                "TickerSymbol",
                "ExpirationDate",
                "BDaysToExp",
                "OpenContracts",
                "TradeVolume",
                "DV01",
                "OpenRate",
                "MinRate",
                "MaxRate",
                "CloseRate",
                "SettlementRate",
                "LastRate",
                "SettlementPrice",
                "LastPrice",
            ]
            selected_cols = [col for col in cols if col in df.columns]
            df = df[selected_cols].copy()

        return df.sort_values(by=["TradeDate", "ExpirationDate"]).reset_index(drop=True)

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
                - SettlementRate: The zero rate (interest rate) for each expiration.
                - ForwardRate: The fwd rate calculated between successive expirations.
        """
        df = self.df
        if df.empty:
            return pd.DataFrame()

        df["ForwardRate"] = tl.forward_rates(
            business_days=bday.count(df["TradeDate"], df["ExpirationDate"]),
            zero_rates=df["SettlementRate"],
            groupby_dates=df["TradeDate"],
        )

        return df[["TradeDate", "ExpirationDate", "SettlementRate", "ForwardRate"]]

    @staticmethod
    def interpolate_rates(
        trade_dates: DateScalar | DateArray,
        expirations: DateScalar | DateArray,
        extrapolate: bool = True,
    ) -> pd.Series:
        """
        Interpolates DI rates for specified trade dates and maturities. The method
        is recomended to be used in datasets calculations with multiple dates.

        This method calculates interpolated DI rates for a given set of trade
        dates and maturities using a flat-forward interpolation method. If no DI
        rates are available for a reference date, the interpolated rate is set to NaN.

        If trade_dates is provided as a scalar and expirations as an array, the
        method assumes the scalar value is the same for all maturities. The same logic
        applies when the maturities are scalar and the trade dates are an array.

        Args:
            trade_dates (DateScalar | DateArray): The trade dates for the rates.
            expirations (DateScalar | DateArray): The expirations corresponding to the
                trade dates.
            extrapolate (bool): Whether to allow extrapolation beyond known DI rates.

        Returns:
            pd.Series: A Series containing the interpolated DI rates.

        Raises:
            ValueError: If `reference_dates` and `maturities` have different lengths.
        """
        # Convert input dates to a consistent format
        trade_dates = dc.convert_input_dates(trade_dates)
        expirations = dc.convert_input_dates(expirations)

        # Ensure the lengths of input arrays are consistent
        match (trade_dates, expirations):
            case pd.Timestamp(), pd.Series():
                dfi = pd.DataFrame({"mat": expirations})
                dfi["tdate"] = trade_dates

            case pd.Series(), pd.Timestamp():
                dfi = pd.DataFrame({"tdate": trade_dates})
                dfi["mat"] = expirations

            case pd.Series(), pd.Series():
                if len(trade_dates) != len(expirations):
                    raise ValueError("Args. should have the same length.")
                dfi = pd.DataFrame({"tdate": trade_dates, "mat": expirations})

            case pd.Timestamp(), pd.Timestamp():
                dfi = pd.DataFrame({"tdate": [trade_dates], "mat": [expirations]})

        # Compute business days between reference dates and maturities
        dfi["bdays"] = bday.count(dfi["tdate"], dfi["mat"])

        # Initialize the interpolated rate column with NaN
        dfi["irate"] = pd.NA
        dfi["irate"] = dfi["irate"].astype("Float64")

        # Load DI rates dataset filtered by the provided reference dates
        dfr = (
            get_di_dataset()
            .query("TradeDate in @dfi['tdate'].unique()")
            .reset_index(drop=True)
        )

        # Return an empty DataFrame if no rates are found
        if dfr.empty:
            return pd.Series()

        # Iterate over each unique reference date
        for date in dfi["tdate"].unique():
            # Filter DI rates for the current reference date
            dfr_subset = dfr.query("TradeDate == @date").reset_index(drop=True)

            # Skip processing if no rates are available for the current date
            if dfr_subset.empty:
                continue

            # Initialize the interpolator with known rates and business days
            interp = interpolator.Interpolator(
                method="flat_forward",
                known_bdays=dfr_subset["BDaysToExp"],
                known_rates=dfr_subset["SettlementRate"],
                extrapolate=extrapolate,
            )

            # Apply interpolation to rows matching the current reference date
            mask: pd.Series = dfi["tdate"] == date
            dfi.loc[mask, "irate"] = dfi.loc[mask, "bdays"].apply(interp)

        # Return the Series with interpolated rates
        s_irates = dfi["irate"]
        s_irates.name = "InterpolatedRates"
        return s_irates

    def rate(
        self,
        expiration: DateScalar,
        interpolate: bool = True,
        extrapolate: bool = False,
    ) -> float:
        """Retrieve the DI rate for a specified expiration date."""
        expiration = dc.convert_input_dates(expiration)

        if self._month_start:
            # Force the expiration date to be the start of the month
            expiration = expiration.to_period("M").to_timestamp()
        else:
            # Force the expiration date to be a business day
            expiration = bday.offset(expiration, 0)

        if not interpolate and extrapolate:
            raise ValueError("Extrapolation is not allowed without interpolation.")
        # Get the DI contract DataFrame
        df = self.df

        if df.empty:
            return float("NaN")

        df_exp = df.query("ExpirationDate == @expiration")

        if df_exp.empty and not interpolate:
            return float("NaN")

        if expiration in df_exp["ExpirationDate"].values:
            return float(df_exp["SettlementRate"].iat[0])

        if not interpolate:
            return float("NaN")

        ff_interp = interpolator.Interpolator(
            method="flat_forward",
            known_bdays=df["BDaysToExp"],
            known_rates=df["SettlementRate"],
            extrapolate=extrapolate,
        )
        bd = bday.count(self._trade_date, expiration)
        return ff_interp(bd)

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
    def trade_date(self, value: DateScalar | None):
        if value:
            self._trade_date = dc.convert_input_dates(value)
        else:
            self._trade_date = self.historical_trade_dates.max()

        self._dirty = True

    @property
    def month_start(self) -> bool:
        """
        Adjusts the expiration dates to the start of the month.

        This property can be both read and set. When set to `True`, all expiration dates
        are adjusted to the first day of the month. For example, an expiration date of
        02/01/2025 will be adjusted to 01/01/2025.

        Returns:
            bool: Whether expiration dates are adjusted to the start of the month.
        """
        return self._month_start

    @month_start.setter
    def month_start(self, value: bool):
        self._month_start = value
        self._dirty = True

    @property
    def pre_filter(self) -> bool:
        """
        Filters DI Futures to match prefixed TN bond maturities.

        This property can be both read and set. When set to `True`, only contracts that
        match the maturities of LTN and NTN-F bonds will be shown.

        Returns:
            bool: Whether the contracts are filtered to match prefixed TN bond
                maturities from the ANBIMA dataset.
        """
        return self._pre_filter

    @pre_filter.setter
    def pre_filter(self, value: bool):
        self._pre_filter = value
        self._dirty = True

    @property
    def all_columns(self) -> bool:
        return self._all_columns

    @all_columns.setter
    def all_columns(self, value: bool):
        self._all_columns = value
        self._dirty = True
