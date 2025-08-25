import datetime as dt
import logging
import socket
from typing import Literal
from urllib.error import HTTPError, URLError
from zoneinfo import ZoneInfo

import pandas as pd

from pyield import bday
from pyield.bc.ptax_api import ptax
from pyield.data_cache import get_cached_dataset
from pyield.date_converter import DateScalar, convert_input_dates
from pyield.retry import default_retry
from pyield.tn.ntnb import duration as duration_b
from pyield.tn.ntnc import duration as duration_c
from pyield.tn.ntnf import duration as duration_f

BZ_TIMEZONE = ZoneInfo("America/Sao_Paulo")

BOND_TYPES = Literal["LTN", "NTN-B", "NTN-C", "NTN-F", "LFT"]

ANBIMA_URL = "https://www.anbima.com.br/informacoes/merc-sec/arqs"
ANBIMA_RTM_HOSTNAME = "www.anbima.associados.rtm"
ANBIMA_RTM_URL = f"http://{ANBIMA_RTM_HOSTNAME}/merc_sec/arqs"
# URL example: https://www.anbima.com.br/informacoes/merc-sec/arqs/ms240614.txt

# Before 13/05/2014 the file was zipped and the endpoint ended with ".exe"
FORMAT_CHANGE_DATE = pd.to_datetime("13-05-2014", dayfirst=True)

logger = logging.getLogger(__name__)


def _bond_type_mapping(bond_type: str) -> str:
    bond_type = bond_type.upper()
    bond_type_mapping = {"NTNB": "NTN-B", "NTNC": "NTN-C", "NTNF": "NTN-F"}
    return bond_type_mapping.get(bond_type, bond_type)


def _build_file_name(date: pd.Timestamp) -> str:
    url_date = date.strftime("%y%m%d")
    if date < FORMAT_CHANGE_DATE:
        file_name = f"ms{url_date}.exe"
    else:
        file_name = f"ms{url_date}.txt"
    return file_name


def _build_file_url(date: pd.Timestamp) -> str:
    last_bday = bday.last_business_day()
    business_days_count = bday.count(date, last_bday)
    if business_days_count > 5:  # noqa
        # For dates older than 5 business days, only the RTM data is available
        logger.info(f"Trying to fetch RTM data for {date.strftime('%d/%m/%Y')}")
        file_url = f"{ANBIMA_RTM_URL}/{_build_file_name(date)}"
    else:
        file_url = f"{ANBIMA_URL}/{_build_file_name(date)}"
    return file_url


@default_retry
def _read_raw_df(file_url: str) -> pd.DataFrame:
    df = pd.read_csv(
        file_url,
        sep="@",
        encoding="latin1",
        skiprows=2,
        decimal=",",
        thousands=".",
        na_values=["--"],
        dtype_backend="pyarrow",
    )
    return df


def _process_raw_df(df_raw: pd.DataFrame) -> pd.DataFrame:
    # Filter selected columns and rename them
    selected_columns_dict = {
        "Titulo": "BondType",
        "Data Referencia": "ReferenceDate",
        "Codigo SELIC": "SelicCode",
        "Data Base/Emissao": "IssueBaseDate",
        "Data Vencimento": "MaturityDate",
        "Tx. Compra": "BidRate",
        "Tx. Venda": "AskRate",
        "Tx. Indicativas": "IndicativeRate",
        "PU": "Price",
        "Desvio padrao": "StdDev",
        "Interv. Ind. Inf. (D0)": "LowerBoundRateD0",
        "Interv. Ind. Sup. (D0)": "UpperBoundRateD0",
        "Interv. Ind. Inf. (D+1)": "LowerBoundRateD1",
        "Interv. Ind. Sup. (D+1)": "UpperBoundRateD1",
        "Criterio": "Criteria",
    }
    df = df_raw.rename(columns=selected_columns_dict)

    # Remove percentage from rates
    rate_cols = [col for col in df.columns if "Rate" in col]
    # Rate columns have percentage values with 4 decimal places in percentage values
    # We will round to 6 decimal places to avoid floating point errors
    df[rate_cols] = (df[rate_cols] / 100).round(6)

    for col in ["ReferenceDate", "MaturityDate", "IssueBaseDate"]:
        df[col] = pd.to_datetime(df[col], format="%Y%m%d").astype("date32[pyarrow]")

    return df


def _process_dv01(df: pd.DataFrame) -> pd.DataFrame:
    df["BDToMat"] = bday.count(
        start=df["ReferenceDate"], end=df["MaturityDate"]
    ).astype("int64[pyarrow]")

    df["Duration"] = 0.0
    df["Duration"] = df["Duration"].astype("float64[pyarrow]")

    df_others = df.query(
        "BondType not in ['LTN', 'NTN-F', 'NTN-B', 'NTN-C']"
    ).reset_index(drop=True)

    df_ltn = df.query("BondType == 'LTN'").reset_index(drop=True)
    if not df_ltn.empty:
        df_ltn["Duration"] = df_ltn["BDToMat"] / 252

    df_ntnf = df.query("BondType == 'NTN-F'").reset_index(drop=True)
    if not df_ntnf.empty:
        df_ntnf["Duration"] = df_ntnf.apply(
            lambda row: duration_f(
                row["ReferenceDate"], row["MaturityDate"], row["IndicativeRate"]
            ),
            axis=1,
        )

    df_ntnb = df.query("BondType == 'NTN-B'").reset_index(drop=True)
    if not df_ntnb.empty:
        df_ntnb["Duration"] = df_ntnb.apply(
            lambda row: duration_b(
                row["ReferenceDate"], row["MaturityDate"], row["IndicativeRate"]
            ),
            axis=1,
        )

    df_ntnc = df.query("BondType == 'NTN-C'").reset_index(drop=True)
    if not df_ntnc.empty:
        df_ntnc["Duration"] = df_ntnc.apply(
            lambda row: duration_c(
                row["ReferenceDate"], row["MaturityDate"], row["IndicativeRate"]
            ),
            axis=1,
        )

    df = pd.concat([df_others, df_ltn, df_ntnf, df_ntnb, df_ntnc])
    df["Duration"] = df["Duration"].astype("float64[pyarrow]")

    mduration = df["Duration"] / (1 + df["IndicativeRate"])
    df["DV01"] = 0.0001 * mduration * df["Price"]

    return df


def _add_usd_dv01(df: pd.DataFrame) -> pd.DataFrame:
    """Add the USD DV01 column to the DataFrame."""
    try:
        reference_date = df["ReferenceDate"].iloc[0]
        ptax_rate = ptax(date=reference_date)
        df["DV01USD"] = df["DV01"] / ptax_rate
    except Exception as e:
        logger.error(f"Error adding USD DV01: {e}")
    return df


def _reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Reorder the columns of the DataFrame according to the specified order."""
    column_order = [
        "BondType",
        "ReferenceDate",
        "SelicCode",
        "IssueBaseDate",
        "MaturityDate",
        "BDToMat",
        "Duration",
        "DV01",
        "DV01USD",
        "Price",
        "BidRate",
        "AskRate",
        "IndicativeRate",
        "StdDev",
        "LowerBoundRateD0",
        "UpperBoundRateD0",
        "LowerBoundRateD1",
        "UpperBoundRateD1",
        "Criteria",
    ]
    column_order = [col for col in column_order if col in df.columns]
    return df[column_order].copy()


def _select_main_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Select only the main columns from the DataFrame."""
    main_columns = [
        "BondType",
        "ReferenceDate",
        "MaturityDate",
        "BDToMat",
        "Duration",
        "DV01",
        "DV01USD",
        "Price",
        "BidRate",
        "AskRate",
        "IndicativeRate",
    ]
    main_columns = [col for col in main_columns if col in df.columns]
    return df[main_columns].copy()


def fetch_tpf_data(
    date: DateScalar,
    bond_type: str | BOND_TYPES | None = None,
    all_columns: bool = True,
) -> pd.DataFrame:
    """Fetch and process TPF secondary market data directly from the ANBIMA website.
    This is a low-level function intended for internal use or specific backend
    jobs. For general use, prefer `tpf_data` which includes caching logic.

    Only the last 5 days of data are available in the ANBIMA website. Data older
    than 5 business days is only available through the RTM network.

    This function retrieves bond market data from the ANBIMA website for a
    specified reference date. It handles different file formats based on the date
    and attempts to download the data from both member and non-member URLs.

    Args:
        date (DateScalar): The reference date for the data.
        bond_type (str, optional):  Filter data by bond type.
            Defaults to None, which returns data for all bond types.
        all_columns (bool, optional):  If True, all columns are returned.
            Defaults to True. If False, only the main columns are returned.

    Returns:
        pd.DataFrame: A DataFrame containing bond market data.
            Returns an empty DataFrame if data is not available for the
            specified date (weekends, holidays, unavailable data, or lack of
            RTM access for older data).
    """
    date = convert_input_dates(date)
    date_log = date.strftime("%d/%m/%Y")
    today = dt.datetime.now(BZ_TIMEZONE).date()
    if date.date() > today:
        logger.info(
            f"Cannot fetch data for a future date ({date_log}). "
            "Returning empty DataFrame."
        )
        return pd.DataFrame()

    file_url = _build_file_url(date)

    # --- "FAIL-FAST" PARA EVITAR RETRIES DESNECESSÁRIOS NA RTM ---
    if ANBIMA_RTM_URL in file_url:
        try:
            # Tenta resolver o hostname da RTM. É uma verificação de rede rápida.
            socket.gethostbyname(ANBIMA_RTM_HOSTNAME)
        except socket.gaierror:
            # Se falhar (gaierror = get address info error), não estamos na RTM.
            # Não adianta prosseguir para a função com retry.
            logger.warning(
                f"Could not resolve RTM host for {date_log}. This is expected if "
                "you are not on the RTM network. Historical data requires RTM access. "
                "Returning empty DataFrame."
            )
            return pd.DataFrame()

    try:
        # Se passamos pela verificação da RTM, agora podemos chamar a função com retry.
        df = _read_raw_df(file_url)
        if df.empty:
            logger.info(
                f"Anbima TPF secondary market data for {date_log} not available. "
                "Returning empty DataFrame."
            )
            return df
        df = _process_raw_df(df)
        df = _process_dv01(df)
        df = _add_usd_dv01(df)
        df = _reorder_columns(df)

        if not all_columns:
            df = _select_main_columns(df)

        if bond_type:
            norm_bond_type = _bond_type_mapping(bond_type)  # noqa
            df = df.query("BondType == @norm_bond_type").reset_index(drop=True)
        return df.sort_values(["BondType", "MaturityDate"]).reset_index(drop=True)

    except HTTPError as e:
        if e.code == 404:  # noqa
            logger.info(
                f"No Anbima TPF secondary market data for {date_log} (HTTP 404). "
                "Returning empty DataFrame."
            )
            return pd.DataFrame()
        logger.error(f"HTTP Error fetching data for {date_log} from {file_url}: {e}")
        raise

    # Este bloco ainda é útil para outros URLErrors (ex: timeout genuíno na URL pública)
    except URLError:
        logger.exception(f"Network error (URLError) fetching TPF data for {date_log}")
        raise

    except Exception:
        msg = f"An unexpected error occurred fetching TPF data for {date_log}"
        logger.exception(msg)
        raise


def tpf_data(
    date: DateScalar,
    bond_type: str | None = None,
    adj_maturities: bool = False,
) -> pd.DataFrame:
    """Retrieve indicative rates for bonds from ANBIMA data.

    This function fetches indicative rates for bonds from ANBIMA,
    initially attempting to retrieve data from a cached dataset. If the data
    is not available in the cache, it fetches it directly from the ANBIMA website.

    Args:
        date (DateScalar): The reference date for the rates.
        bond_type (str, optional): Filter rates by bond type.
            Defaults to None, which returns rates for all bond types.
        adj_maturities (bool, optional): Adjust maturity dates to the next
            business day. Defaults to False.

    Returns:
        pd.DataFrame: A DataFrame with the following columns:
            - BondType: The type of bond.
            - MaturityDate: The maturity date of the bond.
            - IndicativeRate: The indicative rate of the bond.
            - Price: The price (PU) of the bond.

    Examples:
        >>> from pyield import anbima
        >>> anbima.tpf_data(date="22-08-2025")
           ReferenceDate BondType MaturityDate  IndicativeRate         Price
        0     2025-08-22      LFT   2025-09-01        0.000165  17200.957952
        1     2025-08-22      LFT   2026-03-01       -0.000116  17202.058818
        2     2025-08-22      LFT   2026-09-01       -0.000107  17202.901668
        3     2025-08-22      LFT   2027-03-01        0.000302  17193.200289
        4     2025-08-22      LFT   2027-09-01        0.000411  17186.767105
        ...
    """
    df = get_cached_dataset("tpf")
    date = convert_input_dates(date)
    df = df.query("ReferenceDate == @date").reset_index(drop=True)

    if df.empty:
        # Try to fetch the data from the Anbima website
        df = fetch_tpf_data(date)

    if df.empty:
        # If the data is still empty, return an empty DataFrame
        return pd.DataFrame()

    if bond_type:
        df = df.query("BondType == @bond_type").reset_index(drop=True)

    if adj_maturities:
        df["MaturityDate"] = bday.offset(df["MaturityDate"], 0)

    return (
        df[["ReferenceDate", "BondType", "MaturityDate", "IndicativeRate", "Price"]]
        .sort_values(["BondType", "MaturityDate"])
        .reset_index(drop=True)
    )


def tpf_fixed_rate_maturities(date: DateScalar) -> pd.Series:
    """Retrieve pre-defined maturity dates for LTN and NTN-F bonds.

    This function fetches pre-defined maturity dates for 'LTN' (prefixadas) and
    'NTN-F' (indexadas ao CDI) bond types from the cached ANBIMA dataset
    for a given reference date.

    Args:
        date (DateScalar): The reference date for maturity dates.

    Returns:
        pd.Series: A Series containing unique maturity dates for 'LTN' and
            'NTN-F' bonds, sorted in ascending order.
    """
    maturity_dates = (
        tpf_data(date)
        .query("BondType in ['LTN', 'NTN-F']")["MaturityDate"]
        .drop_duplicates()
        .sort_values(ignore_index=True)
        .reset_index(drop=True)
    )
    return maturity_dates
