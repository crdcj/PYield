import datetime as dt
import logging
import socket
from typing import Literal
from urllib.error import HTTPError, URLError
from zoneinfo import ZoneInfo

import pandas as pd

from pyield import bday
from pyield.b3 import di1
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


def _add_dv01(df: pd.DataFrame) -> pd.DataFrame:
    df["BDToMat"] = bday.count(start=df["ReferenceDate"], end=df["MaturityDate"])

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


def _add_di_rate(df: pd.DataFrame) -> pd.DataFrame:
    """Add the DI rate column to the DataFrame."""
    # INSERIR OS DADOS DO DI INTERPOLADO ###
    reference_date = df["ReferenceDate"].iloc[0]
    df["DIRate"] = di1.interpolate_rates(
        dates=reference_date,
        expirations=df["MaturityDate"],
        extrapolate=True,
    )
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
        "DIRate",
        "StdDev",
        "LowerBoundRateD0",
        "UpperBoundRateD0",
        "LowerBoundRateD1",
        "UpperBoundRateD1",
        "Criteria",
    ]
    column_order = [col for col in column_order if col in df.columns]
    return df[column_order].copy()


def _fetch_tpf_data(date: pd.Timestamp) -> pd.DataFrame:
    """Busca e processa dados do mercado secundário de TPF diretamente da fonte ANBIMA.

    Esta é uma função de baixo nível para uso interno. Ela lida com a lógica
    de construir a URL correta (pública ou RTM), baixar os dados com novas
    tentativas e processá-los em um DataFrame estruturado.

    Args:
        date (pd.Timestamp): A data de referência para os dados.

    Returns:
        pd.DataFrame: Um DataFrame contendo os dados de mercado de títulos
            processados, ou um DataFrame vazio se os dados não estiverem
            disponíveis ou ocorrer um erro de conexão.
    """
    file_url = _build_file_url(date)
    date_str = date.strftime("%d/%m/%Y")

    # --- "FAIL-FAST" PARA EVITAR RETRIES DESNECESSÁRIOS NA RTM ---
    if ANBIMA_RTM_URL in file_url:
        try:
            # Tenta resolver o hostname da RTM. É uma verificação de rede rápida.
            socket.gethostbyname(ANBIMA_RTM_HOSTNAME)
        except socket.gaierror:
            # Se falhar (gaierror = get address info error), não estamos na RTM.
            # Não adianta prosseguir para a função com retry.
            logger.warning(
                f"Could not resolve RTM host for {date_str}. This is expected if "
                "you are not on the RTM network. Historical data requires RTM access. "
                "Returning empty DataFrame."
            )
            return pd.DataFrame()

    try:
        # Se passamos pela verificação da RTM, agora podemos chamar a função com retry.
        df = _read_raw_df(file_url)
        if df.empty:
            logger.info(
                f"Anbima TPF secondary market data for {date_str} not available. "
                "Returning empty DataFrame."
            )
            return df
        df = _process_raw_df(df)
        df = _add_dv01(df)
        df = _add_usd_dv01(df)
        df = _add_di_rate(df)
        df = _reorder_columns(df)

        return df.sort_values(["BondType", "MaturityDate"]).reset_index(drop=True)

    except HTTPError as e:
        if e.code == 404:  # noqa
            logger.info(
                f"No Anbima TPF secondary market data for {date_str} (HTTP 404). "
                "Returning empty DataFrame."
            )
            return pd.DataFrame()
        logger.error(f"HTTP Error fetching data for {date_str} from {file_url}: {e}")
        raise

    # Este bloco ainda é útil para outros URLErrors (ex: timeout genuíno na URL pública)
    except URLError:
        logger.exception(f"Network error (URLError) fetching TPF data for {date_str}")
        raise

    except Exception:
        msg = f"An unexpected error occurred fetching TPF data for {date_str}"
        logger.exception(msg)
        raise


def tpf_data(
    date: DateScalar,
    bond_type: str | None = None,
    fetch_from_source: bool = False,
) -> pd.DataFrame:
    """Recupera os dados do mercado secundário de TPF da ANBIMA.

    Esta função busca taxas indicativas e outros dados de títulos públicos
    brasileiros. A obtenção dos dados segue uma hierarquia de fontes para
    otimizar o desempenho e o acesso.

    Args:
        date (DateScalar): A data de referência para os dados (ex: '2024-06-14').
        bond_type (str, optional): Filtra os resultados por um tipo de título
            específico (ex: 'LTN', 'NTN-B'). Por padrão, retorna todos os tipos.
        fetch_from_source (bool, optional): Se True, força a função a ignorar o
            cache e buscar os dados diretamente da fonte (ANBIMA).
            Padrão é False.

    Returns:
        pd.DataFrame: Um DataFrame contendo os dados solicitados. Retorna um
        DataFrame vazio se não houver dados para a data especificada (ex:
        finais de semana, feriados ou datas futuras).

    Examples:
        >>> from pyield import anbima
        >>> anbima.tpf_data(date="22-08-2025")
           ReferenceDate BondType  SelicCode  ...   AskRate IndicativeRate    DIRate
        0     2025-08-22      LFT     210100  ...    0.0001       0.000165   0.14906
        1     2025-08-22      LFT     210100  ... -0.000156      -0.000116   0.14843
        2     2025-08-22      LFT     210100  ... -0.000143      -0.000107    0.1436
        3     2025-08-22      LFT     210100  ...  0.000292       0.000302  0.138189
        ...


    Data columns:
        - BondType: Tipo do título público (e.g., 'LTN', 'NTN-B').
        - ReferenceDate: Data de referência dos dados.
        - SelicCode: Código do título no SELIC.
        - IssueBaseDate: Data base ou de emissão do título.
        - MaturityDate: Data de vencimento do título.
        - BDToMat: Número de dias úteis entre a data de referência e o vencimento.
        - Duration: Macaulay Duration do título em anos.
        - DV01: Variação financeira no preço do título (em BRL) para uma
            mudança de 1 basis point (0,01%) na taxa de juros.
        - DV01USD: O mesmo que DV01, mas convertido para USD pela PTAX do dia.
        - Price: Preço Unitário (PU) do título na data de referência.
        - BidRate: Taxa de compra em formato decimal (e.g., 0.10 para 10%).
        - AskRate: Taxa de venda em formato decimal.
        - IndicativeRate: Taxa indicativa em formato decimal.
        - DIRate: Taxa DI interpolada (flatforward) no vencimento do título.
        - StdDev: Desvio padrão da taxa indicativa.
        - LowerBoundRateD0: Limite inferior do intervalo indicativo para D+0.
        - UpperBoundRateD0: Limite superior do intervalo indicativo para D+0.
        - LowerBoundRateD1: Limite inferior do intervalo indicativo para D+1.
        - UpperBoundRateD1: Limite superior do intervalo indicativo para D+1.
        - Criteria: Critério utilizado pela ANBIMA para o cálculo.

    Notes:
        A fonte dos dados segue a seguinte hierarquia:

        1.  **Cache Local (Padrão):** Fornece acesso rápido a dados históricos
            desde 01/01/2020. É utilizado por padrão (`fetch_from_source=False`).
        2.  **Site Público da ANBIMA:** Acessado quando `fetch_from_source=True`,
            disponibiliza os dados dos últimos 5 dias úteis.
        3.  **Rede RTM da ANBIMA:** Acessada quando `fetch_from_source=True` para
            datas com mais de 5 dias úteis. O acesso ao histórico completo
            requer uma conexão à rede RTM. Sem ela, a consulta para datas
            antigas retornará um DataFrame vazio.
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

    if fetch_from_source:
        # Try to fetch the data from the Anbima website
        df = _fetch_tpf_data(date)
    else:
        df = (
            get_cached_dataset("tpf")
            .query("ReferenceDate == @date")
            .reset_index(drop=True)
        )

    if df.empty:
        # If the data is still empty, return an empty DataFrame
        return pd.DataFrame()

    if bond_type:
        norm_bond_type = _bond_type_mapping(bond_type)  # noqa
        df = df.query("BondType == @norm_bond_type").reset_index(drop=True)

    return df.sort_values(["BondType", "MaturityDate"]).reset_index(drop=True)


def tpf_fixed_rate_maturities(date: DateScalar) -> pd.Series:
    """Retrieve existing maturity dates for fixed-rate public bonds (LTN and NTN-F).

    This function fetches existing maturity dates for 'LTN' and
    'NTN-F' bond types for a given date.

    Args:
        date (DateScalar): The reference date for maturity dates.

    Returns:
        pd.Series: A Series containing unique maturity dates for 'LTN' and
            'NTN-F' bonds, sorted in ascending order.

    Examples:
        >>> from pyield import anbima
        >>> anbima.tpf_fixed_rate_maturities(date="22-08-2025")
        0     2025-10-01
        1     2026-01-01
        2     2026-04-01
        3     2026-07-01
                ...
        14    2031-01-01
        15    2032-01-01
        16    2033-01-01
        17    2035-01-01
        Name: MaturityDate, dtype: date32[day][pyarrow]
    """
    maturity_dates = (
        tpf_data(date)
        .query("BondType in ['LTN', 'NTN-F']")["MaturityDate"]
        .drop_duplicates()
        .sort_values(ignore_index=True)
        .reset_index(drop=True)
    )
    return maturity_dates
