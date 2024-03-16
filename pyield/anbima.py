import pandas as pd
from urllib.error import HTTPError

from . import di_futures as di


def process_reference_date(
    reference_date: str | pd.Timestamp | None = None,
) -> pd.Timestamp:
    if reference_date:
        # Force reference_date to be a pd.Timestamp
        reference_date = pd.Timestamp(reference_date)
    else:  # If no reference_date is given, use yesterday's date
        today = pd.Timestamp.today().date()
        yesterday = today - pd.Timedelta(days=1)
        reference_date = yesterday

    return reference_date


def get_raw_data(reference_date: pd.Timestamp, anbima_member: bool) -> pd.DataFrame:
    """
    Fetch indicative rates from ANBIMA for a specific date.

    Parameters:
    - reference_date: pd.Timestamp for which to fetch the indicative rates.
        If None, fetches yesterday's rates.

    Returns:
    - A pandas DataFrame with the indicative rates for the given date.
    """

    # Example URL: https://www.anbima.com.br/informacoes/merc-sec/arqs/ms231128.txt
    url_date = reference_date.strftime("%y%m%d")
    if anbima_member:
        url = f"http://www.anbima.associados.rtm/merc_sec/arqs/ms{url_date}.txt"
    else:
        url = f"https://www.anbima.com.br/informacoes/merc-sec/arqs/ms{url_date}.txt"

    try:
        df = pd.read_csv(
            url,
            sep="@",
            encoding="latin-1",
            skiprows=2,
            decimal=",",
            thousands=".",
            dtype_backend="numpy_nullable",
        )
    except HTTPError:
        error_date = reference_date.strftime("%d-%m-%Y")
        raise ValueError(f"Failed to get ANBIMA rates for {error_date}")

    return df


def process_raw_data(df: pd.DataFrame) -> pd.DataFrame:
    # Filter selected columns and rename them
    selected_columns_dict = {
        "Titulo": "BondType",
        "Data Referencia": "ReferenceDate",
        # "Codigo SELIC": "SelicCode",
        # "Data Base/Emissao": "IssueBaseDate",
        "Data Vencimento": "MaturityDate",
        "Tx. Compra": "BidRate",
        "Tx. Venda": "AskRate",
        "Tx. Indicativas": "IndicativeRate",
        "PU": "Price",
        # "Desvio padrao": "StdDev",
        # "Interv. Ind. Inf. (D0)",
        # "Interv. Ind. Sup. (D0)",
        # "Interv. Ind. Inf. (D+1)",
        # "Interv. Ind. Sup. (D+1)",
        # "Criterio": "Criteria",
    }
    select_columns = list(selected_columns_dict.keys())
    df = df[select_columns].copy()
    df.rename(columns=selected_columns_dict, inplace=True)

    # Remove percentage from rates
    rate_cols = ["BidRate", "AskRate", "IndicativeRate"]
    df[rate_cols] = df[rate_cols] / 100

    df["ReferenceDate"] = pd.to_datetime(df["ReferenceDate"], format="%Y%m%d")
    df["MaturityDate"] = pd.to_datetime(df["MaturityDate"], format="%Y%m%d")

    return df.sort_values(["BondType", "MaturityDate"], ignore_index=True)


def get_anbima_rates(
    reference_date: str | pd.Timestamp | None = None,
    return_raw=False,
    anbima_member=False,
) -> pd.DataFrame:
    """
    Fetch indicative rates from ANBIMA for a specific date.

    Parameters:
    - reference_date: pd.Timestamp for which to fetch the indicative rates.
        If None, fetches yesterday's rates.

    Returns:
    - A pandas DataFrame with the indicative rates for the given date.
    """
    reference_date = process_reference_date(reference_date)
    df = get_raw_data(reference_date, anbima_member)

    if return_raw:
        return df
    else:
        return process_raw_data(df)


def get_anbima_di_spreads(
    reference_date: str | pd.Timestamp | None = None,
    anbima_member=False,
) -> pd.DataFrame:
    """
    Calcula o prêmio (spread) com relação ao DI nas taxas indicativas da ANBIMA para os títulos LTN e NTN-F.
    A coluna DISpread contém o prêmio em bps.

    Parâmetros:
    -----------
    data_consulta : pd.Timestamp
        A data de referência para a consulta das taxas indicativas da ANBIMA.
    """
    reference_date = process_reference_date(reference_date)

    df_di = di.get_di(reference_date)[["ExpirationDate", "SettlementRate"]]

    df_di.rename(columns={"ExpirationDate": "MaturityDate"}, inplace=True)

    # Ajustar o vencimento para o primeiro dia do mês para concidir com o formato dos títulos
    df_di["MaturityDate"] = df_di["MaturityDate"].dt.to_period("M").dt.to_timestamp()

    df_anb = get_anbima_rates(
        reference_date=reference_date, anbima_member=anbima_member
    )

    # Filtrar apenas os títulos LTN e NTN-F
    df_anb.query("BondType in ['LTN', 'NTN-F']", inplace=True)

    # Unir os dois DataFrames
    df = pd.merge(df_anb, df_di, how="left", on="MaturityDate")

    # Calcular o prêmio implícito na taxa da ANBIMA
    df["DISpread"] = df["IndicativeRate"] - df["SettlementRate"]
    df.drop(columns="SettlementRate", inplace=True)

    # Converter o prêmio para bps e arredondar para 2 casas decimais
    df["DISpread"] = (10_000 * df["DISpread"]).round(2)

    select_columns = ["BondType", "ReferenceDate", "MaturityDate", "DISpread"]
    df = df[select_columns]
    return df.sort_values(["BondType", "MaturityDate"], ignore_index=True)
