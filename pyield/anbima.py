import pandas as pd
from urllib.error import HTTPError

from . import di_futures as di


def calculate_di_spreads(df, reference_date: pd.Timestamp) -> pd.DataFrame:
    """
    Calcula o prêmio implícito nas taxas indicativas da ANBIMA para os títulos LTN e NTN-F.

    Parâmetros:
    -----------
    data_consulta : pd.Timestamp
        A data de referência para a consulta das taxas indicativas da ANBIMA.
    """

    df_di = di.get_di(reference_date)[["MaturityDate", "SettlementRate"]]

    df_di.rename(columns={"ExpirationDate": "MaturityDate"}, inplace=True)

    # Ajustar o vencimento para o primeiro dia do mês para concidir com o formato dos títulos
    df_di["MaturityDate"] = df_di["MaturityDate"].dt.to_period("M").dt.to_timestamp()

    # Unir os dois DataFrames
    df = pd.merge(df, df_di, how="left", on="MaturityDate")

    # Calcular o prêmio implícito na taxa da ANBIMA
    df["DISpread"] = df["IndicativeRate"] - df["SettlementRate"]

    # Converter o prêmio para bps e arredondar para 2 casas decimais
    df["DISpread"] = (10_000 * df["DISpread"]).round(2)

    # Make not fixed rate bonds have NaN spread
    fixed_rate_bonds_mask = df["BondType"].isin(["LTN", "NTN-F"])
    df.loc[~fixed_rate_bonds_mask, "DISpread"] = pd.NA

    return df


def get_anbima_rates(
    reference_date: str | pd.Timestamp | None = None, return_raw=False
) -> pd.DataFrame:
    """
    Fetch indicative rates from ANBIMA for a specific date.

    Parameters:
    - reference_date: pd.Timestamp for which to fetch the indicative rates.
        If None, fetches yesterday's rates.

    Returns:
    - A pandas DataFrame with the indicative rates for the given date.
    """
    if reference_date:
        # Force reference_date to be a pd.Timestamp
        reference_date = pd.Timestamp(reference_date)
    else:  # If no reference_date is given, use yesterday's date
        today = pd.Timestamp.today().date()
        yesterday = today - pd.Timedelta(days=1)
        reference_date = yesterday

    # Example URL: https://www.anbima.com.br/informacoes/merc-sec/arqs/ms231128.txt
    url_date = reference_date.strftime("%y%m%d")
    url = f"https://www.anbima.com.br/informacoes/merc-sec/arqs/ms{url_date}.txt"

    try:
        df = pd.read_csv(
            url, sep="@", encoding="latin1", skiprows=2, decimal=",", thousands="."
        )
    except HTTPError:
        error_date = reference_date.strftime("%d/%m/%Y")
        raise ValueError(f"Failed to get ANBIMA rates for {error_date}")

    if return_raw:
        return df

    # Filter selected columns and rename them
    selected_columns_dict = {
        "Titulo": "BondType",
        "Data Referencia": "ReferenceDate",
        # "Codigo SELIC",
        # "Data Base/Emissao",
        "Data Vencimento": "MaturityDate",
        "Tx. Compra": "BidRate",
        "Tx. Venda": "AskRate",
        "Tx. Indicativas": "IndicativeRate",
        "PU": "Price",
        # "Desvio padrao",
        # "Interv. Ind. Inf. (D0)",
        # "Interv. Ind. Sup. (D0)",
        # "Interv. Ind. Inf. (D+1)",
        # "Interv. Ind. Sup. (D+1)",
        # "Criterio",
    }
    df = df[list(selected_columns_dict.keys())]
    df.rename(columns=selected_columns_dict, inplace=True)
    df["ReferenceDate"] = pd.to_datetime(df["ReferenceDate"], format="%Y%m%d")
    df["MaturityDate"] = pd.to_datetime(df["MaturityDate"], format="%Y%m%d")

    return df.sort_values(["BondType", "MaturityDate"], ignore_index=True)
