import datetime as dt
import io
import logging

import polars as pl
import requests

from pyield import bday
from pyield.fwd import forwards
from pyield.retry import default_retry

# Lista de contratos que negociam por TAXA (Juros/Cupom)
# Nestes contratos, as colunas OHLC são taxas e precisam ser divididas por 100.
RATE_BASED_CONTRACTS = {"DI1", "DAP", "DDI", "FRC", "FRO", "DAP"}
COLUMN_CONFIG = {
    "Instrumento financeiro": (pl.String, "TickerSymbol"),
    "Código ISIN": (pl.String, "ISINCode"),
    "Segmento": (pl.String, "Segment"),
    # Lemos como "Value" genérico, pois pode ser Taxa ou Preço
    "Preço de abertura": (pl.Float64, "OpenValue"),
    "Preço mínimo": (pl.Float64, "MinValue"),
    "Preço máximo": (pl.Float64, "MaxValue"),
    "Preço médio": (pl.Float64, "AvgValue"),
    "Preço de fechamento": (pl.Float64, "CloseValue"),
    "Preço de referência": (pl.Float64, "ReferencePrice"),
    "Última oferta de compra": (pl.Float64, "LastBidValue"),
    "Última oferta de venda": (pl.Float64, "LastAskValue"),
    "Oscilação": (pl.Float64, "Oscillation"),
    "Ajuste": (pl.Float64, "SettlementPrice"),
    # "Ajuste de referência" = Taxa (apenas para derivativos de juros)
    "Ajuste de referência": (pl.Float64, "SettlementRate"),
    "Variação": (pl.Float64, "Variation"),
    "Valor do ajuste por contrato (R$)": (pl.Float64, "AdjustmentValuePerContract"),
    "Quantidade de negócios": (pl.Int64, "TradeCount"),
    "Quantidade de contratos": (pl.Int64, "OpenContracts"),
    "Volume financeiro": (pl.Float64, "FinancialVolume"),
}

CSV_SCHEMA = {k: v[0] for k, v in COLUMN_CONFIG.items()}
RENAME_MAP = {k: v[1] for k, v in COLUMN_CONFIG.items()}

logger = logging.getLogger(__name__)


def add_expiration_date(df: pl.DataFrame, code_column: str, day: int = 1) -> pl.Series:
    month_map = {
        "F": 1,
        "G": 2,
        "H": 3,
        "J": 4,
        "K": 5,
        "M": 6,
        "N": 7,
        "Q": 8,
        "U": 9,
        "V": 10,
        "X": 11,
        "Z": 12,
    }
    df = df.with_columns(
        pl.date(
            # Ano: Pega os 2 últimos dígitos -> Int -> Soma 2000
            year=pl.col(code_column).str.slice(-2).cast(pl.Int32, strict=False) + 2000,
            # Mês: Pega 1ª letra -> Mapeia -> Int
            month=pl.col(code_column)
            .str.slice(0, 1)
            .replace_strict(month_map, default=None, return_dtype=pl.Int8),
            day=day,
        ).alias("ExpirationDate")
    )
    # Ajusta para dia útil, se necessário
    adj_dates = bday.offset(dates=df["ExpirationDate"], offset=0)
    df = df.with_columns(ExpirationDate=adj_dates)
    return df


@default_retry
def _fetch_csv_data(date: dt.date) -> str:
    url = "https://arquivos.b3.com.br/bdi/table/export/csv"
    params = {"lang": "pt-BR"}
    date_str = date.strftime("%Y-%m-%d")
    payload = {
        "Name": "ConsolidatedTradesDerivatives",
        "Date": date_str,
        "FinalDate": date_str,
        "ClientId": "",
        "Filters": {},
    }
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",  # noqa E501
        "Accept": "application/json, text/plain, */*",
    }

    response = requests.post(
        url, params=params, json=payload, headers=headers, timeout=(5, 30)
    )
    response.raise_for_status()
    return response.text


def _parse_raw_df(csv_data: str) -> pl.DataFrame:
    df = pl.read_csv(
        io.StringIO(csv_data.replace(".", "")),
        separator=";",
        skip_lines=2,
        null_values=["-"],
        decimal_comma=True,
        schema=CSV_SCHEMA,
    )
    return df


def _pre_process_df(df: pl.DataFrame, contract_code: str) -> pl.DataFrame:
    # Remove rows and columns where all values are null
    df = (
        df.rename(RENAME_MAP, strict=False)
        .filter(
            pl.col("TickerSymbol").str.contains(contract_code),
            pl.col("TickerSymbol").str.len_chars() == 6,  # noqa
        )
        .with_columns(ExpirationCode=pl.col("TickerSymbol").str.slice(3))
    )

    return df


def _process_df(
    df: pl.DataFrame, trade_date: dt.date, contract_code: str
) -> pl.DataFrame:
    # 1. Datas de Vencimento

    bdays_to_exp = bday.count(trade_date, df["ExpirationDate"])

    df = df.with_columns(
        BDaysToExp=bdays_to_exp,
        DaysToExp=(df["ExpirationDate"] - pl.lit(trade_date)).dt.total_days(),
        TradeDate=trade_date,
    ).filter(pl.col("DaysToExp") > 0)

    # 2. Renomeação Dinâmica (Rate vs Price)
    # Se for contrato de taxa, as colunas "Value" viram "Rate"
    # Se for contrato de preço, as colunas "Value" viram "Price"
    is_rate_based = contract_code in RATE_BASED_CONTRACTS
    target_suffix = "Rate" if is_rate_based else "Price"

    cols_to_rename = [c for c in df.columns if c.endswith("Value")]
    rename_dict = {c: c.replace("Value", target_suffix) for c in cols_to_rename}
    df = df.rename(rename_dict)

    # 3. Tratamento Específico de Taxas
    if is_rate_based:
        # Pega todas as colunas que agora terminam em "Rate" (incluindo SettlementRate)
        rate_cols = [c for c in df.columns if c.endswith("Rate")]

        # Divide por 100 para transformar percentual em decimal (14.50 -> 0.1450)
        df = df.with_columns((pl.col(c) / 100).round(6) for c in rate_cols)

    # 4. Cálculo do DV01 (Apenas para DI1 e se tivermos as colunas necessárias)
    # SettlementPrice aqui já é o PU vindo do CSV (Ex: 99.000)
    # SettlementRate aqui já é a taxa decimal (Ex: 0.14)
    if (
        contract_code == "DI1"
        and "SettlementPrice" in df.columns
        and "SettlementRate" in df.columns
    ):
        # DV01 = (Duration / (1 + Taxa)) * PU * 0.0001
        # Duration Modificada * PU * 1bp
        duration = pl.col("BDaysToExp") / 252
        m_duration = duration / (1 + pl.col("SettlementRate"))
        df = df.with_columns(DV01=(m_duration * pl.col("SettlementPrice") * 0.0001))

    # 5. Forward Rates (Para DI1 e DAP)
    if contract_code in {"DI1", "DAP"} and "SettlementRate" in df.columns:
        # Assume que forwards aceita taxa decimal e dias úteis
        df = df.with_columns(
            ForwardRate=forwards(bdays=df["BDaysToExp"], rates=df["SettlementRate"])
        )

    return df


def _select_and_reorder_columns(df: pl.DataFrame) -> pl.DataFrame:
    # Define a ordem preferida, mas só seleciona o que existe no DF
    preferred_order = [
        "TradeDate",
        "TickerSymbol",
        "ExpirationDate",
        "BDaysToExp",
        # Colunas de Preço (vão existir para DOL, IND, etc)
        "OpenPrice",
        "MinPrice",
        "MaxPrice",
        "AvgPrice",
        "ClosePrice",
        "SettlementPrice",
        # Colunas de Taxa (vão existir para DI1, DAP, etc)
        "OpenRate",
        "MinRate",
        "MaxRate",
        "AvgRate",
        "CloseRate",
        "SettlementRate",
        # Métricas Calculadas e Volume
        "DV01",
        "ForwardRate",
        "OpenContracts",
        "FinancialVolume",
    ]

    existing_cols = [c for c in preferred_order if c in df.columns]
    return df.select(existing_cols)


def fetch_bmf_data(contract_code: str, date: dt.date) -> pl.DataFrame:
    """
    Fetchs the futures data for a given date from B3.

    This function fetches and processes the futures data from B3 for a specific
    trade date. It's the primary external interface for accessing futures data.

    Args:
        asset_code (str): The asset code to fetch the futures data.
        date (dt.date): The trade date to fetch the futures data.
        count_convention (int): The count convention for the DI futures contract.
            Can be 252 business days or 360 calendar days.

    Returns:
        pl.DataFrame: Processed futures data. If no data is found,
            returns an empty DataFrame.
    """
    csv_text = _fetch_csv_data(date)
    if not csv_text:
        return pl.DataFrame()
    df = _parse_raw_df(csv_text)
    df = _pre_process_df(df, contract_code)
    if df.is_empty():
        return pl.DataFrame()

    day = 15 if contract_code == "DAP" else 1
    df = add_expiration_date(df, code_column="TickerSymbol", day=day)

    df = _process_df(df, date, contract_code)
    df = _select_and_reorder_columns(df)
    return df
