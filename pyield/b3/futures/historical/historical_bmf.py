import datetime as dt
import logging

import polars as pl
import polars.selectors as cs
import requests
from lxml import html

from pyield import bday
from pyield.b3.common import add_expiration_date
from pyield.fwd import forwards
from pyield.retry import default_retry

logger = logging.getLogger(__name__)

# --- CONFIGURAÇÃO DE CONTRATOS ---
RATE_CONTRACTS = {"DI1", "DAP", "DDI", "FRC", "FRO"}

COUNT_CONVENTIONS = {"DAP": 252, "DI1": 252, "DDI": 360}
BDAYS_PER_YEAR = 252
CDAYS_PER_YEAR = 360

# --- Mapeamento de Colunas HTML ---

# 1. Colunas FIXAS (Nomes de destino constantes)
BASE_MAPPING = {
    "VENCTO": ("ExpirationCode", pl.Utf8),
    "CONTR. ABERT.(1)": ("OpenContracts", pl.Int64),
    "CONTR. FECH.(2)": ("OpenContractsEndSession", pl.Int64),
    "NÚM. NEGOC.": ("TradeCount", pl.Int64),
    "CONTR. NEGOC.": ("TradeVolume", pl.Int64),
    "VOL.": ("FinancialVolume", pl.Int64),
    "AJUSTE ANTER. (3)": ("PrevSettlementPrice", pl.Float64),
    "AJUSTE CORRIG. (4)": ("AdjSettlementPrice", pl.Float64),
    "AJUSTE": ("SettlementPrice", pl.Float64),  # Sempre Preço
    "AJUSTE\n       DE REF.": ("SettlementRate", pl.Float64),  # Somente FRC
    "VAR. PTOS.": ("PointsVariation", pl.Float64),
}

# 2. Colunas VARIÁVEIS (Sufixo Rate ou Price dependendo do contrato)
VARIABLE_MAPPING = {
    "PREÇO ABERTU.": ("Open", pl.Float64),
    "PREÇO MÍN.": ("Min", pl.Float64),
    "PREÇO MÁX.": ("Max", pl.Float64),
    "PREÇO MÉD.": ("Avg", pl.Float64),
    "ÚLT. PREÇO": ("Close", pl.Float64),
    "ÚLT.OF. COMPRA": ("CloseAsk", pl.Float64),
    "ÚLT.OF. VENDA": ("CloseBid", pl.Float64),
}

# Tipagem para o casting (mapeia o nome FINAL para o Tipo)
# Criamos um dicionário que contém as duas versões (Rate e Price) para o casting.
FINAL_COLUMN_TYPES = {v[0]: v[1] for v in BASE_MAPPING.values()}
for prefix, dtype in VARIABLE_MAPPING.values():
    FINAL_COLUMN_TYPES[f"{prefix}Rate"] = dtype
    FINAL_COLUMN_TYPES[f"{prefix}Price"] = dtype

OLD_MONTH_CODES = {
    "JAN": 1,
    "FEV": 2,
    "MAR": 3,
    "ABR": 4,
    "MAI": 5,
    "JUN": 6,
    "JUL": 7,
    "AGO": 8,
    "SET": 9,
    "OUT": 10,
    "NOV": 11,
    "DEZ": 12,
}

OUTPUT_COLUMNS = [
    "TradeDate",
    "TickerSymbol",
    "ExpirationDate",
    "BDaysToExp",
    "DaysToExp",
    "OpenContracts",
    "TradeCount",
    "TradeVolume",
    "FinancialVolume",
    "DV01",
    "SettlementPrice",
    "OpenRate",
    "OpenPrice",
    "MinRate",
    "MinPrice",
    "AvgRate",
    "AvgPrice",
    "MaxRate",
    "MaxPrice",
    "CloseAskRate",
    "CloseAskPrice",
    "CloseBidRate",
    "CloseBidPrice",
    "CloseRate",
    "ClosePrice",
    "SettlementRate",
    "ForwardRate",
]


def _get_column_rename_map(contract_code: str) -> dict[str, str]:
    """Gera o mapa de renomeação dinâmico baseado no tipo de contrato."""
    suffix = "Rate" if contract_code in RATE_CONTRACTS else "Price"

    # Mapeia base
    rename_map = {k: v[0] for k, v in BASE_MAPPING.items()}
    # Mapeia variáveis com o sufixo correto
    for html_name, (prefix, _) in VARIABLE_MAPPING.items():
        rename_map[html_name] = f"{prefix}{suffix}"

    return rename_map


def _calculate_legacy_expiration_date(
    date: dt.date, expiration_code: str
) -> dt.date | None:
    try:
        month = OLD_MONTH_CODES[expiration_code[:3]]
        year_digit = int(expiration_code[-1])
        year = date.year // 10 * 10 + year_digit
        if year < date.year:
            year += 10
        expiration_date = dt.date(year, month, 1)
        return bday.offset(dates=expiration_date, offset=0)
    except (KeyError, ValueError):
        return None


def _convert_prices_to_rates(
    prices: pl.Series,
    days_to_expiration: pl.Series,
    count_convention: int,
) -> pl.Series:
    if count_convention == CDAYS_PER_YEAR:
        rates = (100_000 / prices - 1) * (CDAYS_PER_YEAR / days_to_expiration)
    else:  # 252
        rates = (100_000 / prices) ** (BDAYS_PER_YEAR / days_to_expiration) - 1
    return rates.round(5)


@default_retry
def _fetch_html_data(date: dt.date, contract_code: str) -> str:
    url_date = date.strftime("%d/%m/%Y")
    url_base = "https://www2.bmf.com.br/pages/portal/bmfbovespa/boletim1/SistemaPregao_excel1.asp"
    params = {"Data": url_date, "Mercadoria": contract_code, "XLS": "true"}
    r = requests.get(url_base, params=params, timeout=10)
    r.raise_for_status()
    r.encoding = "iso-8859-1"
    return r.text


def _parse_html_lxml(html_text: str) -> pl.DataFrame:
    if not html_text:
        return pl.DataFrame()
    tree = html.fromstring(html_text)
    header_rows = tree.xpath('(//tr[@class="tabelaSubTitulo"])[1]')
    if not header_rows:
        return pl.DataFrame()
    first_header_row = header_rows[0]  # type: ignore
    header_cells = first_header_row.xpath(".//th | .//td")  # type: ignore
    col_names = [cell.text_content().strip() for cell in header_cells]  # type: ignore
    if "VENCTO" not in col_names:
        return pl.DataFrame()
    table_container = first_header_row.getparent()  # type: ignore
    rows = table_container.xpath(  # type: ignore
        './/tr[@class="tabelaConteudo1" or @class="tabelaConteudo2"]'
    )
    data = []
    for row in rows:  # type: ignore
        cells = row.xpath(".//td")
        clean_cells = [cell.text_content().strip() for cell in cells]
        if len(clean_cells) == len(col_names):
            data.append(clean_cells)
    return pl.DataFrame(data, schema=col_names, orient="row")


def _clean_string_values(df: pl.DataFrame) -> pl.DataFrame:
    if "PointsVariation" in df.columns:
        df = df.with_columns(
            pl.when(pl.col("PointsVariation").str.ends_with("-"))
            .then("-" + pl.col("PointsVariation").str.replace("-", "", literal=True))
            .otherwise(pl.col("PointsVariation").str.replace("+", "", literal=True))
            .alias("PointsVariation")
        )
    df = df.select(
        pl.all()
        .str.strip_chars()
        .str.replace_all(".", "", literal=True)
        .str.replace(",", ".")
        .replace("-", "")
    )
    return df


def _cast_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Realiza o casting baseado nos nomes de colunas já traduzidos."""
    # Filtra apenas os tipos das colunas que realmente existem no DF atual
    types_to_apply = {k: v for k, v in FINAL_COLUMN_TYPES.items() if k in df.columns}
    return df.cast(types_to_apply, strict=False)


def _add_expiration_dates(
    df: pl.DataFrame, date: dt.date, contract_code: str
) -> pl.DataFrame:
    df = df.with_columns(
        TradeDate=date,
        TickerSymbol=contract_code + pl.col("ExpirationCode"),
    )
    if date < dt.date(2006, 5, 22):
        exp_dates = [
            _calculate_legacy_expiration_date(date, exp_code)
            for exp_code in df["ExpirationCode"]
        ]
        df = df.with_columns(pl.Series("ExpirationDate", exp_dates))
    else:
        df = add_expiration_date(df, contract_code, "TickerSymbol")

    df = df.with_columns(
        BDaysToExp=bday.count(date, df["ExpirationDate"]),
        DaysToExp=(pl.col("ExpirationDate") - pl.col("TradeDate")).dt.total_days(),
    ).filter(pl.col("DaysToExp") > 0)
    return df


def _convert_zeros_to_null(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        (cs.contains("Rate") | cs.contains("Price")).replace(0, None)
    )


def _adjust_legacy_di1_rates(df: pl.DataFrame, rate_cols: list) -> pl.DataFrame:
    for col in rate_cols:
        rate_col = _convert_prices_to_rates(df[col], df["BDaysToExp"], BDAYS_PER_YEAR)
        df = df.with_columns(rate_col.alias(col))
    if {"MinRate", "MaxRate"}.issubset(set(rate_cols)):
        df = df.with_columns(MinRate=pl.col("MaxRate"), MaxRate=pl.col("MinRate"))
    return df


def _transform_rates(
    df: pl.DataFrame, date: dt.date, contract_code: str
) -> pl.DataFrame:
    # Seleciona apenas o que terminou com "Rate"
    rate_cols = [c for c in df.columns if "Rate" in c]

    switch_date = dt.date(2002, 1, 17)
    if date <= switch_date and contract_code == "DI1":
        df = _adjust_legacy_di1_rates(df, rate_cols)
    else:
        if contract_code in {"FRC", "FRO"} and "PointsVariation" in df.columns:
            rate_cols.append("PointsVariation")

        if rate_cols:
            df = df.with_columns(pl.col(rate_cols).truediv(100).round(5))
    return df


def _add_derived_columns(df: pl.DataFrame, contract_code: str) -> pl.DataFrame:
    count_conv = COUNT_CONVENTIONS.get(contract_code)
    if count_conv in {252, 360} and "SettlementPrice" in df.columns:
        n_days = df["BDaysToExp"] if count_conv == BDAYS_PER_YEAR else df["DaysToExp"]
        df = df.with_columns(
            SettlementRate=_convert_prices_to_rates(
                df["SettlementPrice"], n_days, count_conv
            )
        )
    if contract_code == "DI1" and {"SettlementRate", "SettlementPrice"}.issubset(
        df.columns
    ):
        duration = pl.col("BDaysToExp") / 252
        m_duration = duration / (1 + pl.col("SettlementRate"))
        df = df.with_columns(DV01=0.0001 * m_duration * pl.col("SettlementPrice"))

    if contract_code in {"DI1", "DAP"} and "SettlementRate" in df.columns:
        df = df.with_columns(
            ForwardRate=forwards(df["BDaysToExp"], df["SettlementRate"])
        )
    return df


def fetch_bmf_historical_df(date: dt.date, contract_code: str) -> pl.DataFrame:
    html_text = _fetch_html_data(date, contract_code)
    df = _parse_html_lxml(html_text)
    if df.is_empty():
        return pl.DataFrame()

    # 1. Renomeação Dinâmica (Ponto central da mudança)
    rename_map = _get_column_rename_map(contract_code)
    df = df.rename(rename_map, strict=False)

    # 2. Limpeza e Tipagem
    df = _clean_string_values(df)
    df = _cast_columns(df)

    # 3. Processamento
    df = _add_expiration_dates(df, date, contract_code)
    df = _convert_zeros_to_null(df)
    df = _transform_rates(df, date, contract_code)
    df = _add_derived_columns(df, contract_code)

    return df.select([c for c in OUTPUT_COLUMNS if c in df.columns])
