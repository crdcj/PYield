"""
Documentação da API do BC
https://olinda.bcb.gov.br/olinda/servico/leiloes_selic/versao/v1/aplicacao#!/recursos/leiloesTitulosPublicos#eyJmb3JtdWxhcmlvIjp7IiRmb3JtYXQiOiJqc29uIiwiJHRvcCI6MTAwfX0=
"""

import io
import logging
from typing import Literal

import pandas as pd
import requests

import pyield as yd
from pyield import date_converter as dc
from pyield.date_converter import DateScalar
from pyield.tpf.ntnb import duration as duration_b
from pyield.tpf.ntnf import duration as duration_f

"""Dicionário com o mapeamento das colunas da API do BC para o DataFrame final
Chaves com comentário serão descartadas ao final do processamento
A ordem das chaves será a ordem das colunas no DataFrame final
FR = First Round and SR = Second Round
"""
COLUMN_MAPPING = {
    # "id": "ID",
    "dataMovimento": "Date",
    "dataLiquidacao": "Settlement",
    "tipoOferta": "AuctionType",  # ['Venda', 'Compra', 'Tomador']
    # "ofertante": "Ofertante",  # ['Tesouro Nacional', 'Banco Central']
    "edital": "Ordinance",
    "tipoPublico": "Buyer",  # ['TodoMercado', 'SomenteDealerApto', 'SomenteDealer']
    # "prazo": "...", # N. de DC entre a data de liquidação e a data de vencimento
    "codigoTitulo": "SelicCode",  # [100000, 210100, 450000, 760199, 950199]
    "dataVencimento": "Maturity",
    "financeiro": "Value",  # = FR + SR (in millions)
    "quantidadeOfertada": "OfferedQuantityFR",
    "quantidadeAceita": "AcceptedQuantityFR",
    "quantidadeOfertadaSegundaRodada": "OfferedQuantitySR",
    "quantidadeAceitaSegundaRodada": "AcceptedQuantitySR",
    "cotacaoMedia": "AvgPrice",
    "cotacaoCorte": "CutPrice",
    "taxaMedia": "AvgRate",
    "taxaCorte": "CutRate",
}

BASE_API_URL = "https://olinda.bcb.gov.br/olinda/servico/leiloes_selic/versao/v1/odata/leiloesTitulosPublicos(dataMovimentoInicio=@dataMovimentoInicio,dataMovimentoFim=@dataMovimentoFim,dataLiquidacao=@dataLiquidacao,codigoTitulo=@codigoTitulo,dataVencimento=@dataVencimento,edital=@edital,tipoPublico=@tipoPublico,tipoOferta=@tipoOferta)?"


def _load_from_url(url: str) -> pd.DataFrame:
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return pd.read_csv(
        io.StringIO(response.text),
        dtype_backend="numpy_nullable",
        decimal=",",
        date_format="%Y-%m-%d %H:%M:%S",
        parse_dates=["dataMovimento", "dataLiquidacao", "dataVencimento"],
    )


def _pre_process_df(df: pd.DataFrame) -> pd.DataFrame:
    # Only Tesouro Nacional auctions are considered
    df = df.query("ofertante == 'Tesouro Nacional'").reset_index(drop=True)

    # Remover colunas que não serão utilizadas
    keep_columns = [col for col in COLUMN_MAPPING.keys() if col in df.columns]
    return df[keep_columns].rename(columns=COLUMN_MAPPING)


def _process_df(df: pd.DataFrame) -> pd.DataFrame:
    # Second Round (SR) columns can be null -> remove nulls to avoid propagation
    df["AcceptedQuantitySR"] = df["AcceptedQuantitySR"].fillna(0)
    df["OfferedQuantitySR"] = df["OfferedQuantitySR"].fillna(0)

    df["OfferedQuantity"] = df["OfferedQuantityFR"] + df["OfferedQuantitySR"]
    df["AcceptedQuantity"] = df["AcceptedQuantityFR"] + df["AcceptedQuantitySR"]
    # Total value of the auction in R$ millions -> convert it to unit value
    df["Value"] = (1_000_000 * df["Value"]).round(0).astype("Int64")

    # Remove the percentage sign and round to 6 decimal places (4 decimal places in %)
    # Before 25/08/2015, there were no rounding rules for the rates in the BC API
    df["AvgRate"] = (df["AvgRate"] / 100).round(6)
    df["CutRate"] = (df["CutRate"] / 100).round(6)

    # Calculate the financial value of the first round
    first_round_ratio = df["AcceptedQuantityFR"] / df["AcceptedQuantity"]
    # Force 0 when AcceptedQuantityFR is 0 to avoid division by zero
    first_round_ratio = first_round_ratio.where(df["AcceptedQuantityFR"] != 0, 0)
    # O dado do financeiro do BC está em milhões com uma casa decimal de precisão
    # Portanto, podemos converter para inteiro sem perda de informação
    df["ValueFR"] = (first_round_ratio * df["Value"]).round(0).astype("Int64")

    # Calculate the financial value of the second round
    second_round_ratio = df["AcceptedQuantitySR"] / df["AcceptedQuantity"]
    second_round_ratio = second_round_ratio.where(df["AcceptedQuantitySR"] != 0, 0)
    df["ValueSR"] = (second_round_ratio * df["Value"]).round(0).astype("Int64")

    bond_mappping = {
        100000: "LTN",
        210100: "LFT",
        # 450000: "..." # Foi um título ofertado pelo BC em 2009/2010
        760199: "NTN-B",
        950199: "NTN-F",
    }
    df["BondType"] = df["SelicCode"].map(bond_mappping).astype("string")

    # Em 11/06/2024 o BC passou a informar nas colunas de cotacao os valores dos PUs
    # Isso afeta somente os títulos LFT e NTN-B
    change_Date = pd.Timestamp("2024-06-11")
    adjusted_price = (df["ValueFR"] / df["AcceptedQuantityFR"]).round(6)
    is_after_change_Date = df["Date"] >= change_Date
    is_ltn_or_ntnf = df["BondType"].isin(["LTN", "NTN-F"])
    keep_avg_price = is_after_change_Date | is_ltn_or_ntnf
    df["AvgPrice"] = df["AvgPrice"].where(keep_avg_price, adjusted_price)

    return df


def _adjust_null_values(df: pd.DataFrame) -> pd.DataFrame:
    # Onde não há quantidade aceita, não há nem taxa e nem PU definidos.
    is_accepted = df["AcceptedQuantityFR"] != 0
    df["AvgRate"] = df["AvgRate"].where(is_accepted, pd.NA)
    df["CutRate"] = df["CutRate"].where(is_accepted, pd.NA)
    df["AvgPrice"] = df["AvgPrice"].where(is_accepted, pd.NA)
    df["CutPrice"] = df["CutPrice"].where(is_accepted, pd.NA)

    return df


def _add_dv01(df: pd.DataFrame) -> pd.DataFrame:
    df["BDToMat"] = yd.bday.count(start=df["Date"], end=df["Maturity"])

    is_accepted = df["AcceptedQuantityFR"] != 0  # noqa
    df_not_accepted = df.query("~@is_accepted").reset_index(drop=True)
    df_is_accepted = df.query("@is_accepted").reset_index(drop=True)

    df_lft = df_is_accepted.query("BondType == 'LFT'").reset_index(drop=True)

    df_ltn = df_is_accepted.query("BondType == 'LTN'").reset_index(drop=True)
    if not df_ltn.empty:
        df_ltn["Duration"] = df_ltn["BDToMat"] / 252
        df_ltn["MDuration"] = df_ltn["Duration"] / (1 + df_ltn["AvgRate"])
        df_ltn["DV01FR"] = 0.0001 * df_ltn["MDuration"] * df_ltn["AvgPrice"]

    def compute_f_duration(row):
        return duration_f(row["Date"], row["Maturity"], row["CutRate"])

    df_ntnf = df_is_accepted.query("BondType == 'NTN-F'").reset_index(drop=True)
    if not df_ntnf.empty:
        df_ntnf["Duration"] = df_ntnf.apply(compute_f_duration, axis=1)
        df_ntnf["Duration"] = df_ntnf["Duration"].astype("Float64")
        df_ntnf["MDuration"] = df_ntnf["Duration"] / (1 + df_ntnf["AvgRate"])
        df_ntnf["DV01FR"] = 0.0001 * df_ntnf["MDuration"] * df_ntnf["AvgPrice"]

    def compute_b_duration(row):
        return duration_b(row["Date"], row["Maturity"], row["CutRate"])

    df_ntnb = df_is_accepted.query("BondType == 'NTN-B'").reset_index(drop=True)
    if not df_ntnb.empty:
        df_ntnb["Duration"] = df_ntnb.apply(compute_b_duration, axis=1)
        df_ntnb["Duration"] = df_ntnb["Duration"].astype("Float64")
        df_ntnb["MDuration"] = df_ntnb["Duration"] / (1 + df_ntnb["AvgRate"])
        df_ntnb["DV01FR"] = 0.0001 * df_ntnb["MDuration"] * df_ntnb["AvgPrice"]

    df = pd.concat([df_not_accepted, df_lft, df_ltn, df_ntnf, df_ntnb])

    df["DV01FR"] *= df["AcceptedQuantityFR"]
    df["DV01FR"] = df["DV01FR"].round(0).astype("Int64")

    return df


def _filter_auction_by_type(
    df: pd.DataFrame, auction_type: Literal["Sell", "Buy"]
) -> pd.DataFrame:
    auction_type_mapping = {"Venda": "Sell", "Compra": "Buy"}
    auction_type = auction_type_mapping.get(auction_type)
    if auction_type:
        df = df.query(f"AuctionType == '{auction_type}'").reset_index(drop=True)
    return df


def _sort_and_reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    column_sequence = [
        "Date",
        "Settlement",
        "AuctionType",
        "Ordinance",
        "Buyer",
        "BondType",
        "Maturity",
        "SelicCode",
        "Value",
        "ValueFR",
        "ValueSR",
        "OfferedQuantity",
        "OfferedQuantityFR",
        "OfferedQuantitySR",
        "AcceptedQuantity",
        "AcceptedQuantityFR",
        "AcceptedQuantitySR",
        "AvgPrice",
        "CutPrice",
        "AvgRate",
        "CutRate",
        "BDToMat",
        "Duration",
        "MDuration",
        "DV01FR",
    ]

    primary_sort_keys = ["Date", "AuctionType", "BondType", "Maturity"]
    return df[column_sequence].sort_values(by=primary_sort_keys).reset_index(drop=True)


def _fetch_df_from_url(
    url: str,
    auction_type: Literal["Sell", "Buy"] = None,
) -> pd.DataFrame:
    try:
        df = _load_from_url(url)
        df = _pre_process_df(df)
        df = _adjust_null_values(df)
        df = _process_df(df)
        df = _add_dv01(df)
        df = _filter_auction_by_type(df, auction_type)
        df = _sort_and_reorder_columns(df)
        return df
    except Exception:
        logging.exception("Error fetching auction data from BC API.")
        return pd.DataFrame()


def get_all_auctions() -> pd.DataFrame:
    """
    Retrieves a DataFrame containing all public auction data from the BC.

    This function fetches the complete series of public auctions conducted by the
    National Treasury via the BC API and processes it to a standardized DataFrame.

    Returns:
        pd.DataFrame: A DataFrame sorted by Date, AuctionType, BondType, and Maturity
            containing auction data. In case of an error during data retrieval, an empty
            DataFrame is returned and an error message is logged.
    """
    # URL com toda a série de leilões de títulos públicos do Tesouro Nacional
    url = f"{BASE_API_URL}$format=text/csv"

    return _fetch_df_from_url(url)


def auctions(
    start: DateScalar,
    end: DateScalar | None = None,
    auction_type: Literal["Sell", "Buy"] = None,
) -> pd.DataFrame:
    """
    Retrieves auction data for a given date range and auction type.

    It fetches auction data from the BC API for the specified start and end dates.
    If the end date is not provided, it defaults to the start date.
    The resulting data is then filtered by the specified auction type ("Sell" or "Buy").
    "Sell" auctions are those where the National Treasury sells bonds to the market.
    "Buy" auctions are those where the National Treasury buys bonds from the market.

    Args:
        start (DateScalar): The start date for the auction data query.
        end (DateScalar | None, optional): The end date for the auction data query.
            If None, the start date is used. Defaults to None.
        auction_type (Literal["Sell", "Buy"], optional): The type of auction to filter
            by. Defaults to "Sell".

    Returns:
        pd.DataFrame: A DataFrame containing auction data for the specified date range
            and auction type. In case of an error during data retrieval, an empty
            DataFrame is returned and an error message is logged.

    Notes:
        FR = First Round
        SR = Second Round

        The DataFrame has the following columns:
            - Date: Date of the auction.
            - Settlement: Settlement date of the auction.
            - AuctionType: Type of auction (e.g., "Sell" or "Buy").
            - Ordinance: Normative ordinance associated with the auction.
            - Buyer: Buyer category (e.g., "TodoMercado", "SomenteDealerApto").
            - BondType: Category of the bond (e.g., "LTN", "LFT", "NTN-B", "NTN-F").
            - Maturity: Bond maturity date.
            - SelicCode: Bond code in the Selic system.
            - Value: Total value of the auction in R$ (FR + SR).
            - ValueFR: First round (FR) value of the auction in R$.
            - ValueSR: Value in the second round (SR) in R$.
            - OfferedQuantity: Total quantity offered in the auction (FR + SR).
            - OfferedQuantityFR: Quantity offered in the first round (FR).
            - OfferedQuantitySR: Quantity offered in the second round (SR).
            - AcceptedQuantity: Total quantity accepted in the auction (FR + SR).
            - AcceptedQuantityFR: Quantity accepted in the first round (FR).
            - AcceptedQuantitySR: Quantity accepted in the second round (SR).
            - AvgPrice: Average price in the auction.
            - CutPrice: Cut-off price.
            - AvgRate: Average interest rate.
            - CutRate: Cut-off rate.
            - BDToMat: Business days to maturity.
            - Duration: Duration of the bond.
            - MDuration: Modified duration of the bond.
            - DV01FR: First Round (FR) dollar value of a 1 bp change in yield in R$.

    """

    start = dc.convert_input_dates(start)
    if end:
        end = dc.convert_input_dates(end)
    else:
        end = start

    start_str = start.strftime("%Y-%m-%d")
    end_str = end.strftime("%Y-%m-%d")
    # url = f"https://olinda.bcb.gov.br/olinda/servico/leiloes_selic/versao/v1/odata/leiloesTitulosPublicos(dataMovimentoInicio=@dataMovimentoInicio,dataMovimentoFim=@dataMovimentoFim,dataLiquidacao=@dataLiquidacao,codigoTitulo=@codigoTitulo,dataVencimento=@dataVencimento,edital=@edital,tipoPublico=@tipoPublico,tipoOferta=@tipoOferta)?@dataMovimentoInicio='{start_str}'&@dataMovimentoFim='{end_str}'&$format=text/csv"
    url = f"{BASE_API_URL}@dataMovimentoInicio='{start_str}'&@dataMovimentoFim='{end_str}'&$format=text/csv"  # noqa

    return _fetch_df_from_url(url, auction_type)
