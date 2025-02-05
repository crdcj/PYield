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
    "financeiro": "TotalValue",  # = FR + SR (in millions)
    "quantidadeOfertada": "OfferedQuantity",
    "quantidadeAceita": "AcceptedQuantity",
    "quantidadeOfertadaSegundaRodada": "SROfferedQuantity",
    "quantidadeAceitaSegundaRodada": "SRAcceptedQuantity",
    "cotacaoMedia": "AvgPrice",
    "cotacaoCorte": "CutPrice",
    "taxaMedia": "AvgRate",
    "taxaCorte": "CutRate",
}


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
    df["SRAcceptedQuantity"] = df["SRAcceptedQuantity"].fillna(0)
    df["SROfferedQuantity"] = df["SROfferedQuantity"].fillna(0)

    # Calcular o financeiro só do leilão
    total_accepted_quantity = df["AcceptedQuantity"] + df["SRAcceptedQuantity"]
    first_round_ratio = df["AcceptedQuantity"] / total_accepted_quantity
    first_round_ratio = df["AcceptedQuantity"] / total_accepted_quantity
    first_round_ratio = first_round_ratio.where(df["AcceptedQuantity"] != 0, 0)
    # O dado do financeiro do BC está em milhões com uma casa decimal de precisão
    # Portanto, podemos converter para inteiro sem perda de informação
    df["Value"] = (first_round_ratio * df["TotalValue"]).round(1)
    df["Value"] = (first_round_ratio * df["TotalValue"]).round(1)
    df["Value"] = (df["Value"] * 1_000_000).astype("Int64")

    # Calcular o financeiro só da SV
    second_round_ratio = df["SRAcceptedQuantity"] / total_accepted_quantity
    second_round_ratio = second_round_ratio.where(df["SRAcceptedQuantity"] != 0, 0)
    df["SRValue"] = (second_round_ratio * df["TotalValue"]).round(1)
    df["SRValue"] = (df["SRValue"] * 1_000_000).astype("Int64")

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
    adjusted_price = (df["Value"] / df["AcceptedQuantity"]).round(6)
    is_after_change_Date = df["Date"] >= change_Date
    is_ltn_or_ntnf = df["BondType"].isin(["LTN", "NTN-F"])
    keep_avg_price = is_after_change_Date | is_ltn_or_ntnf
    df["AvgPrice"] = df["AvgPrice"].where(keep_avg_price, adjusted_price)

    return df


def _adjust_null_values(df: pd.DataFrame) -> pd.DataFrame:
    # Onde não há quantidade aceita, não há nem taxa e nem PU definidos.
    is_accepted = df["AcceptedQuantity"] != 0
    df["AvgRate"] = df["AvgRate"].where(is_accepted, pd.NA)
    df["CutRate"] = df["CutRate"].where(is_accepted, pd.NA)
    df["AvgPrice"] = df["AvgPrice"].where(is_accepted, pd.NA)
    df["CutPrice"] = df["CutPrice"].where(is_accepted, pd.NA)

    return df


def _add_dv01(df: pd.DataFrame) -> pd.DataFrame:
    df["BDToMat"] = yd.bday.count(start=df["Date"], end=df["Maturity"])

    is_accepted = df["AcceptedQuantity"] != 0  # noqa
    df_not_accepted = df.query("~@is_accepted").reset_index(drop=True)
    df_is_accepted = df.query("@is_accepted").reset_index(drop=True)

    df_lft = df_is_accepted.query("BondType == 'LFT'").reset_index(drop=True)

    df_ltn = df_is_accepted.query("BondType == 'LTN'").reset_index(drop=True)
    if not df_ltn.empty:
        df_ltn["Duration"] = df_ltn["BDToMat"] / 252
        df_ltn["MDuration"] = df_ltn["Duration"] / (1 + df_ltn["AvgRate"] / 100)
        df_ltn["DV01"] = 0.0001 * df_ltn["MDuration"] * df_ltn["AvgPrice"]

    def compute_f_duration(row):
        return duration_f(row["Date"], row["Maturity"], row["CutRate"] / 100)

    df_ntnf = df_is_accepted.query("BondType == 'NTN-F'").reset_index(drop=True)
    if not df_ntnf.empty:
        df_ntnf["Duration"] = df_ntnf.apply(compute_f_duration, axis=1)
        df_ntnf["Duration"] = df_ntnf["Duration"].astype("Float64")
        df_ntnf["MDuration"] = df_ntnf["Duration"] / (1 + df_ntnf["AvgRate"] / 100)
        df_ntnf["DV01"] = 0.0001 * df_ntnf["MDuration"] * df_ntnf["AvgPrice"]

    def compute_b_duration(row):
        return duration_b(row["Date"], row["Maturity"], row["CutRate"] / 100)

    df_ntnb = df_is_accepted.query("BondType == 'NTN-B'").reset_index(drop=True)
    if not df_ntnb.empty:
        df_ntnb["Duration"] = df_ntnb.apply(compute_b_duration, axis=1)
        df_ntnb["Duration"] = df_ntnb["Duration"].astype("Float64")
        df_ntnb["MDuration"] = df_ntnb["Duration"] / (1 + df_ntnb["AvgRate"] / 100)
        df_ntnb["DV01"] = 0.0001 * df_ntnb["MDuration"] * df_ntnb["AvgPrice"]

    df = pd.concat([df_not_accepted, df_lft, df_ltn, df_ntnf, df_ntnb])

    df["DV01"] *= df["AcceptedQuantity"]
    df["DV01"] = df["DV01"].round(0).astype("Int64")

    # Remover as colunas de cálculo do DV01
    df = df.drop(columns=["BDToMat", "Duration", "MDuration"])

    return df.reset_index(drop=True)


def _fetch_df_from_url(url: str) -> pd.DataFrame:
    try:
        df = _load_from_url(url)
        df = _pre_process_df(df)
        df = _adjust_null_values(df)
        df = _process_df(df)
        df = _add_dv01(df)
        sort_columns = ["Date", "AuctionType", "BondType", "Maturity"]
        return df.sort_values(by=sort_columns).reset_index(drop=True)
    except Exception as e:
        logging.error(f"Error on fetching auction data from BC: {e}")
        return pd.DataFrame()


def get_all_auctions() -> pd.DataFrame:
    # URL com toda a série de leilões de títulos públicos do Tesouro Nacional
    url = "https://olinda.bcb.gov.br/olinda/servico/leiloes_selic/versao/v1/odata/leiloesTitulosPublicos(dataMovimentoInicio=@dataMovimentoInicio,dataMovimentoFim=@dataMovimentoFim,dataLiquidacao=@dataLiquidacao,codigoTitulo=@codigoTitulo,dataVencimento=@dataVencimento,edital=@edital,tipoPublico=@tipoPublico,tipoOferta=@tipoOferta)?$format=text/csv"
    return _fetch_df_from_url(url)


def _filter_auction_by_type(
    df: pd.DataFrame, auction_type: Literal["Sell", "Buy"]
) -> pd.DataFrame:
    auction_type_mapping = {"Venda": "Sell", "Compra": "Buy"}
    auction_type = auction_type_mapping.get(auction_type_mapping)
    if auction_type:
        df = df.query(f"AuctionType == '{auction_type}'").reset_index(drop=True)
    return df


def get_auctions_in_range(
    start: DateScalar,
    end: DateScalar,
    auction_type: Literal["Sell", "Buy"] = "Sell",
) -> pd.DataFrame:
    start = dc.convert_input_Dates(start)
    end = dc.convert_input_Dates(end)
    start_str = start.strftime("%Y-%m-%d")
    end_str = end.strftime("%Y-%m-%d")

    url = f"https://olinda.bcb.gov.br/olinda/servico/leiloes_selic/versao/v1/odata/leiloesTitulosPublicos(dataMovimentoInicio=@dataMovimentoInicio,dataMovimentoFim=@dataMovimentoFim,dataLiquidacao=@dataLiquidacao,codigoTitulo=@codigoTitulo,dataMaturity=@dataMaturity,edital=@edital,tipoPublico=@tipoPublico,tipoOferta=@tipoOferta)?@dataMovimentoInicio='{start_str}'&@dataMovimentoFim='{end_str}'&$format=text/csv"
    df = _fetch_df_from_url(url)

    return _filter_auction_by_type(df, auction_type)


def retrieve_auction_on_date(
    date: DateScalar,
    auction_type: Literal["Sell", "Buy"] = "Sell",
) -> pd.DataFrame:
    return get_auctions_in_range(date, date, auction_type)
