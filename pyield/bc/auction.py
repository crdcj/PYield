"""
Documentação da API do BC
    https://olinda.bcb.gov.br/olinda/servico/leiloes_selic/versao/v1/aplicacao#!/recursos/leiloesTitulosPublicos#eyJmb3JtdWxhcmlvIjp7IiRmb3JtYXQiOiJqc29uIiwiJHRvcCI6MTAwfX0=
Exemplo de chamada:
    https://olinda.bcb.gov.br/olinda/servico/leiloes_selic/versao/v1/odata/leiloesTitulosPublicos(dataMovimentoInicio=@dataMovimentoInicio,dataMovimentoFim=@dataMovimentoFim,dataLiquidacao=@dataLiquidacao,codigoTitulo=@codigoTitulo,dataVencimento=@dataVencimento,edital=@edital,tipoPublico=@tipoPublico,tipoOferta=@tipoOferta)?@dataMovimentoInicio='2025-01-30'&@dataMovimentoFim='2025-01-30'&@tipoOferta='Venda'&$top=100&$format=json
"""

import io
import logging
from typing import Literal

import pandas as pd
import requests

from pyield import bday
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
    df["BDToMat"] = bday.count(start=df["Date"], end=df["Maturity"])

    is_accepted = df["AcceptedQuantityFR"] != 0  # noqa
    df_not_accepted = df.query("~@is_accepted").reset_index(drop=True)
    df_is_accepted = df.query("@is_accepted").reset_index(drop=True)

    df_lft = df_is_accepted.query("BondType == 'LFT'").reset_index(drop=True)

    df_ltn = df_is_accepted.query("BondType == 'LTN'").reset_index(drop=True)
    if not df_ltn.empty:
        df_ltn["Duration"] = df_ltn["BDToMat"] / 252
        mduration = df_ltn["Duration"] / (1 + df_ltn["AvgRate"])
        df_ltn["DV01FR"] = 0.0001 * mduration * df_ltn["AvgPrice"]

    def compute_f_duration(row):
        return duration_f(row["Date"], row["Maturity"], row["CutRate"])

    df_ntnf = df_is_accepted.query("BondType == 'NTN-F'").reset_index(drop=True)
    if not df_ntnf.empty:
        df_ntnf["Duration"] = df_ntnf.apply(compute_f_duration, axis=1)
        df_ntnf["Duration"] = df_ntnf["Duration"].astype("Float64")
        mduration = df_ntnf["Duration"] / (1 + df_ntnf["AvgRate"])
        df_ntnf["DV01FR"] = 0.0001 * mduration * df_ntnf["AvgPrice"]

    def compute_b_duration(row):
        return duration_b(row["Date"], row["Maturity"], row["CutRate"])

    df_ntnb = df_is_accepted.query("BondType == 'NTN-B'").reset_index(drop=True)
    if not df_ntnb.empty:
        df_ntnb["Duration"] = df_ntnb.apply(compute_b_duration, axis=1)
        df_ntnb["Duration"] = df_ntnb["Duration"].astype("Float64")
        mduration = df_ntnb["Duration"] / (1 + df_ntnb["AvgRate"])
        df_ntnb["DV01FR"] = 0.0001 * mduration * df_ntnb["AvgPrice"]

    df = pd.concat([df_not_accepted, df_lft, df_ltn, df_ntnf, df_ntnb])

    df["DV01FR"] *= df["AcceptedQuantityFR"]
    df["DV01FR"] = df["DV01FR"].round(0).astype("Int64")

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
        "BDToMat",
        "Duration",
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
        "DV01FR",
        "AvgPrice",
        "CutPrice",
        "AvgRate",
        "CutRate",
    ]

    primary_sort_keys = ["Date", "AuctionType", "BondType", "Maturity"]
    return df[column_sequence].sort_values(by=primary_sort_keys).reset_index(drop=True)


def _fetch_df_from_url(url: str) -> pd.DataFrame:
    try:
        df = _load_from_url(url)
        if df.empty:
            logging.warning("No auction data found for the specified period.")
            return pd.DataFrame()
        df = _pre_process_df(df)
        df = _adjust_null_values(df)
        df = _process_df(df)
        df = _add_dv01(df)
        df = _sort_and_reorder_columns(df)
        return df
    except Exception:
        logging.exception("Error fetching auction data from BC API.")
        return pd.DataFrame()


def auctions(
    start: DateScalar | None = None,
    end: DateScalar | None = None,
    auction_type: Literal["Sell", "Buy"] = None,
) -> pd.DataFrame:
    """
    Recupera dados de leilões para um determinado período e tipo de leilão da API do BC.

    **Consultas de Período:**
    - Para consultar dados de um intervalo, forneça as datas de `start` e `end`.
      Exemplo: `auctions(start='2024-10-20', end='2024-10-27')`
    - Se apenas `start` for fornecido, a API do BC retornará dados de leilão a partir
      da data de `start` **até a data mais recente disponível**.
      Exemplo: `auctions(start='2024-10-20')`
    - Se apenas `end` for fornecido, a API do BC retornará dados de leilão **desde a
      data mais antiga disponível até a data de `end`**.
      Exemplo: `auctions(end='2024-10-27')`

    **Série Histórica Completa:**
    - Para recuperar a série histórica completa de leilões (desde 12/11/2012 até o
      último dia útil), chame a função sem fornecer os parâmetros `start` e `end`.
      Exemplo: `auctions()`

    Busca dados de leilões da API do BC para as datas de início e fim especificadas,
    filtrando os resultados diretamente na API pelo tipo de leilão, se especificado.
    O comportamento da função em relação aos parâmetros `start` e `end` segue o padrão
    da API do Banco Central:
    - Se `start` for fornecido e `end` não, a função retorna dados de `start` até o fim.
    - Se `end` for fornecido e `start` não, a API retorna dados do início até `end`.
    - Se ambos `start` e `end` forem omitidos, a API retorna a série histórica completa.

    Os dados podem ser filtrados pelo tipo de leilão especificado ("Sell" ou "Buy").
    Leilões de "Sell" são aqueles em que o Tesouro Nacional vende títulos ao mercado.
    Leilões de "Buy" são aqueles em que o Tesouro Nacional compra títulos do mercado.

    Args:
        start (DateScalar, opcional): A data de início para a consulta dos leilões.
            Se `start` for fornecido e `end` for `None`, a API retornará dados de
            leilão a partir de `start` até a data mais recente disponível.
            Se `start` e `end` forem `None`, a série histórica completa será retornada.
            Padrão é `None`.
        end (DateScalar, opcional): A data de fim para a consulta de dados de leilão.
            Se `end` for fornecido e `start` for `None`, a API retornará dados de
            leilão desde a data mais antiga disponível até a data de `end`.
            Se `start` e `end` forem `None`, a série histórica completa será retornada.
            Padrão é `None`.
        auction_type (Literal["Sell", "Buy"], opcional): O tipo de leilão para filtrar
            diretamente na API. Padrão é `None` (retorna todos os tipos de leilão).

    Returns:
        pd.DataFrame: Um DataFrame contendo dados de leilões para o período e tipo
            especificados. Em caso de erro ao buscar os dados, um DataFrame vazio
            é retornado e uma mensagem de erro é registrada no log.

    Notes:
        FR = First Round (Primeira Rodada)
        SR = Second Round (Segunda Rodada)

        O DataFrame possui as seguintes colunas:
            - Date: Data do leilão.
            - Settlement: Data de liquidação do leilão.
            - AuctionType: Tipo de leilão (ex: "Sell" ou "Buy").
            - Ordinance: Edital normativo associado ao leilão.
            - Buyer: Categoria do comprador (ex: "TodoMercado", "SomenteDealerApto").
            - BondType: Categoria do título (ex: "LTN", "LFT", "NTN-B", "NTN-F").
            - Maturity: Data de vencimento do título.
            - SelicCode: Código do título no sistema Selic.
            - Value: Valor total do leilão em R$ (FR + SR).
            - ValueFR: Valor da primeira rodada (FR) do leilão em R$.
            - ValueSR: Valor da segunda rodada (SR) em R$.
            - OfferedQuantity: Quantidade total ofertada no leilão (FR + SR).
            - OfferedQuantityFR: Quantidade ofertada na primeira rodada (FR).
            - OfferedQuantitySR: Quantidade ofertada na segunda rodada (SR).
            - AcceptedQuantity: Quantidade total aceita no leilão (FR + SR).
            - AcceptedQuantityFR: Quantidade aceita na primeira rodada (FR).
            - AcceptedQuantitySR: Quantidade aceita na segunda rodada (SR).
            - AvgPrice: Preço médio no leilão.
            - CutPrice: Preço de corte.
            - AvgRate: Taxa de juros média.
            - CutRate: Taxa de corte.
            - BDToMat: Dias úteis até o vencimento.
            - Duration: Duration (Duração) do título.
            - DV01FR: Dollar Value of 1 bp change in yield - Primeira Rodada (FR) em R$.
    """
    url = BASE_API_URL
    if start:
        start = dc.convert_input_dates(start)
        start_str = start.strftime("%Y-%m-%d")
        url += f"@dataMovimentoInicio='{start_str}'"

    if end:
        end = dc.convert_input_dates(end)
        end_str = end.strftime("%Y-%m-%d")
        url += f"&@dataMovimentoFim='{end_str}'"

    # Mapeamento do auction_type para o valor esperado pela API
    auction_type_mapping = {"Sell": "Venda", "Buy": "Compra"}
    auction_type_api_value = auction_type_mapping.get(auction_type)
    # Adiciona o parâmetro tipoOferta à URL se auction_type for fornecido
    if auction_type_api_value:
        url += f"&@tipoOferta='{auction_type_api_value}'"

    url += "&$format=text/csv"  # Adiciona o formato CSV ao final

    return _fetch_df_from_url(url)
