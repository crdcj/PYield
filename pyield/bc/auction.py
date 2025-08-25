"""
Documentação da API do BC
    https://olinda.bcb.gov.br/olinda/servico/leiloes_selic/versao/v1/aplicacao#!/recursos/leiloesTitulosPublicos#eyJmb3JtdWxhcmlvIjp7IiRmb3JtYXQiOiJqc29uIiwiJHRvcCI6MTAwfX0=
Exemplo de chamada:
    "https://olinda.bcb.gov.br/olinda/servico/leiloes_selic/versao/v1/odata/leiloesTitulosPublicos(dataMovimentoInicio=@dataMovimentoInicio,dataMovimentoFim=@dataMovimentoFim,dataLiquidacao=@dataLiquidacao,codigoTitulo=@codigoTitulo,dataVencimento=@dataVencimento,edital=@edital,tipoPublico=@tipoPublico,tipoOferta=@tipoOferta)?@dataMovimentoInicio='2025-04-08'&@dataMovimentoFim='2025-04-08'&$top=100&$format=json"
"""

import datetime as dt
import io
import logging
from typing import Literal

import pandas as pd
import requests

from pyield import bday
from pyield import date_converter as dc
from pyield.bc import ptax_api as pt
from pyield.date_converter import DateScalar
from pyield.retry import default_retry
from pyield.tn.ntnb import duration as duration_b
from pyield.tn.ntnf import duration as duration_f

logger = logging.getLogger(__name__)

"""
Dicionário com o mapeamento das colunas da API do BC para o DataFrame final
Chaves com comentário serão descartadas ao final do processamento
FR = First Round and SR = Second Round
"""

DTYPE_MAPPING = {
    # Datas já são tratadas pelo `parse_dates`, mas podemos forçar o tipo final.
    "dataMovimento": "date32[pyarrow]",
    "dataLiquidacao": "date32[pyarrow]",
    "dataVencimento": "date32[pyarrow]",
    # Colunas de texto
    "tipoOferta": "string[pyarrow]",
    "tipoPublico": "string[pyarrow]",
    # Colunas numéricas (inteiros)
    "edital": "int64[pyarrow]",
    "codigoTitulo": "int64[pyarrow]",
    "quantidadeOfertada": "int64[pyarrow]",
    "quantidadeAceita": "int64[pyarrow]",
    "quantidadeOfertadaSegundaRodada": "int64[pyarrow]",
    "quantidadeAceitaSegundaRodada": "int64[pyarrow]",
    # Colunas numéricas (ponto flutuante)
    "financeiro": "float64[pyarrow]",
    "cotacaoMedia": "float64[pyarrow]",
    "cotacaoCorte": "float64[pyarrow]",
    "taxaMedia": "float64[pyarrow]",
    "taxaCorte": "float64[pyarrow]",
}

NAME_MAPPING = {
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


@default_retry
def _load_from_url(url: str) -> pd.DataFrame:
    response = requests.get(url, timeout=10)
    response.raise_for_status()

    # 1. Leitura inicial dos dados
    df = pd.read_csv(
        io.StringIO(response.text),
        dtype_backend="pyarrow",
        decimal=",",
        date_format="%Y-%m-%d %H:%M:%S",
        parse_dates=["dataMovimento", "dataLiquidacao", "dataVencimento"],
    )

    # 2. Aplica todos os tipos de uma só vez usando o dicionário
    df = df.astype(DTYPE_MAPPING)

    df = df.query("ofertante == 'Tesouro Nacional'").reset_index(drop=True)

    # 3. Renomeia as colunas para o padrão desejado
    df = df.rename(columns=NAME_MAPPING)

    # 4. Seleciona apenas as colunas que foram mapeadas (descartando as comentadas)
    final_columns = [col for col in NAME_MAPPING.values() if col in df.columns]

    return df[final_columns].reset_index(drop=True)


def _process_df(df: pd.DataFrame) -> pd.DataFrame:
    # A API retorna somente os valores de SR e FR
    # Assim, vamos ter que somar SR com FR para obter os valores totais

    # Remover valores nulos de SR e FR para o nulo não ser propagado
    df["AcceptedQuantitySR"] = df["AcceptedQuantitySR"].fillna(0)
    df["OfferedQuantitySR"] = df["OfferedQuantitySR"].fillna(0)

    df["OfferedQuantity"] = df["OfferedQuantityFR"] + df["OfferedQuantitySR"]
    df["AcceptedQuantity"] = df["AcceptedQuantityFR"] + df["AcceptedQuantitySR"]
    # Total value of the auction in R$ millions
    # Convert to R$ and use int since there is no decimal value after conversion
    df["Value"] = (1_000_000 * df["Value"]).round(0).astype("int64[pyarrow]")

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
    df["ValueFR"] = (first_round_ratio * df["Value"]).round(0).astype("int64[pyarrow]")
    df["ValueSR"] = df["Value"] - df["ValueFR"]

    bond_mappping = {
        100000: "LTN",
        210100: "LFT",
        # 450000: "..." # Foi um título ofertado pelo BC em 2009/2010
        760199: "NTN-B",
        950199: "NTN-F",
    }
    df["BondType"] = df["SelicCode"].map(bond_mappping).astype("string[pyarrow]")

    # Em 11/06/2024 o BC passou a informar nas colunas de cotacao os valores dos PUs
    # Isso afeta somente os títulos LFT e NTN-B
    change_date = dt.datetime.strptime("11-06-2024", "%d-%m-%Y").date()
    adjusted_avg_price = (df["ValueFR"] / df["AcceptedQuantityFR"]).round(6)
    is_date_after_change = df["Date"] >= change_date
    is_ltn_or_ntnf = df["BondType"].isin(["LTN", "NTN-F"])
    # Se for depois da data de mudança ou for LTN/NTN-F, manter o preço médio
    keep_avg_price = is_date_after_change | is_ltn_or_ntnf
    df["AvgPrice"] = df["AvgPrice"].where(keep_avg_price, adjusted_avg_price)

    # Usar a data de liquidação para calcular o número de dias úteis até o vencimento
    df["BDToMat"] = bday.count(start=df["Settlement"], end=df["Maturity"])

    return df


def _adjust_null_values(df: pd.DataFrame) -> pd.DataFrame:
    # Onde não há quantidade aceita na primeira volta, não há taxa ou PU definidos.
    is_accepted = df["AcceptedQuantityFR"] != 0
    cols_to_update = ["AvgRate", "CutRate", "AvgPrice", "CutPrice"]
    df[cols_to_update] = df[cols_to_update].where(is_accepted, pd.NA)

    return df


def _add_duration(df: pd.DataFrame) -> pd.DataFrame:
    df["Duration"] = 0.0
    df_lft = df.query("BondType in ['LFT']").reset_index(drop=True)

    df_ltn = df.query("BondType == 'LTN'").reset_index(drop=True)
    if not df_ltn.empty:
        df_ltn["Duration"] = df_ltn["BDToMat"] / 252

    df_ntnf = df.query("BondType == 'NTN-F'").reset_index(drop=True)
    if not df_ntnf.empty:
        df_ntnf["Duration"] = df_ntnf.apply(
            lambda row: duration_f(row["Settlement"], row["Maturity"], row["AvgRate"]),
            axis=1,
        )

    df_ntnb = df.query("BondType == 'NTN-B'").reset_index(drop=True)
    if not df_ntnb.empty:
        df_ntnb["Duration"] = df_ntnb.apply(
            lambda row: duration_b(row["Settlement"], row["Maturity"], row["AvgRate"]),
            axis=1,
        )

    df = pd.concat([df_lft, df_ltn, df_ntnf, df_ntnb]).reset_index(drop=True)
    df["Duration"] = df["Duration"].astype("float64[pyarrow]")

    return df


def _add_dv01(df: pd.DataFrame) -> pd.DataFrame:
    # DV01 por título calculado com base nos valores da primeira rodada
    mduration = df["Duration"] / (1 + df["AvgRate"])
    dv01 = 0.0001 * mduration * df["AvgPrice"]

    # Valores totais de DV01 para o leilão
    df["DV01"] = dv01 * df["AcceptedQuantity"]
    df["DV01FR"] = dv01 * df["AcceptedQuantityFR"]
    df["DV01SR"] = dv01 * df["AcceptedQuantitySR"]

    for col in ["DV01", "DV01FR", "DV01SR"]:
        # Definrir DV01 nulos do leilão como 0
        df[col] = df[col].fillna(0).round(2)

    # Forçar 0 nas LFT, pois não há DV01
    df.loc[df["BondType"] == "LFT", ["DV01", "DV01FR", "DV01SR"]] = 0.0

    return df


def _get_ptax_df(start_date: dt.date, end_date: dt.date) -> pd.DataFrame:
    # A série de leilões começa em 2007
    df = pt.ptax_series(start=start_date, end=end_date)
    return df[["Date", "MidRate"]].rename(columns={"MidRate": "PTAX"})


def _add_usd_dv01(df: pd.DataFrame) -> pd.DataFrame:
    # 1. Garanta que o DataFrame 'right' esteja ordenado pela chave de merge.
    df_ptax = _get_ptax_df(start_date=df["Date"].min(), end_date=df["Date"].max())

    # 2. Garanta que o DataFrame 'left' esteja ordenado pela chave de merge.
    df = df.sort_values(by="Date").reset_index(drop=True)
    df = pd.merge_ordered(left=df, right=df_ptax, on="Date", how="left")
    # Se não houver PTAX, preencher com o último valor conhecido
    df["PTAX"] = df["PTAX"].ffill()

    dv01_cols = [c for c in df.columns if c.startswith("DV01")]
    for col in dv01_cols:
        df[f"{col}USD"] = df[col] / df["PTAX"]

    return df.drop(columns=["PTAX"])


def _add_avg_maturity(df: pd.DataFrame) -> pd.DataFrame:
    # Na metodolgia do Tesouro Nacional, a maturidade média é a mesma que a duração
    df["AvgMaturity"] = df["Duration"]

    # Para LFT, a maturidade média é calculada como o n. de dias úteis até o vencimento
    is_lft = df["BondType"] == "LFT"
    df.loc[is_lft, "AvgMaturity"] = df.loc[is_lft, "BDToMat"] / 252

    return df


def _sort_and_reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    column_sequence = [
        "Date",
        "Settlement",
        "AuctionType",
        "Ordinance",
        "Buyer",
        "BondType",
        "SelicCode",
        "Maturity",
        "BDToMat",
        "Duration",
        "AvgMaturity",
        "AvgPrice",
        "CutPrice",
        "AvgRate",
        "CutRate",
        "DV01FR",
        "DV01SR",
        "DV01",
        "DV01FRUSD",
        "DV01SRUSD",
        "DV01USD",
        "OfferedQuantityFR",
        "OfferedQuantitySR",
        "OfferedQuantity",
        "AcceptedQuantityFR",
        "AcceptedQuantitySR",
        "AcceptedQuantity",
        "ValueFR",
        "ValueSR",
        "Value",
    ]

    primary_sort_keys = ["Date", "AuctionType", "BondType", "Maturity"]
    return df[column_sequence].sort_values(by=primary_sort_keys).reset_index(drop=True)


def _fetch_df_from_url(url: str) -> pd.DataFrame:
    try:
        df = _load_from_url(url)
        if df.empty:
            logger.warning("No auction data found for the specified period.")
            return pd.DataFrame()

        df = _adjust_null_values(df)
        df = _process_df(df)
        df = _add_duration(df)
        df = _add_dv01(df)
        df = _add_usd_dv01(df)
        df = _add_avg_maturity(df)
        df = _sort_and_reorder_columns(df)
        return df
    except Exception:
        logger.exception("Error fetching auction data from BC API.")
        return pd.DataFrame()


def auctions(
    start: DateScalar | None = None,
    end: DateScalar | None = None,
    auction_type: Literal["sell", "buy"] | None = None,
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
        auction_type (Literal["sell", "buy"], opcional): O tipo de leilão para filtrar
            diretamente na API. Padrão é `None` (retorna todos os tipos de leilão).

    Returns:
        pd.DataFrame: Um DataFrame contendo dados de leilões para o período e tipo
            especificados. Em caso de erro ao buscar os dados, um DataFrame vazio
            é retornado e uma mensagem de erro é registrada no log.

    Examples:
        >>> from pyield import bc
        >>> df = bc.auctions(start="19-08-2025", end="19-08-2025")
        >>> df
                Date Settlement AuctionType  ...     ValueFR  ValueSR       Value
        0 2025-08-19 2025-08-20       Venda  ...  2572400000        0  2572400000
        1 2025-08-19 2025-08-20       Venda  ... 12804476147 17123853 12821600000
        2 2025-08-19 2025-08-20       Venda  ...  1289936461  3263539  1293200000
        3 2025-08-19 2025-08-20       Venda  ...  2071654327  2245673  2073900000
        4 2025-08-19 2025-08-20       Venda  ...  2010700000        0  2010700000
        [5 rows x 30 columns]

    Notes:
        FR = First Round (Primeira Rodada)

        SR = Second Round (Segunda Rodada)

    DataFrame Columns:
        - Date: Data do leilão.
        - Settlement: Data de liquidação do leilão.
        - AuctionType: Tipo de leilão (ex: "Sell" ou "Buy").
        - Ordinance: Edital normativo associado ao leilão.
        - Buyer: Categoria do comprador (ex: "TodoMercado", "SomenteDealerApto").
        - BondType: Categoria do título (ex: "LTN", "LFT", "NTN-B", "NTN-F").
        - SelicCode: Código do título no sistema Selic.
        - Maturity: Data de vencimento do título.
        - DV01: Valor do DV01 total do leilão em R$.
        - DV01FR: DV01 da Primeira Rodada (FR) em R$.
        - DV01SR: DV01 da Segunda Rodada (SR) em R$.
        - DV01USD: DV01 total do leilão em dólares (USD).
        - DV01FRUSD: DV01 da Primeira Rodada (FR) em dólares (USD).
        - DV01SRUSD: DV01 da Segunda Rodada (SR) em dólares (USD).
        - AvgMaturity: Maturidade média do título (em anos).
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
        - BDToMat: Dias úteis entre a data de liquidação da 1R e a data de
            vencimento do título.
        - Duration: Duration (Duração) calculada com base na data de
            liquidação da 1R e na data de vencimento do título.
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
    auction_type_mapping = {"sell": "Venda", "buy": "Compra"}
    if auction_type:
        auction_type = str(auction_type).lower()
        auction_type_api_value = auction_type_mapping[auction_type]
        # Adiciona o parâmetro tipoOferta à URL se auction_type for fornecido
        url += f"&@tipoOferta='{auction_type_api_value}'"

    url += "&$format=text/csv"  # Adiciona o formato CSV ao final

    return _fetch_df_from_url(url)
