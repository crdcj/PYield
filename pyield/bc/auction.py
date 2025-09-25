"""
Documentação da API do BC
    https://olinda.bcb.gov.br/olinda/servico/leiloes_selic/versao/v1/aplicacao#!/recursos/leiloesTitulosPublicos#eyJmb3JtdWxhcmlvIjp7IiRmb3JtYXQiOiJqc29uIiwiJHRvcCI6MTAwfX0=
Exemplo de chamada:
    "https://olinda.bcb.gov.br/olinda/servico/leiloes_selic/versao/v1/odata/leiloesTitulosPublicos(dataMovimentoInicio=@dataMovimentoInicio,dataMovimentoFim=@dataMovimentoFim,dataLiquidacao=@dataLiquidacao,codigoTitulo=@codigoTitulo,dataVencimento=@dataVencimento,edital=@edital,tipoPublico=@tipoPublico,tipoOferta=@tipoOferta)?@dataMovimentoInicio='2025-04-08'&@dataMovimentoFim='2025-04-08'&$top=100&$format=json"

Examplo de retorno da API:

id                             , dataMovimento      , dataLiquidacao     , edital, tipoPublico, prazo, quantidadeOfertada, quantidadeAceita, codigoTitulo, dataVencimento     , tipoOferta, ofertante       , quantidadeOfertadaSegundaRodada, quantidadeAceitaSegundaRodada, cotacaoMedia  , cotacaoCorte  , taxaMedia, taxaCorte, financeiro
9fe0a3ed0ae043f545d8918000005d , 2025-08-28 00:00:00, 2025-08-29 00:00:00,    202, TodoMercado,  2316,            3000000,          3000000,       100000, 2032-01-01 00:00:00, Venda     , Tesouro Nacional,                          750000,                             0, "443,791682"  , "443,791682"  , "13,7599", "13,7599",   "1331,4"
9fe0a3ed0ae043f545d8918000005e , 2025-08-28 00:00:00, 2025-08-29 00:00:00,    203, TodoMercado,  1951,            1000000,          1000000,       950199, 2031-01-01 00:00:00, Venda     , Tesouro Nacional,                          250000,                             0, "887,706572"  , "887,706572"  , "13,703" , "13,703" ,    "887,7"
9fe0a3ed0ae043f545d8918000005c , 2025-08-28 00:00:00, 2025-08-29 00:00:00,    202, TodoMercado,  1402,            6000000,          6000000,       100000, 2029-07-01 00:00:00, Venda     , Tesouro Nacional,                         1500000,                             0, "620,124125"  , "620,116199"  , "13,3786", "13,379" ,   "3720,7"
9fe0a3ed0ae043f545d8918000005f , 2025-08-28 00:00:00, 2025-08-29 00:00:00,    203, TodoMercado,  3412,             300000,           300000,       950199, 2035-01-01 00:00:00, Venda     , Tesouro Nacional,                           75000,                         42426, "825,382124"  , "825,382124"  , "13,928" , "13,928" ,    "282,7"
9fe0a3ed0ae043f545d8918000005b , 2025-08-28 00:00:00, 2025-08-29 00:00:00,    202, TodoMercado,   763,            6000000,          6000000,       100000, 2027-10-01 00:00:00, Venda     , Tesouro Nacional,                         1500000,                             0, "769,543353"  , "769,439322"  , "13,4259", "13,4333",   "4617,3"
9fe09a766d18e3de0ac228c80000c5a, 2025-08-28 00:00:00, 2025-08-29 00:00:00,    202, TodoMercado,   398,            2000000,           960000,       100000, 2026-10-01 00:00:00, Venda     , Tesouro Nacional,                               0,                             0, "865,527516"  , "865,432896"  , "14,2045", "14,216" ,    "830,9"
"""  # noqa: E501

import datetime as dt
import io
import logging
from typing import Literal

import pandas as pd
import polars as pl
import polars.selectors as cs
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
    "cotacaoMedia": "AvgPrice",
    "cotacaoCorte": "CutPrice",
    "taxaMedia": "AvgRate",
    "taxaCorte": "CutRate",
    "quantidadeOfertada": "OfferedQuantityFR",
    "quantidadeAceita": "AcceptedQuantityFR",
    "quantidadeOfertadaSegundaRodada": "OfferedQuantitySR",
    "quantidadeAceitaSegundaRodada": "AcceptedQuantitySR",
    "financeiro": "Value",  # = FR + SR (in millions)
}

# Schema (tipos) esperado para o CSV bruto retornado pela API do BC.
API_SCHEMA = {
    "id": pl.String,  # não usamos, mas é retornado
    "dataMovimento": pl.Datetime,  # será convertido para Date depois
    "dataLiquidacao": pl.Datetime,
    "edital": pl.Int64,
    "tipoPublico": pl.String,
    "prazo": pl.Int64,  # dias corridos entre liquidação e vencimento
    "quantidadeOfertada": pl.Int64,
    "quantidadeAceita": pl.Int64,
    "codigoTitulo": pl.Int64,
    "dataVencimento": pl.Datetime,
    "tipoOferta": pl.String,
    "ofertante": pl.String,
    "quantidadeOfertadaSegundaRodada": pl.Int64,
    "quantidadeAceitaSegundaRodada": pl.Int64,
    "cotacaoMedia": pl.Float64,
    "cotacaoCorte": pl.Float64,
    "taxaMedia": pl.Float64,
    "taxaCorte": pl.Float64,
    "financeiro": pl.Float64,
}

BASE_API_URL = "https://olinda.bcb.gov.br/olinda/servico/leiloes_selic/versao/v1/odata/leiloesTitulosPublicos(dataMovimentoInicio=@dataMovimentoInicio,dataMovimentoFim=@dataMovimentoFim,dataLiquidacao=@dataLiquidacao,codigoTitulo=@codigoTitulo,dataVencimento=@dataVencimento,edital=@edital,tipoPublico=@tipoPublico,tipoOferta=@tipoOferta)?"


def _build_url(
    start: DateScalar | None = None,
    end: DateScalar | None = None,
    auction_type: Literal["sell", "buy"] | None = None,
) -> str:
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

    return url


@default_retry
def _get_api_csv(url: str) -> bytes:
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    response.encoding = "utf-8"
    return response.text


def _parse_csv(csv_text: str) -> pl.DataFrame:
    # Lê usando schema explícito para garantir estabilidade dos tipos.
    # Evitamos try_parse_dates para não promover colunas inesperadas a datas.
    df = pl.read_csv(
        io.StringIO(csv_text),
        decimal_comma=True,
        schema=API_SCHEMA,
        null_values=["null"],
    )
    # Converte os campos datetime para Date (mantemos apenas a data).
    df = df.with_columns(
        pl.col(["dataMovimento", "dataLiquidacao", "dataVencimento"]).cast(pl.Date)
    )
    return df


def _format_df(df: pl.DataFrame) -> pl.DataFrame:
    # Seleciona apenas as colunas que foram mapeadas (descartando as comentadas)
    return (
        df.filter(pl.col("ofertante") == "Tesouro Nacional")
        .select([col for col in NAME_MAPPING if col in df.columns])
        .rename(NAME_MAPPING)
    )


def _process_df(df: pl.DataFrame) -> pl.DataFrame:
    # Em 11/06/2024 o BC passou a informar nas colunas de cotacao os valores dos PUs
    # Isso afeta somente os títulos LFT e NTN-B
    change_date = dt.datetime.strptime("11-06-2024", "%d-%m-%Y").date()

    bond_mapping = {
        100000: "LTN",
        210100: "LFT",
        # 450000: "..." # Foi um título ofertado pelo BC em 2009/2010
        760199: "NTN-B",
        950199: "NTN-F",
    }

    # A API retorna somente as quantidades de SR e FR e o financeiro total (FR + SR).
    # Assim, vamos ter que somar SR com FR para obter as quantidades totais.
    # E calcular o financeiro da FR e SR com base na proporção das quantidades.
    df = (
        df.with_columns(
            # 1. Calcula as quantidades totais, tratando nulos automaticamente.
            pl.sum_horizontal("OfferedQuantityFR", "OfferedQuantitySR").alias(
                "OfferedQuantity"
            ),
            pl.sum_horizontal("AcceptedQuantityFR", "AcceptedQuantitySR").alias(
                "AcceptedQuantity"
            ),
            # 2. Converte o valor financeiro de milhões para unidades e tipo inteiro
            (pl.col("Value") * 1_000_000).round(0).cast(pl.Int64).alias("Value"),
            # 3. Converte as taxas de % para decimais
            (pl.col("AvgRate") / 100).round(6).alias("AvgRate"),
            (pl.col("CutRate") / 100).round(6).alias("CutRate"),
        )
        .with_columns(
            # 5. Calcula o valor financeiro da primeira rodada (ValueFR)
            pl.when(pl.col("AcceptedQuantityFR") != 0)
            .then(
                (pl.col("AcceptedQuantityFR") / pl.col("AcceptedQuantity"))
                * pl.col("Value")
            )
            .otherwise(0)
            .round(0)
            .cast(pl.Int64)
            .alias("ValueFR"),
        )
        .with_columns(
            # 6. Calcula o valor da segunda rodada (ValueSR)
            (pl.col("Value") - pl.col("ValueFR")).alias("ValueSR"),
            # 7. Mapeia o código SELIC para o tipo de título (BondType)
            pl.col("SelicCode")
            .replace_strict(bond_mapping, return_dtype=pl.String)
            .alias("BondType"),
        )
        .with_columns(
            # 8. Ajusta o preço médio (AvgPrice) com base na data e tipo do título
            pl.when(
                (pl.col("Date") >= change_date)
                | (pl.col("BondType").is_in(["LTN", "NTN-F"]))
            )
            .then(pl.col("AvgPrice"))
            .otherwise((pl.col("ValueFR") / pl.col("AcceptedQuantityFR")).round(6))
            .alias("AvgPrice")
        )
    )
    s_bd_to_mat_pd = bday.count(
        start=df.get_column("Settlement"),
        end=df.get_column("Maturity"),
    )
    s_bd_to_mat = pl.Series(s_bd_to_mat_pd)
    df = df.with_columns(s_bd_to_mat.alias("BDToMat"))
    return df


def _adjust_values_without_auction(df: pl.DataFrame) -> pl.DataFrame:
    # Onde não há quantidade aceita na primeira volta, não há taxa ou PU definidos.
    # A API do BC retorna 0.0 nesses casos, mas vamos ajustar para None.
    cols_to_update = ["AvgRate", "CutRate", "AvgPrice", "CutPrice"]
    df = df.with_columns(
        pl.when(pl.col("AcceptedQuantityFR") == 0)
        .then(None)
        .otherwise(pl.col(cols_to_update))
        .name.keep()
    )
    return df


def _add_duration(df: pl.DataFrame) -> pd.DataFrame:
    """
    Calcula a duration para cada tipo de título, aplicando uma função
    linha a linha para os casos não-vetorizáveis (NTN-F e NTN-B).
    """

    def calculate_duration_per_row(row: dict) -> float:
        """Função auxiliar que aplica a lógica para uma única linha."""
        bond_type = row["BondType"]

        if bond_type == "LTN":
            return row["BDToMat"] / 252
        elif bond_type == "NTN-F":
            # Chamada da sua função externa, linha a linha
            return duration_f(row["Settlement"], row["Maturity"], row["AvgRate"])
        elif bond_type == "NTN-B":
            # Chamada da sua função externa, linha a linha
            return duration_b(row["Settlement"], row["Maturity"], row["AvgRate"])
        else:  # LFT e outros casos
            return 0.0

    df = df.with_columns(
        pl.struct(["BondType", "Settlement", "Maturity", "AvgRate", "BDToMat"])
        .map_elements(calculate_duration_per_row, return_dtype=pl.Float64)
        .alias("Duration")
    )
    return df


def _add_dv01(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calcula o DV01 para o leilão de forma 100% vetorizada em Polars.
    """
    # 1. Define a expressão base para o cálculo do DV01 unitário.
    dv01_unit_expr = (
        0.0001 * pl.col("AvgPrice") * pl.col("Duration") / (1 + pl.col("AvgRate"))
    )

    df = df.with_columns(
        # 2. Criar as colunas DV01 multiplicando a expressão base pelas quantidades.
        (dv01_unit_expr * pl.col("AcceptedQuantity")).alias("DV01"),
        (dv01_unit_expr * pl.col("AcceptedQuantityFR")).alias("DV01FR"),
        (dv01_unit_expr * pl.col("AcceptedQuantitySR")).alias("DV01SR"),
    )

    return df


def _get_ptax_df(start_date: dt.date, end_date: dt.date) -> pl.DataFrame:
    """
    Busca a série histórica da PTAX no intervalo de datas especificado
    e retorna como um DataFrame Polars.
    """
    # Garante que pelo menos um dia útil seja buscado
    # Isso é importante caso seja o leilão do dia atual e não haja PTAX ainda
    bz_last_bday = bday.last_business_day()
    if start_date >= bz_last_bday:
        start_date = bday.offset(bz_last_bday, -1)

    # Busca a série PTAX usando a função já existente
    df_pd = pt.ptax_series(start=start_date, end=end_date)
    if df_pd.empty:
        return pl.DataFrame()

    # Converte para Polars, seleciona, renomeia e ordena (importante para join_asof)
    return (
        pl.from_pandas(df_pd)
        .select(["Date", "MidRate"])
        .rename({"MidRate": "PTAX"})
        .sort("Date")
    )


def _add_usd_dv01(df: pl.DataFrame) -> pl.DataFrame:
    """
    Adiciona o DV01 em USD usando um join_asof para encontrar a PTAX mais recente.
    """
    # Determina o intervalo de datas necessário a partir do DataFrame de leilões
    ptax_start_date = df.get_column("Date").min()
    ptax_end_date = df.get_column("Date").max()

    # Busca o DataFrame da PTAX
    df_ptax = _get_ptax_df(start_date=ptax_start_date, end_date=ptax_end_date)
    if df_ptax.is_empty():
        # Se não houver dados de PTAX, retorna o DataFrame original sem alterações
        logger.warning("No PTAX data available to calculate DV01 in USD.")
        return df

    df = (
        df.sort("Date")  # Importante para o join_asof
        .join_asof(df_ptax, on="Date", strategy="backward")
        .with_columns(
            (cs.starts_with("DV01") / pl.col("PTAX")).round(2).name.suffix("USD")
        )
        .drop("PTAX")
    )
    return df


def _add_avg_maturity(df: pl.DataFrame) -> pl.DataFrame:
    # Na metodolgia do Tesouro Nacional, a maturidade média é a mesma que a duração
    df = df.with_columns(
        pl.when(pl.col("BondType") == "LFT")
        .then(pl.col("BDToMat") / 252)
        .otherwise(pl.col("Duration"))
        .alias("AvgMaturity")
    )

    return df


def _sort_and_reorder_columns(df: pl.DataFrame) -> pl.DataFrame:
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

    column_keys = ["Date", "AuctionType", "BondType", "Maturity"]
    return df.select(column_sequence).sort(column_keys)


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
        - BDToMat: Dias úteis entre a liquidação da 1R e a data de vencimento do título.
        - Duration: Duration (Duração) calculada com base na data de
            liquidação da 1R e na data de vencimento do título.
        - AvgMaturity: Maturidade média do título (em anos).
        - AvgPrice: Preço médio no leilão.
        - CutPrice: Preço de corte.
        - AvgRate: Taxa de juros média.
        - CutRate: Taxa de corte.
        - DV01FR: DV01 da Primeira Rodada (FR) em R$.
        - DV01SR: DV01 da Segunda Rodada (SR) em R$.
        - DV01: Valor do DV01 total do leilão em R$.
        - DV01FRUSD: DV01 da Primeira Rodada (FR) em dólares (USD).
        - DV01SRUSD: DV01 da Segunda Rodada (SR) em dólares (USD).
        - DV01USD: DV01 total do leilão em dólares (USD).
        - OfferedQuantityFR: Quantidade ofertada na primeira rodada (FR).
        - OfferedQuantitySR: Quantidade ofertada na segunda rodada (SR).
        - OfferedQuantity: Quantidade total ofertada no leilão (FR + SR).
        - AcceptedQuantityFR: Quantidade aceita na primeira rodada (FR).
        - AcceptedQuantitySR: Quantidade aceita na segunda rodada (SR).
        - AcceptedQuantity: Quantidade total aceita no leilão (FR + SR).
        - ValueFR: Valor da primeira rodada (FR) do leilão em R$.
        - ValueSR: Valor da segunda rodada (SR) em R$.
        - Value: Valor total do leilão em R$ (FR + SR).
    """
    try:
        url = _build_url(start=start, end=end, auction_type=auction_type)
        api_csv_text = _get_api_csv(url)
        df = _parse_csv(api_csv_text)
        if df.is_empty():
            logger.warning("No auction data found after parsing the API response.")
            return pd.DataFrame()
        df = _format_df(df)
        df = _process_df(df)
        df = _adjust_values_without_auction(df)
        df = _add_duration(df)
        df = _add_dv01(df)
        df = _add_usd_dv01(df)
        df = _add_avg_maturity(df)
        df = _sort_and_reorder_columns(df)
        return df.to_pandas(use_pyarrow_extension_array=True)
    except Exception as e:
        logger.exception(f"Error fetching auction data from BC API: {e}")
        return pd.DataFrame()
