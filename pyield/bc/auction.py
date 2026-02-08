"""
Documentação da API do BC
    https://olinda.bcb.gov.br/olinda/servico/leiloes_selic/versao/v1/aplicacao#!/recursos/leiloesTitulosPublicos#eyJmb3JtdWxhcmlvIjp7IiRmb3JtYXQiOiJqc29uIiwiJHRvcCI6MTAwfX0=
Exemplo de chamada:
    "https://olinda.bcb.gov.br/olinda/servico/leiloes_selic/versao/v1/odata/leiloesTitulosPublicos(dataMovimentoInicio=@dataMovimentoInicio,dataMovimentoFim=@dataMovimentoFim,dataLiquidacao=@dataLiquidacao,codigoTitulo=@codigoTitulo,dataVencimento=@dataVencimento,edital=@edital,tipoPublico=@tipoPublico,tipoOferta=@tipoOferta)?@dataMovimentoInicio='2025-04-08'&@dataMovimentoFim='2025-04-08'&$top=100&$format=json"

"""  # noqa: E501

import datetime as dt
import logging
from typing import Literal

import polars as pl
import polars.selectors as cs
import requests

import pyield.bc.ptax_api as pt
import pyield.converters as cv
from pyield import bday
from pyield.retry import default_retry
from pyield.tn.ntnb import duration as duration_b
from pyield.tn.ntnf import duration as duration_f
from pyield.types import DateLike

logger = logging.getLogger(__name__)

# FR = First Round (Primeira Rodada), SR = Second Round (Segunda Rodada)
COLUMN_MAP = {
    "id": ("ID", pl.String),
    "dataMovimento": ("Date", pl.Datetime),
    "dataLiquidacao": ("Settlement", pl.Datetime),
    "edital": ("Ordinance", pl.Int64),
    "tipoPublico": ("Buyer", pl.String),
    "prazo": ("CalendarDays", pl.Int64),
    "quantidadeOfertada": ("OfferedQuantityFR", pl.Int64),
    "quantidadeAceita": ("AcceptedQuantityFR", pl.Int64),
    "codigoTitulo": ("SelicCode", pl.Int64),
    "dataVencimento": ("Maturity", pl.Datetime),
    "tipoOferta": ("AuctionType", pl.String),
    "ofertante": ("Issuer", pl.String),
    "quantidadeOfertadaSegundaRodada": ("OfferedQuantitySR", pl.Int64),
    "quantidadeAceitaSegundaRodada": ("AcceptedQuantitySR", pl.Int64),
    "cotacaoMedia": ("AvgPrice", pl.Float64),
    "cotacaoCorte": ("CutPrice", pl.Float64),
    "taxaMedia": ("AvgRate", pl.Float64),
    "taxaCorte": ("CutRate", pl.Float64),
    "financeiro": ("Value", pl.Float64),
    "quantidadeLiquidada": ("SettledQuantityFR", pl.Int64),
    "quantidadeLiquidadaSegundaRodada": ("SettledQuantitySR", pl.Int64),
}

API_SCHEMA = {col: dtype for col, (_, dtype) in COLUMN_MAP.items()}
COLUMN_MAPPING = {col: alias for col, (alias, _) in COLUMN_MAP.items()}

FINAL_COLUMN_ORDER = [
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
    "SettledQuantityFR",
    "SettledQuantitySR",
    "SettledQuantity",
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

SORTING_KEYS = ["Date", "AuctionType", "BondType", "Maturity"]

BASE_API_URL = "https://olinda.bcb.gov.br/olinda/servico/leiloes_selic/versao/v1/odata/leiloesTitulosPublicos(dataMovimentoInicio=@dataMovimentoInicio,dataMovimentoFim=@dataMovimentoFim,dataLiquidacao=@dataLiquidacao,codigoTitulo=@codigoTitulo,dataVencimento=@dataVencimento,edital=@edital,tipoPublico=@tipoPublico,tipoOferta=@tipoOferta)?"


def _build_url(
    start: DateLike | None = None,
    end: DateLike | None = None,
    auction_type: Literal["sell", "buy"] | None = None,
) -> str:
    url = BASE_API_URL
    if start:
        start = cv.convert_dates(start)
        start_str = start.strftime("%Y-%m-%d")
        url += f"@dataMovimentoInicio='{start_str}'"

    if end:
        end = cv.convert_dates(end)
        end_str = end.strftime("%Y-%m-%d")
        url += f"&@dataMovimentoFim='{end_str}'"

    # Mapeamento do auction_type para o valor esperado pela API
    if auction_type:
        normalized_auction_type = auction_type.lower()
        auction_type_mapping = {"sell": "Venda", "buy": "Compra"}
        auction_type_api_value = auction_type_mapping[normalized_auction_type]
        # Adiciona o parâmetro tipoOferta à URL se auction_type for fornecido
        url += f"&@tipoOferta='{auction_type_api_value}'"

    url += "&$format=text/csv"  # Adiciona o formato CSV ao final

    return url


@default_retry
def _get_api_csv(url: str) -> bytes:
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.content


def _parse_csv(csv_content: bytes) -> pl.DataFrame:
    if not csv_content.strip():
        return pl.DataFrame()
    # Lê usando schema explícito para garantir estabilidade dos tipos.
    # Evitamos try_parse_dates para não promover colunas inesperadas a datas.
    df = pl.read_csv(
        csv_content,
        decimal_comma=True,
        schema_overrides=API_SCHEMA,
        null_values=["null"],
        encoding="utf-8",
    )
    # Converte os campos datetime para Date (mantemos apenas a data).
    df = df.with_columns(
        pl.col("dataMovimento", "dataLiquidacao", "dataVencimento").cast(pl.Date)
    )
    return df


def _format_df(df: pl.DataFrame) -> pl.DataFrame:
    return df.filter(pl.col("ofertante") == "Tesouro Nacional").rename(COLUMN_MAPPING)


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
            # Converte o valor financeiro de milhões para unidades e tipo inteiro
            pl.col("Value").mul(1_000_000).cast(pl.Int64),
            # Converte as taxas de % para decimais
            pl.col("AvgRate", "CutRate").truediv(100).round(6),
            # Calcula as quantidades totais, tratando nulos automaticamente.
            OfferedQuantity=pl.sum_horizontal("OfferedQuantityFR", "OfferedQuantitySR"),
            AcceptedQuantity=pl.sum_horizontal(
                "AcceptedQuantityFR", "AcceptedQuantitySR"
            ),
            SettledQuantity=pl.sum_horizontal("SettledQuantityFR", "SettledQuantitySR"),
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
            ValueSR=pl.col("Value") - pl.col("ValueFR"),
            # 7. Mapeia o código SELIC para o tipo de título (BondType)
            BondType=pl.col("SelicCode").replace_strict(
                bond_mapping, return_dtype=pl.String
            ),
        )
        .with_columns(
            # 8. Ajusta o preço médio (AvgPrice) com base na data e tipo do título
            pl.when(
                (pl.col("Date") >= change_date)
                | (pl.col("BondType").is_in(["LTN", "NTN-F"]))
            )
            .then("AvgPrice")
            .otherwise((pl.col("ValueFR") / pl.col("AcceptedQuantityFR")).round(6))
            .alias("AvgPrice")
        )
    )
    df = df.with_columns(BDToMat=bday.count_expr("Settlement", "Maturity"))
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


def _add_duration(df: pl.DataFrame) -> pl.DataFrame:
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
        DV01=dv01_unit_expr * pl.col("AcceptedQuantity"),
        DV01FR=dv01_unit_expr * pl.col("AcceptedQuantityFR"),
        DV01SR=dv01_unit_expr * pl.col("AcceptedQuantitySR"),
    )

    return df


def _get_ptax_df(df: pl.DataFrame) -> pl.DataFrame:
    """Busca a série histórica da PTAX para o intervalo de datas do DataFrame."""
    start_date = df["Date"].min()
    end_date = df["Date"].max()
    assert isinstance(start_date, dt.date)
    assert isinstance(end_date, dt.date)

    # Garante que pelo menos um dia útil seja buscado
    # Isso é importante caso seja o leilão do dia atual e não haja PTAX ainda
    bz_last_bday = bday.last_business_day()
    if start_date >= bz_last_bday:
        start_date = bday.offset(bz_last_bday, -1)

    df_ptax = pt.ptax_series(start=start_date, end=end_date)
    if df_ptax.is_empty():
        return pl.DataFrame()

    return df_ptax.select("Date", "MidRate").rename({"MidRate": "PTAX"}).sort("Date")


def _add_usd_dv01(df: pl.DataFrame, df_ptax: pl.DataFrame) -> pl.DataFrame:
    """
    Adiciona o DV01 em USD usando um join_asof para encontrar a PTAX mais recente.
    """
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
        .otherwise("Duration")
        .alias("AvgMaturity")
    )

    return df


def _sort_and_reorder_columns(df: pl.DataFrame) -> pl.DataFrame:
    return df.select(FINAL_COLUMN_ORDER).sort(SORTING_KEYS)


def auctions(
    start: DateLike | None = None,
    end: DateLike | None = None,
    auction_type: Literal["sell", "buy"] | None = None,
) -> pl.DataFrame:
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
        start (DateLike, opcional): A data de início para a consulta dos leilões.
            Se `start` for fornecido e `end` for `None`, a API retornará dados de
            leilão a partir de `start` até a data mais recente disponível.
            Se `start` e `end` forem `None`, a série histórica completa será retornada.
            Padrão é `None`.
        end (DateLike, opcional): A data de fim para a consulta de dados de leilão.
            Se `end` for fornecido e `start` for `None`, a API retornará dados de
            leilão desde a data mais antiga disponível até a data de `end`.
            Se `start` e `end` forem `None`, a série histórica completa será retornada.
            Padrão é `None`.
        auction_type (Literal["sell", "buy"], opcional): O tipo de leilão para filtrar
            diretamente na API. Padrão é `None` (retorna todos os tipos de leilão).

    Returns:
        pl.DataFrame: Um DataFrame contendo dados de leilões para o período e tipo
            especificados. Em caso de erro ao buscar os dados, um DataFrame vazio
            é retornado e uma mensagem de erro é registrada no log.

    Examples:
        >>> from pyield import bc
        >>> bc.auctions(start="19-08-2025", end="19-08-2025")
        shape: (5, 33)
        ┌────────────┬────────────┬─────────────┬───────────┬───┬──────────────────┬─────────────┬──────────┬─────────────┐
        │ Date       ┆ Settlement ┆ AuctionType ┆ Ordinance ┆ … ┆ AcceptedQuantity ┆ ValueFR     ┆ ValueSR  ┆ Value       │
        │ ---        ┆ ---        ┆ ---         ┆ ---       ┆   ┆ ---              ┆ ---         ┆ ---      ┆ ---         │
        │ date       ┆ date       ┆ str         ┆ i64       ┆   ┆ i64              ┆ i64         ┆ i64      ┆ i64         │
        ╞════════════╪════════════╪═════════════╪═══════════╪═══╪══════════════════╪═════════════╪══════════╪═════════════╡
        │ 2025-08-19 ┆ 2025-08-20 ┆ Venda       ┆ 192       ┆ … ┆ 150000           ┆ 2572400000  ┆ 0        ┆ 2572400000  │
        │ 2025-08-19 ┆ 2025-08-20 ┆ Venda       ┆ 192       ┆ … ┆ 751003           ┆ 12804476147 ┆ 17123853 ┆ 12821600000 │
        │ 2025-08-19 ┆ 2025-08-20 ┆ Venda       ┆ 193       ┆ … ┆ 300759           ┆ 1289936461  ┆ 3263539  ┆ 1293200000  │
        │ 2025-08-19 ┆ 2025-08-20 ┆ Venda       ┆ 194       ┆ … ┆ 500542           ┆ 2071654327  ┆ 2245673  ┆ 2073900000  │
        │ 2025-08-19 ┆ 2025-08-20 ┆ Venda       ┆ 194       ┆ … ┆ 500000           ┆ 2010700000  ┆ 0        ┆ 2010700000  │
        └────────────┴────────────┴─────────────┴───────────┴───┴──────────────────┴─────────────┴──────────┴─────────────┘

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
        - SettledQuantityFR: Quantidade liquidada na primeira rodada (FR).
        - SettledQuantitySR: Quantidade liquidada na segunda rodada (SR).
        - SettledQuantity: Quantidade total liquidada no leilão (FR + SR
        - ValueFR: Valor da primeira rodada (FR) do leilão em R$.
        - ValueSR: Valor da segunda rodada (SR) em R$.
        - Value: Valor total do leilão em R$ (FR + SR).
    """  # noqa: E501
    try:
        url = _build_url(start=start, end=end, auction_type=auction_type)
        api_csv_text = _get_api_csv(url)
        df = _parse_csv(api_csv_text)
        if df.is_empty():
            logger.warning("No auction data found after parsing the API response.")
            return pl.DataFrame()
        df = _format_df(df)
        df = _process_df(df)
        df = _adjust_values_without_auction(df)
        df = _add_duration(df)
        df = _add_dv01(df)
        df_ptax = _get_ptax_df(df)
        df = _add_usd_dv01(df, df_ptax)
        df = _add_avg_maturity(df)
        df = _sort_and_reorder_columns(df)
        # Substituir eventuais NaNs por None para compatibilidade com bancos de dados
        df = df.with_columns(cs.float().fill_nan(None))

        return df
    except Exception as e:
        logger.exception(f"Error fetching auction data from BC API: {e}")
        return pl.DataFrame()
