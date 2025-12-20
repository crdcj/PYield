"""
Documentação da API do BC
    https://olinda.bcb.gov.br/olinda/servico/leiloes_selic/versao/v1/aplicacao#!/recursos/leiloesTitulosPublicos#eyJmb3JtdWxhcmlvIjp7IiRmb3JtYXQiOiJqc29uIiwiJHRvcCI6MTAwfX0=
Exemplo de chamada:
    "https://olinda.bcb.gov.br/olinda/servico/leiloes_selic/versao/v1/odata/leiloesTitulosPublicos(dataMovimentoInicio=@dataMovimentoInicio,dataMovimentoFim=@dataMovimentoFim,dataLiquidacao=@dataLiquidacao,codigoTitulo=@codigoTitulo,dataVencimento=@dataVencimento,edital=@edital,tipoPublico=@tipoPublico,tipoOferta=@tipoOferta)?@dataMovimentoInicio='2025-04-08'&@dataMovimentoFim='2025-04-08'&$top=100&$format=json"

"""  # noqa: E501

import datetime as dt
import io
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
    "quantidadeLiquidada": "SettledQuantityFR",
    "quantidadeOfertadaSegundaRodada": "OfferedQuantitySR",
    "quantidadeAceitaSegundaRodada": "AcceptedQuantitySR",
    "quantidadeLiquidadaSegundaRodada": "SettledQuantitySR",
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
    "quantidadeLiquidada": pl.Int64,
    "quantidadeLiquidadaSegundaRodada": pl.Int64,
}

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
        schema_overrides=API_SCHEMA,
        null_values=["null"],
    )
    # Converte os campos datetime para Date (mantemos apenas a data).
    df = df.with_columns(
        pl.col("dataMovimento", "dataLiquidacao", "dataVencimento").cast(pl.Date)
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
            # Converte o valor financeiro de milhões para unidades e tipo inteiro
            (pl.col("Value") * 1_000_000).round(0).cast(pl.Int64),
            # Converte as taxas de % para decimais
            (pl.col("AvgRate", "CutRate") / 100).round(6),
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
    bdays_to_mat = bday.count(
        start=df.get_column("Settlement"), end=df.get_column("Maturity")
    )
    df = df.with_columns(bdays_to_mat.alias("BDToMat"))
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
    df = pt.ptax_series(start=start_date, end=end_date)
    if df.is_empty():
        return pl.DataFrame()

    # Converte para Polars, seleciona, renomeia e ordena (importante para join_asof)
    return df.select("Date", "MidRate").rename({"MidRate": "PTAX"}).sort("Date")


def _add_usd_dv01(df: pl.DataFrame) -> pl.DataFrame:
    """
    Adiciona o DV01 em USD usando um join_asof para encontrar a PTAX mais recente.
    """
    # Determina o intervalo de datas necessário a partir do DataFrame de leilões
    ptax_start_date = df["Date"].min()
    ptax_end_date = df["Date"].max()

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

    column_keys = ["Date", "AuctionType", "BondType", "Maturity"]
    return df.select(column_sequence).sort(column_keys)


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
        df = _add_usd_dv01(df)
        df = _add_avg_maturity(df)
        df = _sort_and_reorder_columns(df)
        # Substituir eventuais NaNs por None para compatibilidade com bancos de dados
        df = df.with_columns(cs.float().fill_nan(None))

        return df
    except Exception as e:
        logger.exception(f"Error fetching auction data from BC API: {e}")
        return pl.DataFrame()
