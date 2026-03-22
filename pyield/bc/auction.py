"""
Documentação da API do BC:
    https://olinda.bcb.gov.br/olinda/servico/leiloes_selic/versao/v1/aplicacao#!/recursos/leiloesTitulosPublicos#eyJmb3JtdWxhcmlvIjp7IiRmb3JtYXQiOiJqc29uIiwiJHRvcCI6MTAwfX0=

Exemplo de chamada:
    https://olinda.bcb.gov.br/olinda/servico/leiloes_selic/versao/v1/odata/leiloesTitulosPublicos(dataMovimentoInicio=@dataMovimentoInicio,dataMovimentoFim=@dataMovimentoFim,dataLiquidacao=@dataLiquidacao,codigoTitulo=@codigoTitulo,dataVencimento=@dataVencimento,edital=@edital,tipoPublico=@tipoPublico,tipoOferta=@tipoOferta)?@dataMovimentoInicio='2025-04-08'&@dataMovimentoFim='2025-04-08'&$top=100&$format=json
"""

import datetime as dt
import logging
from typing import Literal

import polars as pl
import polars.selectors as cs
import requests

import pyield._internal.converters as cv
import pyield.bc.ptax_api as pt
from pyield import bday
from pyield._internal.br_numbers import float_br, taxa_br
from pyield._internal.retry import retry_padrao
from pyield._internal.types import DateLike
from pyield.tn.ntnb import duration as duration_b
from pyield.tn.ntnf import duration as duration_f

registro = logging.getLogger(__name__)

MAPA_TITULOS = {
    100000: "LTN",
    210100: "LFT",
    760199: "NTN-B",
    950199: "NTN-F",
}

ORDEM_COLUNAS_FINAL = [
    "data_leilao",
    "data_liquidacao",
    "tipo_leilao",
    "numero_edital",
    "tipo_publico",
    "titulo",
    "codigo_selic",
    "data_vencimento",
    "dias_uteis",
    "duration",
    "prazo_medio",
    "pu_medio",
    "pu_corte",
    "taxa_media",
    "taxa_corte",
    "dv01_1v",
    "dv01_2v",
    "dv01_total",
    "ptax",
    "dv01_1v_usd",
    "dv01_2v_usd",
    "dv01_total_usd",
    "quantidade_liquidada_1v",
    "quantidade_liquidada_2v",
    "quantidade_liquidada_total",
    "quantidade_ofertada_1v",
    "quantidade_ofertada_2v",
    "quantidade_ofertada_total",
    "quantidade_aceita_1v",
    "quantidade_aceita_2v",
    "quantidade_aceita_total",
    "financeiro_1v",
    "financeiro_2v",
    "financeiro_total",
]

CHAVES_ORDENACAO = ["data_leilao", "tipo_leilao", "titulo", "data_vencimento"]

URL_BASE_API = "https://olinda.bcb.gov.br/olinda/servico/leiloes_selic/versao/v1/odata/leiloesTitulosPublicos(dataMovimentoInicio=@dataMovimentoInicio,dataMovimentoFim=@dataMovimentoFim,dataLiquidacao=@dataLiquidacao,codigoTitulo=@codigoTitulo,dataVencimento=@dataVencimento,edital=@edital,tipoPublico=@tipoPublico,tipoOferta=@tipoOferta)?"


MAPA_TIPO_LEILAO = {"sell": "Venda", "buy": "Compra"}


def _montar_url(
    inicio: DateLike | None = None,
    fim: DateLike | None = None,
    tipo_leilao: Literal["sell", "buy"] | None = None,
) -> str:
    url = URL_BASE_API
    if inicio:
        inicio = cv.converter_datas(inicio)
        url += f"@dataMovimentoInicio='{inicio:%Y-%m-%d}'"
    if fim:
        fim = cv.converter_datas(fim)
        url += f"&@dataMovimentoFim='{fim:%Y-%m-%d}'"
    if tipo_leilao:
        url += f"&@tipoOferta='{MAPA_TIPO_LEILAO[tipo_leilao.lower()]}'"
    url += "&$format=text/csv"
    return url


@retry_padrao
def _buscar_csv(url: str) -> bytes:
    resposta = requests.get(url, timeout=10)
    resposta.raise_for_status()
    return resposta.content


def _parsear_df(dados: bytes) -> pl.DataFrame:
    """Lê CSV como strings."""
    if not dados.strip():
        return pl.DataFrame()
    return pl.read_csv(dados, infer_schema=False, null_values=["null"])


def _processar_df(df: pl.DataFrame) -> pl.DataFrame:
    """Filtra, converte tipos e calcula colunas derivadas."""
    # Em 11/06/2024 o BC passou a informar os PUs diretamente nas colunas de cotação
    data_mudanca = dt.date(2024, 6, 11)

    def _duracao_por_linha(linha: dict) -> float:
        tipo = linha["titulo"]
        if tipo == "LTN":
            return linha["dias_uteis"] / 252
        if tipo == "NTN-F":
            return duration_f(
                linha["data_liquidacao"], linha["data_vencimento"], linha["taxa_media"]
            )
        if tipo == "NTN-B":
            return duration_b(
                linha["data_liquidacao"], linha["data_vencimento"], linha["taxa_media"]
            )
        return 0.0

    expr_dv01_unitario = (
        0.0001 * pl.col("pu_medio") * pl.col("duration") / (1 + pl.col("taxa_media"))
    )

    colunas_preco_taxa = ["taxa_media", "taxa_corte", "pu_medio", "pu_corte"]

    return (
        df.filter(pl.col("ofertante") == "Tesouro Nacional")
        # 1. Converter tipos
        .with_columns(
            data_leilao=pl.col("dataMovimento").str.to_date("%Y-%m-%d %H:%M:%S"),
            data_liquidacao=pl.col("dataLiquidacao").str.to_date("%Y-%m-%d %H:%M:%S"),
            data_vencimento=pl.col("dataVencimento").str.to_date("%Y-%m-%d %H:%M:%S"),
            numero_edital=pl.col("edital").cast(pl.Int64),
            tipo_publico=pl.col("tipoPublico"),
            codigo_selic=pl.col("codigoTitulo").cast(pl.Int64),
            tipo_leilao=pl.col("tipoOferta"),
            pu_medio=float_br("cotacaoMedia"),
            pu_corte=float_br("cotacaoCorte"),
            taxa_media=taxa_br("taxaMedia"),
            taxa_corte=taxa_br("taxaCorte"),
            financeiro_total=(float_br("financeiro") * 1_000_000)
            .round(0)
            .cast(pl.Int64),
            quantidade_ofertada_1v=pl.col("quantidadeOfertada").cast(pl.Int64),
            quantidade_aceita_1v=pl.col("quantidadeAceita").cast(pl.Int64),
            quantidade_liquidada_1v=pl.col("quantidadeLiquidada").cast(pl.Int64),
            quantidade_ofertada_2v=pl.col("quantidadeOfertadaSegundaRodada").cast(
                pl.Int64
            ),
            quantidade_aceita_2v=pl.col("quantidadeAceitaSegundaRodada").cast(pl.Int64),
            quantidade_liquidada_2v=pl.col("quantidadeLiquidadaSegundaRodada").cast(
                pl.Int64
            ),
        )
        # 2. Colunas derivadas de primeiro nível
        .with_columns(
            titulo=pl.col("codigo_selic").replace_strict(
                MAPA_TITULOS, return_dtype=pl.String
            ),
            quantidade_ofertada_total=pl.sum_horizontal(
                "quantidade_ofertada_1v", "quantidade_ofertada_2v"
            ),
            quantidade_aceita_total=pl.sum_horizontal(
                "quantidade_aceita_1v", "quantidade_aceita_2v"
            ),
            quantidade_liquidada_total=pl.sum_horizontal(
                "quantidade_liquidada_1v", "quantidade_liquidada_2v"
            ),
            dias_uteis=bday.count_expr("data_liquidacao", "data_vencimento"),
        )
        # 3. Financeiro da 1ª volta (depende de quantidade_aceita_total)
        .with_columns(
            financeiro_1v=pl.when(pl.col("quantidade_aceita_1v") != 0)
            .then(
                (pl.col("quantidade_aceita_1v") / pl.col("quantidade_aceita_total"))
                * pl.col("financeiro_total")
            )
            .otherwise(0)
            .round(0)
            .cast(pl.Int64),
        )
        # 4. Financeiro da 2ª volta e ajuste de PU médio (dependem de financeiro_1v)
        .with_columns(
            financeiro_2v=pl.col("financeiro_total") - pl.col("financeiro_1v"),
            pu_medio=pl.when(
                (pl.col("data_leilao") >= data_mudanca)
                | pl.col("titulo").is_in(["LTN", "NTN-F"])
            )
            .then("pu_medio")
            .otherwise(
                (pl.col("financeiro_1v") / pl.col("quantidade_aceita_1v")).round(6)
            ),
        )
        # 5. Anular taxa/preço onde não houve aceite na 1ª volta
        .with_columns(
            pl.when(pl.col("quantidade_aceita_1v") == 0)
            .then(None)
            .otherwise(pl.col(colunas_preco_taxa))
            .name.keep()
        )
        # 6. Duration
        .with_columns(
            duration=pl.struct(
                "titulo",
                "data_liquidacao",
                "data_vencimento",
                "taxa_media",
                "dias_uteis",
            ).map_elements(_duracao_por_linha, return_dtype=pl.Float64)
        )
        # 7. DV01 e prazo médio (dependem de duration)
        .with_columns(
            dv01_total=expr_dv01_unitario * pl.col("quantidade_aceita_total"),
            dv01_1v=expr_dv01_unitario * pl.col("quantidade_aceita_1v"),
            dv01_2v=expr_dv01_unitario * pl.col("quantidade_aceita_2v"),
            prazo_medio=pl.when(pl.col("titulo") == "LFT")
            .then(pl.col("dias_uteis") / 252)
            .otherwise("duration"),
        )
        # 8. Substituir NaN por None
        .with_columns(cs.float().fill_nan(None))
    )


def _buscar_ptax(df: pl.DataFrame) -> pl.DataFrame:
    """Busca a série histórica da PTAX para o intervalo de datas do DataFrame."""
    data_inicio = df["data_leilao"].min()
    data_fim = df["data_leilao"].max()
    assert isinstance(data_inicio, dt.date)
    assert isinstance(data_fim, dt.date)

    # Garante que pelo menos um dia útil seja buscado
    # Isso é importante caso seja o leilão do dia atual e não haja PTAX ainda
    ultimo_dia_util = bday.last_business_day()
    if data_inicio >= ultimo_dia_util:
        data_inicio = bday.offset(ultimo_dia_util, -1)

    df_ptax = pt.ptax_series(start=data_inicio, end=data_fim)
    if df_ptax.is_empty():
        return pl.DataFrame()

    return (
        df_ptax.select("data", "cotacao_media")
        .rename({"data": "data_ref", "cotacao_media": "ptax"})
        .sort("data_ref")
    )


def _adicionar_dv01_usd(df: pl.DataFrame) -> pl.DataFrame:
    """Busca PTAX e adiciona o DV01 em USD via join_asof."""
    df_ptax = _buscar_ptax(df)
    if df_ptax.is_empty():
        registro.warning("Sem dados de PTAX para calcular DV01 em USD.")
        return df

    return (
        df.sort("data_leilao")
        .join_asof(
            df_ptax, left_on="data_leilao", right_on="data_ref", strategy="backward"
        )
        .with_columns(
            (cs.starts_with("dv01") / pl.col("ptax")).round(2).name.suffix("_usd")
        )
    )


def auctions(
    start: DateLike | None = None,
    end: DateLike | None = None,
    auction_type: Literal["sell", "buy"] | None = None,
) -> pl.DataFrame:
    """
    Recupera dados de leilões para um determinado período e tipo de leilão da API do BC.

    **Consultas de período:**
    - Para consultar dados de um intervalo, forneça as datas de `start` e `end`.
      Exemplo: `auctions(start='2024-10-20', end='2024-10-27')`
    - Se apenas `start` for fornecido, a API do BC retornará dados de leilão a partir
      da data de `start` **até a data mais recente disponível**.
      Exemplo: `auctions(start='2024-10-20')`
    - Se apenas `end` for fornecido, a API do BC retornará dados de leilão **desde a
      data mais antiga disponível até a data de `end`**.
      Exemplo: `auctions(end='2024-10-27')`

    **Série histórica completa:**
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
        shape: (5, 34)
        ┌─────────────┬─────────────────┬─────────────┬───────────────┬───┬─────────────────────────┬───────────────┬───────────────┬──────────────────┐
        │ data_leilao ┆ data_liquidacao ┆ tipo_leilao ┆ numero_edital ┆ … ┆ quantidade_aceita_total ┆ financeiro_1v ┆ financeiro_2v ┆ financeiro_total │
        │ ---         ┆ ---             ┆ ---         ┆ ---           ┆   ┆ ---                     ┆ ---           ┆ ---           ┆ ---              │
        │ date        ┆ date            ┆ str         ┆ i64           ┆   ┆ i64                     ┆ i64           ┆ i64           ┆ i64              │
        ╞═════════════╪═════════════════╪═════════════╪═══════════════╪═══╪═════════════════════════╪═══════════════╪═══════════════╪══════════════════╡
        │ 2025-08-19  ┆ 2025-08-20      ┆ Venda       ┆ 192           ┆ … ┆ 150000                  ┆ 2572400000    ┆ 0             ┆ 2572400000       │
        │ 2025-08-19  ┆ 2025-08-20      ┆ Venda       ┆ 192           ┆ … ┆ 751003                  ┆ 12804476147   ┆ 17123853      ┆ 12821600000      │
        │ 2025-08-19  ┆ 2025-08-20      ┆ Venda       ┆ 193           ┆ … ┆ 300759                  ┆ 1289936461    ┆ 3263539       ┆ 1293200000       │
        │ 2025-08-19  ┆ 2025-08-20      ┆ Venda       ┆ 194           ┆ … ┆ 500542                  ┆ 2071654327    ┆ 2245673       ┆ 2073900000       │
        │ 2025-08-19  ┆ 2025-08-20      ┆ Venda       ┆ 194           ┆ … ┆ 500000                  ┆ 2010700000    ┆ 0             ┆ 2010700000       │
        └─────────────┴─────────────────┴─────────────┴───────────────┴───┴─────────────────────────┴───────────────┴───────────────┴──────────────────┘

    Notes:
        1v = Primeira Volta (Rodada)
        2v = Segunda Volta (Rodada)

    Colunas do DataFrame:
        - data_leilao (Date): data do leilão.
        - data_liquidacao (Date): data de liquidação do leilão.
        - tipo_leilao (String): tipo de leilão (ex: "Venda" ou "Compra").
        - numero_edital (Int64): edital normativo associado ao leilão.
        - tipo_publico (String): categoria do comprador.
        - titulo (String): tipo do título (ex: "LTN", "LFT", "NTN-B", "NTN-F").
        - codigo_selic (Int64): código do título no sistema Selic.
        - data_vencimento (Date): data de vencimento do título.
        - dias_uteis (Int32): dias úteis entre a liquidação e o vencimento.
        - duration (Float64): duração de Macaulay em anos.
        - prazo_medio (Float64): prazo médio do título em anos.
        - pu_medio (Float64): preço unitário médio no leilão.
        - pu_corte (Float64): preço unitário de corte.
        - taxa_media (Float64): taxa de juros média (formato decimal).
        - taxa_corte (Float64): taxa de corte (formato decimal).
        - dv01_1v (Float64): DV01 da 1ª volta em R$.
        - dv01_2v (Float64): DV01 da 2ª volta em R$.
        - dv01_total (Float64): DV01 total do leilão em R$.
        - ptax (Float64): taxa PTAX utilizada na conversão para USD.
        - dv01_1v_usd (Float64): DV01 da 1ª volta em USD.
        - dv01_2v_usd (Float64): DV01 da 2ª volta em USD.
        - dv01_total_usd (Float64): DV01 total em USD.
        - quantidade_liquidada_1v (Int64): quantidade liquidada na 1ª volta.
        - quantidade_liquidada_2v (Int64): quantidade liquidada na 2ª volta.
        - quantidade_liquidada_total (Int64): quantidade total liquidada.
        - quantidade_ofertada_1v (Int64): quantidade ofertada na 1ª volta.
        - quantidade_ofertada_2v (Int64): quantidade ofertada na 2ª volta.
        - quantidade_ofertada_total (Int64): quantidade total ofertada.
        - quantidade_aceita_1v (Int64): quantidade aceita na 1ª volta.
        - quantidade_aceita_2v (Int64): quantidade aceita na 2ª volta.
        - quantidade_aceita_total (Int64): quantidade total aceita.
        - financeiro_1v (Int64): valor financeiro da 1ª volta em R$.
        - financeiro_2v (Int64): valor financeiro da 2ª volta em R$.
        - financeiro_total (Int64): valor financeiro total em R$.
    """
    url = _montar_url(inicio=start, fim=end, tipo_leilao=auction_type)
    dados = _buscar_csv(url)
    df = _parsear_df(dados)
    if df.is_empty():
        return pl.DataFrame()
    df = _processar_df(df)
    df = _adicionar_dv01_usd(df)
    return df.select(ORDEM_COLUNAS_FINAL).sort(CHAVES_ORDENACAO)
