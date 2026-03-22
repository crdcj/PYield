from typing import Literal

import polars as pl
import requests

from pyield._internal.br_numbers import float_br, inteiro_m, taxa_br
from pyield._internal.cache import ttl_cache
from pyield._internal.retry import retry_padrao

TiposIMA = Literal[
    "IRF-M 1",
    "IRF-M 1+",
    "IRF-M",
    "IMA-B 5",
    "IMA-B 5+",
    "IMA-B",
    "IMA-S",
    "IMA-GERAL-EX-C",
    "IMA-GERAL",
]


URL_ULTIMO_IMA = "https://www.anbima.com.br/informacoes/ima/arqs/ima_completo.txt"


@ttl_cache()
@retry_padrao
def _buscar_texto_ultimo_ima() -> str:
    resposta = requests.get(URL_ULTIMO_IMA, timeout=3)
    resposta.raise_for_status()
    resposta.encoding = "latin1"
    return resposta.text


def _parsear_df(texto: str) -> pl.DataFrame:
    texto_csv = texto.split("2@COMPOSIÇÃO DE CARTEIRA")[1].strip()
    return pl.read_csv(
        texto_csv.encode(),
        separator="@",
        infer_schema=False,
        null_values="--",
    )


def _processar_df(df: pl.DataFrame) -> pl.DataFrame:
    mduration_expr = pl.col("duration") / (1 + pl.col("taxa_indicativa"))
    dv01_mercado_expr = pl.col("dv01") * pl.col("quantidade_mercado")
    df = (
        df.with_columns(
            pu=float_br("PU (R$)"),
            duration=pl.col("Duration (d.u.)").cast(pl.Int64).truediv(252),
            taxa_indicativa=taxa_br("Taxa Indicativa (% a.a.)", 4),
            quantidade_mercado=inteiro_m("Quantidade (1.000 títulos)"),
        )
        .with_columns(dv01=0.0001 * pl.col("pu") * mduration_expr)
        .select(
            data_referencia=pl.col("Data de Referência").str.to_date("%d/%m/%Y"),
            indice=pl.col("INDICE"),
            titulo=pl.col("Títulos"),
            data_vencimento=pl.col("Data de Vencimento").str.to_date("%d/%m/%Y"),
            codigo_selic=pl.col("Código SELIC").cast(pl.Int64),
            isin=pl.col("Código ISIN"),
            dias_uteis=pl.col("Prazo (d.u.)").cast(pl.Int64),
            duration=pl.col("duration"),
            taxa_indicativa=pl.col("taxa_indicativa"),
            pu=pl.col("pu"),
            pu_juros=float_br("PU de Juros (R$)"),
            dv01=pl.col("dv01"),
            pmr=float_br("PMR"),
            peso=float_br("Peso (%)"),
            convexidade=float_br("Convexidade"),
            quantidade_teorica=float_br("Quantidade Teórica (1.000 títulos)"),
            operacoes=pl.col("Número de Operações *").cast(pl.Int64),
            quantidade_negociada=inteiro_m("Quant. Negociada (1.000 títulos) *"),
            valor_negociado=inteiro_m("Valor Negociado (R$ mil) *"),
            dv01_mercado=dv01_mercado_expr.round(0).cast(pl.Int64),
            quantidade_mercado=pl.col("quantidade_mercado"),
            valor_mercado=inteiro_m("Carteira a Mercado (R$ mil)"),
        )
    )
    return df


def last_ima(ima_type: TiposIMA | None = None) -> pl.DataFrame:
    """Obtém os últimos dados de composição de carteira IMA disponíveis na ANBIMA.

    Busca e processa os dados do arquivo IMA completo publicado pela ANBIMA,
    retornando um DataFrame estruturado.

    Args:
        ima_type (str, optional): Tipo de índice IMA para filtrar os dados.
            Se None, retorna todos os índices. Padrão é None.

    Returns:
        pl.DataFrame: DataFrame com os dados do IMA.

    Output Columns:
        - data_referencia (Date): data de referência.
        - indice (String): índice IMA (ex: 'IMA-B', 'IRF-M').
        - titulo (String): título (ex: 'LTN', 'NTN-B').
        - data_vencimento (Date): data de vencimento do título.
        - codigo_selic (Int64): código do título no sistema SELIC.
        - isin (String): código ISIN.
        - dias_uteis (Int64): dias úteis até o vencimento.
        - duration (Float64): duration do título em anos úteis (252 d.u./ano).
        - taxa_indicativa (Float64): taxa indicativa em decimal (ex: 0.10 para 10%).
        - pu (Float64): preço unitário (PU) em R$.
        - pu_juros (Float64): PU de juros em R$.
        - dv01 (Float64): DV01 em R$.
        - pmr (Float64): prazo médio de repactuação.
        - peso (Float64): peso do título no índice (%).
        - convexidade (Float64): convexidade do título.
        - quantidade_teorica (Float64): quantidade teórica (em 1.000 títulos).
        - operacoes (Int64): número de operações.
        - quantidade_negociada (Int64): quantidade negociada (unidades).
        - valor_negociado (Int64): valor negociado em R$.
        - dv01_mercado (Int64): DV01 de mercado em R$.
        - quantidade_mercado (Int64): quantidade em carteira (unidades).
        - valor_mercado (Int64): valor de mercado em R$.

    Examples:
        >>> yd.anbima.last_ima("IMA-B")  # doctest: +SKIP
    """
    texto_ima = _buscar_texto_ultimo_ima()
    df = _parsear_df(texto_ima)
    df = _processar_df(df)
    if ima_type:
        df = df.filter(pl.col("indice") == ima_type)
    return df.sort("indice", "titulo", "data_vencimento")
