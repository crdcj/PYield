from typing import Literal

import polars as pl
import requests

from pyield._internal.converters import converter_datas, data_referencia_valida
from pyield._internal.data_cache import obter_dataset_cacheado
from pyield._internal.types import DateLike
from pyield.anbima import taxas as _anbima_taxas

TipoTPF = Literal["LFT", "NTN-B", "NTN-C", "LTN", "NTN-F", "PRE"]

_COLUNAS_SAIDA = (
    "titulo",
    "data_referencia",
    "codigo_selic",
    "data_base",
    "data_vencimento",
    "pu",
    "taxa_compra",
    "taxa_venda",
    "taxa_indicativa",
)


def _mapear_tipo_titulo(tipo_titulo: str) -> list[str]:
    tipo_titulo = tipo_titulo.upper()
    mapa_titulos = {
        "PRE": ["LTN", "NTN-F"],
        "NTNB": ["NTN-B"],
        "NTNC": ["NTN-C"],
        "NTNF": ["NTN-F"],
    }
    return mapa_titulos.get(tipo_titulo, [tipo_titulo])


def _obter_historico() -> pl.DataFrame:
    """Carrega o histórico de taxas de TPF usado pela camada de negócio."""
    return obter_dataset_cacheado("tpf")


def _vencimentos_historicos(titulo: TipoTPF) -> pl.DataFrame:
    """Retorna datas de referência e vencimentos históricos por tipo de TPF."""
    df = taxas_historicas(titulo=titulo)
    if df.is_empty():
        return pl.DataFrame(
            schema={"data_referencia": pl.Date, "data_vencimento": pl.Date}
        )

    return (
        df.select("data_referencia", "data_vencimento")
        .unique()
        .sort("data_referencia", "data_vencimento")
    )


def taxas_historicas(
    inicio: DateLike | None = None,
    fim: DateLike | None = None,
    titulo: TipoTPF | None = None,
) -> pl.DataFrame:
    """Consulta o histórico de taxas e preços indicativos de TPFs.

    Fonte: ANBIMA, em painel histórico publicado pela PYield. Os filtros de
    período são inclusivos. Sem argumentos, retorna todo o histórico disponível.

    Args:
        inicio: Data inicial do período. Se omitida, não limita o início.
        fim: Data final do período. Se omitida, não limita o fim.
        titulo: Tipo do título público federal. Aceita ``LFT``, ``NTN-B``,
            ``NTN-C``, ``LTN``, ``NTN-F`` ou ``PRE``.

    Returns:
        DataFrame Polars com o histórico de taxas e preços indicativos. Retorna
        DataFrame vazio se não houver dados para os filtros informados.

    Output Columns:
        * titulo (String): tipo do título público.
        * data_referencia (Date): data de referência dos dados.
        * codigo_selic (Int64): código do título no SELIC.
        * data_base (Date): data base ou de emissão do título.
        * data_vencimento (Date): data de vencimento do título.
        * pu (Float64): preço unitário para liquidação em D0.
        * taxa_compra (Float64): taxa de compra em D0.
        * taxa_venda (Float64): taxa de venda em D0.
        * taxa_indicativa (Float64): taxa indicativa em D0.

    Raises:
        ValueError: Se ``inicio`` for posterior a ``fim``.

    Examples:
        >>> df = yd.tpf.taxas_historicas(
        ...     inicio="01-01-2025", fim="31-01-2025", titulo="PRE"
        ... )
    """
    data_inicio = converter_datas(inicio) if inicio is not None else None
    data_fim = converter_datas(fim) if fim is not None else None
    if data_inicio is not None and data_fim is not None and data_inicio > data_fim:
        msg = "inicio deve ser menor ou igual a fim."
        raise ValueError(msg)

    df = _obter_historico()
    if df.is_empty():
        return df

    if data_inicio is not None:
        df = df.filter(pl.col("data_referencia") >= data_inicio)
    if data_fim is not None:
        df = df.filter(pl.col("data_referencia") <= data_fim)
    if titulo:
        tipos_titulo = _mapear_tipo_titulo(titulo)
        df = df.filter(pl.col("titulo").is_in(tipos_titulo))

    return df.select(_COLUNAS_SAIDA).sort(
        "data_referencia", "titulo", "data_vencimento"
    )


def taxas(
    data: DateLike,
    titulo: TipoTPF | None = None,
) -> pl.DataFrame:
    """Busca taxas e preços indicativos de TPFs.

    Fonte: ANBIMA. Primeiro consulta o cache local de dados históricos; se a
    data não estiver no cache, busca diretamente na fonte da ANBIMA.

    Args:
        data: Data de referência.
        titulo: Tipo do título público federal. Aceita ``LFT``, ``NTN-B``,
            ``NTN-C``, ``LTN``, ``NTN-F`` ou ``PRE``.

    Returns:
        DataFrame Polars com taxas e preços indicativos. Retorna DataFrame
        vazio se não houver dados para a data.

    Output Columns:
        * titulo (String): tipo do título público.
        * data_referencia (Date): data de referência dos dados.
        * codigo_selic (Int64): código do título no SELIC.
        * data_base (Date): data base ou de emissão do título.
        * data_vencimento (Date): data de vencimento do título.
        * pu (Float64): preço unitário para liquidação em D0.
        * taxa_compra (Float64): taxa de compra em D0.
        * taxa_venda (Float64): taxa de venda em D0.
        * taxa_indicativa (Float64): taxa indicativa em D0.

    Examples:
        >>> df = yd.tpf.taxas(data="06-02-2026")
    """
    data = converter_datas(data)

    if not data_referencia_valida(data):
        return pl.DataFrame()

    try:
        df = _obter_historico()
    except (requests.exceptions.RequestException, pl.exceptions.PolarsError):
        df = pl.DataFrame()
    if not df.is_empty():
        df = df.filter(pl.col("data_referencia") == data)
    if df.is_empty():
        df = _anbima_taxas.buscar(data)

    if df.is_empty():
        return pl.DataFrame()

    df = df.select(col for col in _COLUNAS_SAIDA if col in df.columns)
    if titulo:
        tipos_titulo = _mapear_tipo_titulo(titulo)
        df = df.filter(pl.col("titulo").is_in(tipos_titulo))

    return df.sort("data_referencia", "titulo", "data_vencimento")


def vencimentos(
    data: DateLike,
    titulo: TipoTPF,
) -> pl.Series:
    """Busca vencimentos de TPFs disponíveis nas taxas indicativas.

    Fonte: ANBIMA, mesma base usada por ``yd.tpf.taxas``.

    Args:
        data: Data de referência.
        titulo: Tipo do título público federal. Aceita ``LFT``, ``NTN-B``,
            ``NTN-C``, ``LTN``, ``NTN-F`` ou ``PRE``.

    Returns:
        Series ordenada com os vencimentos disponíveis.

    Examples:
        >>> yd.tpf.vencimentos(data="22-08-2025", titulo="PRE")
        shape: (18,)
        Series: 'data_vencimento' [date]
        [
            2025-10-01
            2026-01-01
            2026-04-01
            2026-07-01
            2026-10-01
            …
            2030-01-01
            2031-01-01
            2032-01-01
            2033-01-01
            2035-01-01
        ]
    """
    return taxas(data, titulo)["data_vencimento"].unique().sort()
