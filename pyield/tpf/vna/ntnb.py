"""Valores oficiais e projetados de VNA da NTN-B."""

import datetime as dt

import polars as pl

import pyield._internal.converters as conversores
from pyield._internal.types import DateLike, any_is_empty
from pyield.tpf.vna import _download, _utils

_DIA_INICIO_VIGENCIA = 15
_URL_PUBLICACAO = (
    "https://www.tesourotransparente.gov.br/publicacoes/valor-nominal-de-ntn-b/"
)


def _processar(df_bruto: pl.DataFrame) -> pl.DataFrame:
    """Normaliza as duas colunas da planilha de NTN-B."""
    return (
        df_bruto.select(
            data=_utils.expressao_data(),
            vna=pl.col("column_2").cast(pl.Float64, strict=False),
        )
        .filter(pl.col("data").is_not_null(), pl.col("vna").is_not_null())
        .unique(subset="data", keep="last")
        .sort("data")
    )


def vnas() -> pl.DataFrame:
    """Busca os VNAs oficiais publicados para a NTN-B.

    Fonte: Tesouro Nacional, publicação "Valor Nominal de NTN-B" no portal
    Tesouro Transparente. Os valores são mensais e referem-se ao dia 15.

    Returns:
        DataFrame Polars com o histórico oficial.

    Output Columns:
        - data (Date): Data de referência do VNA.
        - vna (Float64): Valor nominal atualizado da NTN-B.
    """
    conteudo = _download.baixar_planilha(_URL_PUBLICACAO)
    return _processar(_download.ler_planilha(conteudo, "NTNB"))


def vna(data: DateLike | None = None) -> float:
    """Obtém o VNA da NTN-B em uma data de referência.

    Em datas de referência oficiais, retorna o valor publicado pelo Tesouro
    Nacional. Entre duas referências publicadas, calcula o VNA por pró-rata
    exponencial em dias corridos. Não realiza projeções após a última
    referência disponível.

    Args:
        data: Data de referência. Os valores oficiais são mensais e referem-se
            ao dia 15. Se omitida ou nula, retorna ``nan``.

    Returns:
        VNA da NTN-B, truncado em seis casas quando calculado por pró-rata.
        Retorna ``nan`` quando a data estiver fora do intervalo publicado.

    Examples:
        >>> from pyield import ntnb
        >>> ntnb.vna("15-12-2025")  # ponto publicado
        4570.078408
        >>> ntnb.vna("30-12-2025")  # pró-rata entre pontos publicados
        4577.369436
    """
    if any_is_empty(data):
        return float("nan")
    data_convertida = conversores.converter_datas(data)
    if data_convertida is None:
        return float("nan")
    return _utils.calcular_vna(vnas(), data_convertida)


def _obter_vigencia(data: dt.date) -> tuple[dt.date, dt.date]:
    """Obtém a vigência mensal 15--15 que contém a data."""
    if data.day >= _DIA_INICIO_VIGENCIA:
        inicio = data.replace(day=_DIA_INICIO_VIGENCIA)
        fim = (inicio + dt.timedelta(days=32)).replace(day=_DIA_INICIO_VIGENCIA)
    else:
        fim = data.replace(day=_DIA_INICIO_VIGENCIA)
        inicio = (fim.replace(day=1) - dt.timedelta(days=1)).replace(
            day=_DIA_INICIO_VIGENCIA
        )
    return inicio, fim


def vna_projetado(
    data: DateLike,
    vna_base: float,
    inflacao: float,
) -> float:
    """Calcula o VNA projetado da NTN-B por pró-rata exponencial.

    O VNA-base deve corresponder ao início da vigência mensal que contém a
    data. Para a NTN-B, cada vigência começa no dia 15 e termina no dia 15 do
    mês seguinte. A projeção é distribuída exponencialmente em dias corridos.

    Args:
        data: Data para a qual o VNA será projetado.
        vna_base: VNA oficial no início da vigência.
        inflacao: Inflação mensal projetada em percentual. Por exemplo,
            ``0.45`` representa 0,45%.

    Returns:
        VNA projetado, truncado em seis casas decimais. Retorna ``nan`` se
        alguma entrada for nula ou vazia.

    Notes:
        Conforme a metodologia da STN, o VNA-base é truncado em seis casas,
        a projeção é arredondada em duas e o pró-rata é truncado em catorze.

    References:
        - https://crdcj.github.io/PYield/referencias/metodologia-calculo-tpf-stn/

    Raises:
        ValueError: Se o VNA-base não for positivo ou a inflação for menor ou
            igual a -100%.

    Examples:
        >>> from pyield import ntnb
        >>> ntnb.vna_projetado("15-06-2026", 4731.856412, 0.45)
        4731.856412
        >>> ntnb.vna_projetado("30-06-2026", 4731.856412, 0.45)
        4742.491138
        >>> ntnb.vna_projetado("21-05-2008", 1726.9264599, 0.464)
        1728.461136
    """
    if any_is_empty(data, vna_base, inflacao):
        return float("nan")
    if inflacao <= _utils.LIMITE_INFERIOR_PERCENTUAL:
        raise ValueError("A inflação deve ser maior que -100%.")
    data_convertida = conversores.converter_datas(data)
    inicio, fim = _obter_vigencia(data_convertida)
    expoente = (data_convertida - inicio).days / (fim - inicio).days
    return _utils.calcular_vna_projetado(vna_base, inflacao, expoente)
