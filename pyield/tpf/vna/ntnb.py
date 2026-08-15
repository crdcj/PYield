"""Valores oficiais e projetados de VNA da NTN-B."""

import datetime as dt
import math
from decimal import Decimal

import polars as pl

import pyield._internal.converters as conversores
from pyield._internal.numbers import truncar, truncar_decimal
from pyield._internal.types import DateLike, any_is_empty
from pyield.ipca import historico as _ipca
from pyield.tpf.vna import _download
from pyield.tpf.vna import calculo as _vna

_DIA_INICIO_VIGENCIA = 15
_QTD_MARCOS_VIGENCIA = 2
_URL_PUBLICACAO = (
    "https://www.tesourotransparente.gov.br/publicacoes/valor-nominal-de-ntn-b/"
)


def _processar(df_bruto: pl.DataFrame) -> pl.DataFrame:
    """Normaliza as duas colunas da planilha de NTN-B."""
    return (
        df_bruto.select(
            data=_vna.expressao_data(),
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


def vna(data: DateLike | None = None) -> Decimal:
    """Obtém o VNA da NTN-B em uma data de referência.

    Em datas de referência oficiais, retorna o valor publicado pelo Tesouro
    Nacional. Entre duas referências publicadas, calcula o VNA por pró-rata
    exponencial em dias corridos com os números-índice do IPCA publicados pelo
    IBGE. Não realiza projeções após a última referência disponível.

    Args:
        data: Data de referência. Os valores oficiais são mensais e referem-se
            ao dia 15. Se omitida ou nula, retorna ``Decimal('NaN')``.

    Returns:
        Decimal: VNA da NTN-B com seis casas decimais. Retorna
            ``Decimal('NaN')`` quando a data estiver fora do intervalo
            publicado.

    Examples:
        >>> from pyield import ntnb
        >>> ntnb.vna("15-12-2025")  # ponto publicado
        Decimal('4570.078408')
        >>> ntnb.vna("30-12-2025")  # pró-rata entre pontos publicados
        Decimal('4577.369436')
        >>> ntnb.vna("13-08-2026")  # precisão dos índices do IPCA
        Decimal('4742.530180')
    """
    if any_is_empty(data):
        return Decimal("NaN")
    data_convertida = conversores.converter_datas(data)
    if data_convertida is None:
        return Decimal("NaN")
    df = vnas()
    ponto_exato = df.filter(pl.col("data") == data_convertida)
    if ponto_exato.height == 1:
        return truncar_decimal(ponto_exato.item(0, "vna"), 6)

    inicio, fim = _obter_vigencia(data_convertida)
    pontos_vigencia = df.filter(pl.col("data").is_in([inicio, fim]))
    if pontos_vigencia.height != _QTD_MARCOS_VIGENCIA:
        return Decimal("NaN")

    fator_ipca = _obter_fator_ipca(inicio, fim)
    if math.isnan(fator_ipca):
        return Decimal("NaN")
    return truncar_decimal(
        _vna.calcular_vna(
            df,
            data_convertida,
            fator_variacao=fator_ipca,
        ),
        6,
    )


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


def _obter_fator_ipca(inicio: dt.date, fim: dt.date) -> float:
    """Obtém o fator entre os números-índice que atualizam a vigência."""
    mes_inicial = inicio.replace(day=1) - dt.timedelta(days=1)
    mes_final = fim.replace(day=1) - dt.timedelta(days=1)
    df = _ipca.indices(mes_inicial, mes_final)
    periodo_inicial = int(mes_inicial.strftime("%Y%m"))
    periodo_final = int(mes_final.strftime("%Y%m"))
    indice_inicial = df.filter(pl.col("periodo") == periodo_inicial)
    indice_final = df.filter(pl.col("periodo") == periodo_final)
    if indice_inicial.is_empty() or indice_final.is_empty():
        return float("nan")
    fator = indice_final.item(0, "indice") / indice_inicial.item(0, "indice")
    return truncar(fator, 16)


def vna_projetado(
    data: DateLike,
    vna_base: float | Decimal,
    inflacao: float | Decimal,
) -> Decimal:
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
        Decimal: VNA projetado, truncado em seis casas decimais. Retorna
            ``Decimal('NaN')`` se alguma entrada for nula ou vazia.

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
        Decimal('4731.856412')
        >>> ntnb.vna_projetado("30-06-2026", 4731.856412, 0.45)
        Decimal('4742.491138')
        >>> ntnb.vna_projetado("21-05-2008", 1726.9264599, 0.464)
        Decimal('1728.461136')
    """
    if any_is_empty(data, vna_base, inflacao):
        return Decimal("NaN")
    if inflacao <= _vna.LIMITE_INFERIOR_PERCENTUAL:
        raise ValueError("A inflação deve ser maior que -100%.")
    data_convertida = conversores.converter_datas(data)
    inicio, fim = _obter_vigencia(data_convertida)
    expoente = (data_convertida - inicio).days / (fim - inicio).days
    return truncar_decimal(
        _vna.calcular_vna_projetado(float(vna_base), float(inflacao), expoente),
        6,
    )
