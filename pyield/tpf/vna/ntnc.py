"""Valores oficiais e projetados de VNA da NTN-C."""

import datetime as dt

import polars as pl

import pyield._internal.converters as conversores
from pyield._internal.types import DateLike, any_is_empty
from pyield.tpf.vna import _download, _utils

_URL_PUBLICACAO = (
    "https://www.tesourotransparente.gov.br/publicacoes/valor-nominal-de-ntn-c/"
)
_ANOS_VENCIMENTO = {
    "column_2": [2005, 2008, 2011, 2017, 2021, 2031],
    "column_3": [2002, 2006],
}


def _processar(df_bruto: pl.DataFrame) -> pl.DataFrame:
    """Normaliza as duas séries de vencimentos existentes para a NTN-C."""
    series = []
    for coluna, anos in _ANOS_VENCIMENTO.items():
        serie = df_bruto.select(
            data=_utils.expressao_data(),
            anos_vencimento=pl.lit(anos, dtype=pl.List(pl.Int64)),
            vna=pl.col(coluna).cast(pl.Float64, strict=False),
        ).filter(
            pl.col("data").is_not_null(),
            pl.col("vna").is_not_null(),
            pl.col("vna") > 0,
        )
        series.append(serie)
    return (
        pl.concat(series)
        .unique(subset=["data", "anos_vencimento"], keep="last")
        .sort("data", "anos_vencimento")
    )


def vnas() -> pl.DataFrame:
    """Busca os VNAs oficiais publicados para a NTN-C.

    Fonte: Tesouro Nacional, publicação "Valor Nominal de NTN-C" no portal
    Tesouro Transparente. A planilha possui séries distintas conforme o ano de
    vencimento do título.

    Returns:
        DataFrame Polars com o histórico oficial.

    Output Columns:
        - data (Date): Data de referência do VNA.
        - anos_vencimento (List[Int64]): Anos de vencimento aos quais o VNA se
            aplica.
        - vna (Float64): Valor nominal atualizado da NTN-C.
    """
    conteudo = _download.baixar_planilha(_URL_PUBLICACAO)
    return _processar(_download.ler_planilha(conteudo, "NTN-C"))


def vna(
    data: DateLike | None = None,
    vencimento: DateLike | None = None,
) -> float:
    """Obtém o VNA da NTN-C em uma data de referência.

    A fonte possui séries diferentes conforme o vencimento. Por isso, o
    vencimento é necessário para selecionar o VNA correto. Em datas de
    referência oficiais, retorna o valor publicado; entre duas referências
    publicadas, calcula o pró-rata exponencial em dias corridos.

    Args:
        data: Data de referência. Os valores oficiais são mensais e referem-se
            ao primeiro dia do mês. Se omitida ou nula, retorna ``nan``.
        vencimento: Data de vencimento da NTN-C. Se omitida ou nula, retorna
            ``nan``.

    Returns:
        VNA da NTN-C, truncado em seis casas quando calculado por pró-rata.
        Retorna ``nan`` quando a data estiver fora do intervalo publicado ou
        não houver série para o vencimento.

    Examples:
        >>> from pyield import ntnc
        >>> ntnc.vna("01-12-2025", "01-01-2031")  # ponto publicado
        6450.107485
        >>> ntnc.vna("16-12-2025", "01-01-2031")  # pró-rata entre pontos
        6449.641358
    """
    if any_is_empty(data, vencimento):
        return float("nan")
    data_convertida = conversores.converter_datas(data)
    vencimento_convertido = conversores.converter_datas(vencimento)
    if data_convertida is None or vencimento_convertido is None:
        return float("nan")

    df = vnas().filter(
        pl.col("anos_vencimento").list.contains(vencimento_convertido.year)
    )
    return _utils.calcular_vna(df, data_convertida)


def _obter_vigencia(data: dt.date) -> tuple[dt.date, dt.date]:
    """Obtém a vigência mensal entre primeiros dias que contém a data."""
    inicio = data.replace(day=1)
    fim = (inicio + dt.timedelta(days=32)).replace(day=1)
    return inicio, fim


def vna_projetado(
    data: DateLike,
    vna_base: float,
    inflacao: float,
) -> float:
    """Calcula o VNA projetado da NTN-C por pró-rata exponencial.

    O VNA-base deve corresponder ao primeiro dia do mês que contém a data. A
    projeção do IGP-M é distribuída exponencialmente pelos dias corridos até o
    primeiro dia do mês seguinte.

    Args:
        data: Data para a qual o VNA será projetado.
        vna_base: VNA oficial no início da vigência.
        inflacao: Inflação mensal projetada em percentual. Por exemplo,
            ``0.30`` representa 0,30%.

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
        >>> from pyield import ntnc
        >>> ntnc.vna_projetado("01-06-2026", 6693.537239, 0.30)
        6693.537239
        >>> ntnc.vna_projetado("16-06-2026", 6693.537239, 0.30)
        6703.570025
        >>> ntnc.vna_projetado("21-05-2008", 2102.8055189, 1.754)
        2126.473734
    """
    if any_is_empty(data, vna_base, inflacao):
        return float("nan")
    if inflacao <= _utils.LIMITE_INFERIOR_PERCENTUAL:
        raise ValueError("A inflação deve ser maior que -100%.")
    data_convertida = conversores.converter_datas(data)
    inicio, fim = _obter_vigencia(data_convertida)
    expoente = (data_convertida - inicio).days / (fim - inicio).days
    return _utils.calcular_vna_projetado(vna_base, inflacao, expoente)
