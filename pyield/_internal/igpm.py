"""Números-índice do IGP-M da FGV disponibilizados pelo Ipeadata."""

import json
from decimal import Decimal

import polars as pl
import requests

from pyield import relogio
from pyield._internal.cache import ttl_cache
from pyield._internal.converters import converter_datas
from pyield._internal.retry import retry_padrao
from pyield._internal.types import DateLike, any_is_empty

_URL_INDICES = (
    "https://www.ipeadata.gov.br/api/odata4/ValoresSerie(SERCODIGO='IGP12_IGPM12')"
)
_TIPO_INDICE = pl.Decimal(precision=18, scale=10)
_ESQUEMA = {"periodo": pl.Int64, "indice": _TIPO_INDICE}


@ttl_cache()
@retry_padrao
def _buscar_historico() -> bytes:
    """Busca o JSON bruto da série de números-índice do IGP-M."""
    resposta = requests.get(_URL_INDICES, timeout=30)
    resposta.raise_for_status()
    return resposta.content


def _parsear_historico(conteudo: bytes) -> list[dict[str, str | Decimal | None]]:
    """Decodifica os números do JSON diretamente como Decimal."""
    return json.loads(conteudo, parse_float=Decimal)["value"]


def _processar_historico(dados: list[dict[str, str | Decimal | None]]) -> pl.DataFrame:
    """Normaliza as competências mensais e preserva a precisão dos índices."""
    return (
        pl.from_dicts(dados, schema={"VALDATA": pl.String, "VALVALOR": _TIPO_INDICE})
        .select(
            periodo=pl.col("VALDATA")
            .str.slice(0, 10)
            .str.to_date("%Y-%m-%d")
            .dt.strftime("%Y%m")
            .cast(pl.Int64),
            indice=pl.col("VALVALOR"),
        )
        .drop_nulls("indice")
        .sort("periodo")
    )


def indice_serie(
    inicio: DateLike | None = None,
    fim: DateLike | None = None,
    *,
    ultimos: int | None = None,
) -> pl.DataFrame:
    """Obtém os números-índice mensais do IGP-M por intervalo ou últimos meses.

    Fonte: FGV/Conjuntura Econômica, disponibilizada pelo Ipeadata na série
    ``IGP12_IGPM12``, com base agosto de 1994 = 100.

    Args:
        inicio: Qualquer data no primeiro mês do intervalo.
        fim: Qualquer data no último mês do intervalo. Se ``None``, usa a
            data atual no Brasil.
        ultimos: Número de meses mais recentes a recuperar. Se informado,
            tem prioridade sobre ``inicio`` e ``fim``.

    Returns:
        DataFrame com os números-índice em ordem crescente de competência.
        Retorna vazio, preservando o esquema, se não houver dados no intervalo.

    Output Columns:
        * periodo (Int64): competência mensal no formato YYYYMM.
        * indice (Decimal(18, 10)): número-índice do IGP-M.

    Notes:
        Os números do JSON são lidos diretamente como Decimal, sem passagem
        por float. As dez casas preservam também os valores anteriores a 1994,
        que podem ser menores que 0.001. Valores ausentes são omitidos.

    Raises:
        ValueError: Se não informar ``inicio`` nem ``ultimos``, se ``ultimos``
            for menor ou igual a zero ou se alguma data for inválida.

    Examples:
        >>> yd.igpm.indice_serie("01-06-2000", "31-07-2000").to_dicts()
        [{'periodo': 200006, 'indice': Decimal('183.7450000000')},
         {'periodo': 200007, 'indice': Decimal('186.6340000000')}]
    """
    if ultimos is not None:
        if ultimos <= 0:
            raise ValueError("O número de meses deve ser maior que 0.")
        return _processar_historico(_parsear_historico(_buscar_historico())).tail(
            ultimos
        )
    if inicio is None:
        raise ValueError("Informe 'inicio' ou 'ultimos'.")
    data_inicio = converter_datas(inicio)
    data_fim = converter_datas(fim) if fim is not None else relogio.hoje()
    if data_inicio is None or data_fim is None:
        raise ValueError("As datas do intervalo devem ser informadas.")
    periodo_inicio = data_inicio.year * 100 + data_inicio.month
    periodo_fim = data_fim.year * 100 + data_fim.month
    if periodo_inicio > periodo_fim:
        return pl.DataFrame(schema=_ESQUEMA)
    return _processar_historico(_parsear_historico(_buscar_historico())).filter(
        pl.col("periodo").is_between(periodo_inicio, periodo_fim)
    )


def indice(data: DateLike | None) -> Decimal:
    """Obtém o número-índice do IGP-M para um mês específico.

    Fonte: FGV/Conjuntura Econômica, disponibilizada pelo Ipeadata na série
    ``IGP12_IGPM12``, com base agosto de 1994 = 100.

    Args:
        data: Qualquer data dentro do mês desejado, nos formatos da biblioteca.

    Returns:
        Decimal: Número-índice com dez casas decimais ou ``Decimal('NaN')``
        se a data for nula, vazia ou o índice não estiver disponível.

    Examples:
        >>> yd.igpm.indice("01-06-2000")
        Decimal('183.7450000000')
        >>> yd.igpm.indice("01-06-1989")
        Decimal('0.0000308619')
    """
    if data is None or any_is_empty(data):
        return Decimal("NaN")
    df = indice_serie(data, data)
    if df.is_empty():
        return Decimal("NaN")
    return df.item(0, "indice")
