"""Calendário automático do Copom a partir das atas e da agenda ICS do BCB."""

import json
import re

import polars as pl
import requests

from pyield import du, relogio
from pyield._internal.cache import ttl_cache
from pyield._internal.converters import converter_datas
from pyield._internal.retry import retry_padrao
from pyield._internal.types import DateLike

_URL_ATAS = "https://www.bcb.gov.br/api/servico/sitebcb/copom/atas"
_URL_ICS = (
    "https://www.bcb.gov.br/api/exportarics/sitebcb/agendaics"
    "?lista=Reuni%C3%B5es%20do%20Copom"
)
_DIAS_POR_REUNIAO = 2
_SCHEMA = {
    "nro_reuniao": pl.Int64,
    "data_inicio": pl.Date,
    "data_decisao": pl.Date,
    "data_publicacao": pl.Date,
    "data_efetividade": pl.Date,
    "status": pl.String,
    "titulo": pl.String,
    "fonte": pl.String,
}


@ttl_cache(ttl=8 * 60 * 60)
@retry_padrao
def _buscar_atas() -> bytes:
    resposta = requests.get(_URL_ATAS, params={"quantidade": 500}, timeout=30)
    resposta.raise_for_status()
    return resposta.content


@ttl_cache(ttl=8 * 60 * 60)
@retry_padrao
def _buscar_calendario() -> bytes:
    resposta = requests.get(_URL_ICS, timeout=30)
    resposta.raise_for_status()
    return resposta.content


def _parsear_atas(conteudo: bytes) -> pl.DataFrame:
    return pl.from_dicts(
        json.loads(conteudo)["conteudo"],
        schema={
            "nroReuniao": pl.Int64,
            "dataReferencia": pl.String,
            "dataPublicacao": pl.String,
            "titulo": pl.String,
        },
    )


def _parsear_calendario(conteudo: bytes) -> pl.DataFrame:
    texto = re.sub(r"\r?\n[ \t]", "", conteudo.decode("utf-8-sig"))
    if not texto.startswith("BEGIN:VCALENDAR") or "END:VCALENDAR" not in texto:
        raise ValueError("Calendário ICS inválido: VCALENDAR ausente.")
    datas = []
    for evento in re.findall(r"BEGIN:VEVENT\r?\n(.*?)END:VEVENT", texto, re.S):
        inicio = re.search(
            r"^DTSTART(?:;[^:]+)?:(\d{8})(?:T\d{6}Z?)?\r?$", evento, re.M
        )
        if inicio is None:
            raise ValueError("Evento do calendário ICS sem DTSTART válido.")
        datas.append(inicio[1])
    return pl.DataFrame({"data": datas}, schema={"data": pl.String})


def _processar_atas(dados: pl.DataFrame) -> pl.DataFrame:
    return dados.select(
        nro_reuniao=pl.col("nroReuniao"),
        data_decisao=pl.col("dataReferencia").str.to_date("%Y-%m-%d"),
        data_publicacao=pl.col("dataPublicacao").str.to_date("%Y-%m-%d"),
        titulo=pl.col("titulo"),
    ).unique("data_decisao", keep="first")


def _processar_calendario(dados: pl.DataFrame) -> pl.DataFrame:
    reunioes = (
        dados.with_columns(data=pl.col("data").str.to_date("%Y%m%d"))
        .unique()
        .sort("data")
        .with_columns(
            grupo=(pl.col("data").diff().dt.total_days() != 1).fill_null(True).cum_sum()
        )
        .group_by("grupo")
        .agg(
            data_inicio=pl.col("data").first(),
            data_decisao=pl.col("data").last(),
            dias=pl.len(),
        )
    )
    if reunioes.filter(pl.col("dias") != _DIAS_POR_REUNIAO).height:
        raise ValueError("Calendário ICS contém reunião sem dois dias consecutivos.")
    return reunioes.select("data_inicio", "data_decisao")


def calendario(
    inicio: DateLike | None = None,
    fim: DateLike | None = None,
) -> pl.DataFrame:
    """Consulta reuniões históricas e agendadas do Copom nas fontes do BCB.

    Combina a API de atas históricas com o calendário ICS de reuniões do BCB,
    sem datas anuais fixas. As atas têm prioridade na data de decisão; o ICS
    complementa o início e mantém reuniões ainda sem ata, inclusive passadas.

    Args:
        inicio: Limite inicial inclusivo para a data de decisão. Sem limite
            quando None. Aceita DD-MM-YYYY, DD/MM/YYYY, YYYY-MM-DD e date.
        fim: Limite final inclusivo para a data de decisão, nos mesmos formatos.
            Sem limite quando None.

    Returns:
        DataFrame ordenado por data_decisao, sem duplicatas nessa coluna.
        Consultas sem registros retornam DataFrame vazio com o mesmo esquema.

    Output Columns:
        nro_reuniao (Int64): Número sequencial informado na ata; nulo sem ata.
        data_inicio (Date): Primeiro dia informado no ICS; nulo fora da agenda.
        data_decisao (Date): Último dia da reunião, dataReferencia nas atas.
        data_publicacao (Date): Publicação da ata; nula quando não disponível.
        data_efetividade (Date): Próximo dia útil após a decisão, calculado pelo
            calendário brasileiro de yd.du; data de vencimento/liquidação CPM.
        status (String): "Realizada" se a decisão for até hoje no Brasil;
            "A realizar" caso contrário. Classificação por data, não por ata.
        titulo (String): Título original da ata; nulo quando não disponível.
        fonte (String): "Ata" ou "Calendário", conforme a origem da decisão.

    Notes:
        O horizonte depende das datas publicadas pelo BCB. A consulta às atas
        solicita até 500 registros. O conteúdo bruto fica em cache por oito
        horas; o status é recalculado a cada chamada. Erros de rede e payload
        inválido são propagados. O ICS publica um evento por dia: dias únicos
        consecutivos devem formar pares; grupos incompletos geram ValueError.
        O início histórico não é inferido subtraindo um dia da decisão.

    Examples:
        >>> cal = yd.copom.calendario(inicio="01-01-2026")  # doctest: +SKIP
        >>> cal["data_decisao"].is_sorted()  # doctest: +SKIP
        True
    """
    inicio = converter_datas(inicio)
    fim = converter_datas(fim)
    if inicio is not None and fim is not None and inicio > fim:
        raise ValueError("inicio deve ser anterior ou igual a fim.")
    atas = _processar_atas(_parsear_atas(_buscar_atas()))
    agenda = _processar_calendario(_parsear_calendario(_buscar_calendario()))
    df = (
        atas.join(agenda, on="data_decisao", how="full", coalesce=True)
        .with_columns(
            data_efetividade=du.deslocar_expr("data_decisao", 1),
            status=pl.when(pl.col("data_decisao") <= relogio.hoje())
            .then(pl.lit("Realizada"))
            .otherwise(pl.lit("A realizar")),
            fonte=pl.when(pl.col("nro_reuniao").is_not_null())
            .then(pl.lit("Ata"))
            .otherwise(pl.lit("Calendário")),
        )
        .select(tuple(_SCHEMA))
        .sort("data_decisao")
    )
    if inicio is not None:
        df = df.filter(pl.col("data_decisao") >= inicio)
    if fim is not None:
        df = df.filter(pl.col("data_decisao") <= fim)
    return df


def proxima_reuniao(referencia: DateLike | None = None) -> pl.DataFrame:
    """Consulta a primeira reunião com decisão em ou após a referência.

    Usa a API de atas e o calendário ICS do BCB, através de calendario.

    Args:
        referencia: Data inclusiva nos formatos DD-MM-YYYY, DD/MM/YYYY,
            YYYY-MM-DD ou date. None usa a data atual no Brasil.

    Returns:
        DataFrame com até uma linha, com o mesmo esquema de calendario.
        Retorna vazio quando não há reunião publicada a partir da referência.

    Output Columns:
        nro_reuniao (Int64): Número sequencial da ata, quando disponível.
        data_inicio (Date): Primeiro dia no ICS, quando disponível.
        data_decisao (Date): Último dia da reunião.
        data_publicacao (Date): Publicação da ata, quando disponível.
        data_efetividade (Date): Próximo dia útil após a decisão.
        status (String): "Realizada" ou "A realizar", conforme a data atual.
        titulo (String): Título da ata, quando disponível.
        fonte (String): "Ata" ou "Calendário".

    Examples:
        >>> yd.copom.proxima_reuniao("01-01-2026").height  # doctest: +SKIP
        1
    """
    referencia = relogio.hoje() if referencia is None else converter_datas(referencia)
    return calendario(inicio=referencia).head(1)
