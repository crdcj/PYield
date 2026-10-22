"""Calendário de divulgação do IGP-M publicado pela FGV IBRE."""

import datetime as dt
import re
import ssl

import polars as pl
import requests
from lxml import html
from requests.adapters import HTTPAdapter

from pyield._internal.cache import ttl_cache
from pyield._internal.retry import retry_padrao

_URL = "https://portalibre.fgv.br/calendario-de-divulgacao"


class _AdaptadorFGV(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        contexto = ssl.create_default_context()
        # O servidor da FGV negocia TLS 1.2 com RSA e AES-GCM, sem ECDHE.
        cifras = ":".join(cifra["name"] for cifra in contexto.get_ciphers())
        contexto.set_ciphers(f"{cifras}:AES128-GCM-SHA256")
        kwargs["ssl_context"] = contexto
        return super().init_poolmanager(*args, **kwargs)


@ttl_cache()
@retry_padrao
def _buscar_calendario(ano: int) -> bytes:
    with requests.Session() as sessao:
        sessao.mount(_URL, _AdaptadorFGV())
        resposta = sessao.get(
            _URL,
            params={
                "field_indice_target_id": "128",
                "field_divulgacao_data_value": f"{ano}-01-01",
                "field_divulgacao_data_value_1": f"{ano}-12-31",
            },
            timeout=30,
        )
        resposta.raise_for_status()
        return resposta.content


def _parsear_calendario(conteudo: bytes) -> pl.DataFrame:
    linhas = []
    for item in html.fromstring(conteudo).iter("li"):
        texto = " ".join(" ".join(str(trecho) for trecho in item.itertext()).split())
        if not re.search(r"\bIGP-M e os componentes:", texto):
            continue
        data = re.search(r"\b\d{2}/\d{2}/\d{4}\b", texto)
        if data is None:
            raise ValueError("Divulgação do IGP-M sem data válida.")
        linhas.append(data[0])
    return pl.DataFrame(
        {"data_divulgacao": linhas}, schema={"data_divulgacao": pl.String}
    )


def _processar_calendario(dados: pl.DataFrame) -> pl.DataFrame:
    calendario = (
        dados.with_columns(
            data_divulgacao=pl.col("data_divulgacao").str.to_date("%d/%m/%Y")
        )
        .select(
            competencia=pl.col("data_divulgacao").dt.month_start(),
            data_divulgacao=pl.col("data_divulgacao"),
        )
        .unique()
    )
    if calendario["competencia"].is_duplicated().any():
        raise ValueError("Datas conflitantes no calendário do IGP-M.")
    return calendario.sort("competencia")


def calendario(ano: int, *, atualizar: bool = False) -> pl.DataFrame:
    """Consulta o calendário de divulgação mensal do IGP-M na FGV IBRE.

    Args:
        ano: Ano das divulgações e competências.
        atualizar: Ignora e renova o cache de 60 segundos desse ano.

    Returns:
        DataFrame ordenado por competência. Sem registros, retorna vazio com
        o mesmo esquema.

    Output Columns:
        competencia (Date): Primeiro dia do mês da divulgação mensal do IGP-M.
        data_divulgacao (Date): Data prevista pela FGV IBRE.

    Notes:
        Fonte: calendário de divulgação da FGV IBRE. Seleciona o IGP-M mensal
        com seus componentes, excluindo prévias decendiais e outros índices.
        A competência corresponde ao mês da divulgação mensal. A data prevista
        não comprova publicação. Não cria janelas estimadas quando faltam dados.
        Erros de rede e payload inválido são propagados.

    Raises:
        ValueError: Ano inválido, data inválida ou datas conflitantes.
    """
    if type(ano) is not int or not dt.MINYEAR <= ano <= dt.MAXYEAR:
        raise ValueError("ano deve ser um inteiro entre 1 e 9999.")
    return _processar_calendario(
        _parsear_calendario(_buscar_calendario(ano, _atualizar=atualizar))
    ).filter(pl.col("competencia").dt.year() == ano)
