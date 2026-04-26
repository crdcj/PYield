"""
Busca dados intradiários de negociações secundárias da dívida pública federal.
https://www.bcb.gov.br/htms/selic/selicprecos.asp?frame=1
"""

import datetime as dt

import polars as pl
import requests

from pyield import du, relogio
from pyield._internal.br_numbers import float_br, inteiro_br, taxa_br
from pyield._internal.cache import ttl_cache
from pyield._internal.retry import retry_padrao

HORA_INICIO_TEMPO_REAL = dt.time(9, 0, 0)
HORA_FIM_TEMPO_REAL = dt.time(22, 0, 0)
URL_BASE_TEMPO_REAL = (
    "https://www3.bcb.gov.br/novoselic/rest/precosNegociacao/pub/download/estatisticas/"
)


@ttl_cache()
@retry_padrao
def _buscar_csv() -> bytes:
    """
    Exemplo de URL do CSV com dados intradiários:
        https://www3.bcb.gov.br/novoselic/rest/precosNegociacao/pub/download/estatisticas/02-06-2025
    """
    hoje = relogio.hoje()
    data_formatada = hoje.strftime("%d-%m-%Y")
    url = f"{URL_BASE_TEMPO_REAL}{data_formatada}"
    r = requests.get(url, timeout=30)  # API costuma levar ~10s
    r.raise_for_status()
    return r.content


def _parsear_df(dados: bytes) -> pl.DataFrame:
    """Lê CSV como strings."""
    return pl.read_csv(
        dados,
        separator=";",
        infer_schema=False,
        null_values="-",
    ).rename(lambda c: c.strip())


def _processar_df(df: pl.DataFrame) -> pl.DataFrame:
    """Filtra registros, converte tipos e reordena colunas."""
    agora = relogio.agora()
    return df.filter(pl.col("//1") == "1").select(
        data_hora_consulta=agora,
        data_liquidacao=agora.date(),
        titulo=pl.col("sigla").str.strip_chars(),
        codigo_selic=inteiro_br("código título"),
        data_vencimento=pl.col("data vencimento").str.to_date("%d/%m/%Y"),
        pu_minimo=float_br("pu mínimo"),
        pu_medio=float_br("pu médio"),
        pu_maximo=float_br("pu máximo"),
        pu_ultimo=float_br("mercado à vista pu último"),
        taxa_minima=taxa_br("tx mínimo"),
        taxa_media=taxa_br("tx médio"),
        taxa_maxima=taxa_br("tx máximo"),
        taxa_ultima=taxa_br("tx último"),
        operacoes=inteiro_br("totais liquidados operações"),
        quantidade=inteiro_br("títulos"),
        financeiro=float_br("financeiro"),
        operacoes_corretagem=inteiro_br("corretagem liquidados operações"),
        quantidade_corretagem=inteiro_br("corretagem títulos"),
        termo_pu_minimo=float_br("pu mínimo_duplicated_0"),
        termo_pu_medio=float_br("pu médio_duplicated_0"),
        termo_pu_ultimo=float_br("mercado a termo pu último"),
        termo_pu_maximo=float_br("pu máximo_duplicated_0"),
        termo_taxa_ultima=taxa_br("tx último_duplicated_0"),
        termo_taxa_minima=taxa_br("tx mínimo_duplicated_0"),
        termo_taxa_media=taxa_br("tx médio_duplicated_0"),
        termo_taxa_maxima=taxa_br("tx máximo_duplicated_0"),
        termo_operacoes=inteiro_br("totais contratados operações"),
        termo_quantidade=inteiro_br("títulos_duplicated_0"),
        termo_financeiro=float_br("financeiro_duplicated_0"),
        termo_operacoes_corretagem=inteiro_br("corretagem contratados operações"),
        termo_quantidade_corretagem=inteiro_br("corretagem títulos_duplicated_0"),
    )


def _mercado_selic_aberto() -> bool:
    """Verifica se o mercado SELIC está aberto no momento."""
    agora = relogio.agora()
    hoje = agora.date()
    hora = agora.time()
    eh_dia_util = du.eh_dia_util(hoje)
    eh_horario = HORA_INICIO_TEMPO_REAL <= hora <= HORA_FIM_TEMPO_REAL

    return eh_dia_util and eh_horario


def secundario_intradia_bcb() -> pl.DataFrame:
    """Implementação técnica de busca do secundário intradia de TPF.

    API pública e docstring canônica: ``pyield.tpf.secundario_intradia``.
    """
    if not _mercado_selic_aberto():
        return pl.DataFrame()

    texto_bruto = _buscar_csv()
    df = _parsear_df(texto_bruto)
    return _processar_df(df)
