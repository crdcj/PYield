import logging

import polars as pl
import requests

from pyield import clock

logger = logging.getLogger(__name__)

URL_BASE_API = (
    "https://apiapex.tesouro.gov.br/aria/v1/api-leiloes-pub/custom/benchmarks"
)

PARAM_INCLUIR_HISTORICO = "incluir_historico"


MAPA_COLUNAS = {
    "TÍTULO": ("BondType", pl.String),
    "VENCIMENTO": ("MaturityDate", pl.Date),
    "BENCHMARK": ("Benchmark", pl.String),
    "INÍCIO": ("StartDate", pl.Date),
    "TERMINO": ("EndDate", pl.Date),
}

MAPEAMENTO_COLUNAS = {col: alias for col, (alias, _) in MAPA_COLUNAS.items()}
ESQUEMA_DADOS = {alias: dtype for _, (alias, dtype) in MAPA_COLUNAS.items()}
ORDEM_FINAL_COLUNAS = list(ESQUEMA_DADOS.keys())


def _buscar_benchmarks_brutos(include_history: bool) -> list[dict]:
    """
    Busca os dados brutos de benchmarks na API do Tesouro Nacional.
    Lida com requests de rede, tentativas e validação básica de resposta.
    """
    sessao = requests.Session()
    valor_param_incluir_historico = "S" if include_history else "N"
    endpoint_api = (
        f"{URL_BASE_API}?{PARAM_INCLUIR_HISTORICO}={valor_param_incluir_historico}"
    )

    try:
        resposta = sessao.get(endpoint_api, timeout=10)
        resposta.raise_for_status()
    except requests.exceptions.SSLError as e:
        logger.warning(
            "Erro SSL encontrado: %s. Tentando novamente sem verificação de "
            "certificado (risco de segurança).",
            e,
        )
        resposta = sessao.get(endpoint_api, verify=False, timeout=10)
        resposta.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error("Erro ao buscar benchmarks na API: %s", e)
        return []  # Retorna lista vazia em caso de erro

    try:
        resposta_dict = resposta.json()
    except ValueError as e:
        logger.error("Resposta inválida (JSON) da API: %s", e)
        return []

    if not resposta_dict or "registros" not in resposta_dict:
        logger.warning("Resposta da API sem a chave 'registros' ou vazia.")
        return []

    return resposta_dict["registros"]


def _processar_dados_api(dados_brutos: list[dict]) -> pl.DataFrame:
    if not dados_brutos:
        return pl.DataFrame(schema=ESQUEMA_DADOS)

    tabela = (
        pl.DataFrame(dados_brutos)
        .rename(MAPEAMENTO_COLUNAS)
        .with_columns(pl.col("Benchmark", "BondType").str.strip_chars())
        .cast(ESQUEMA_DADOS, strict=False)
    )

    contagem_nulos = tabela.null_count().row(0)
    total_nulos = sum(contagem_nulos)
    if total_nulos:
        logger.warning(
            "Foram encontradas células nulas após o parse (total=%s). "
            "Linhas com nulos serão descartadas.",
            total_nulos,
        )

    return tabela.drop_nulls()


def benchmarks(
    bond_type: str | None = None, include_history: bool = False
) -> pl.DataFrame:
    """Busca benchmarks de títulos públicos brasileiros na API do TN.

    Recupera dados atuais ou históricos de benchmarks para títulos do Tesouro
    Nacional (ex.: LTN, LFT, NTN-B). Os dados são obtidos diretamente da
    API oficial do Tesouro Nacional.

    Args:
        bond_type (str, optional): Tipo do título a filtrar (ex.: "LFT").
        include_history (bool, optional): Se `True`, inclui histórico; se `False`
            (padrão), retorna apenas benchmarks vigentes.

    Returns:
        pl.DataFrame: DataFrame Polars com os benchmarks.

    Output Columns:
        * `BondType` (String): Tipo do título (ex.: "LTN", "LFT", "NTN-B").
        * `MaturityDate` (Date): Data de vencimento do benchmark.
        * `Benchmark` (String): Nome/identificador do benchmark.
        * `StartDate` (Date): Data de início da vigência.
        * `EndDate` (Date): Data de término da vigência.

    Notes:
        * Dados obtidos da API oficial do Tesouro Nacional.
        * Há retry sem verificação de certificado apenas em caso de erro SSL.
        * Linhas com valores nulos são descartadas antes do retorno.
        * Documentação da API:
          https://portal-conhecimento.tesouro.gov.br/catalogo-componentes/api-leil%C3%B5es

    Examples:
        >>> from pyield import tn
        >>> df_current = tn.benchmarks()
        >>> # Benchmarks históricos
        >>> tn.benchmarks(bond_type="LFT", include_history=True).head()
        shape: (5, 5)
        ┌──────────┬──────────────┬────────────┬────────────┬────────────┐
        │ BondType ┆ MaturityDate ┆ Benchmark  ┆ StartDate  ┆ EndDate    │
        │ ---      ┆ ---          ┆ ---        ┆ ---        ┆ ---        │
        │ str      ┆ date         ┆ str        ┆ date       ┆ date       │
        ╞══════════╪══════════════╪════════════╪════════════╪════════════╡
        │ LFT      ┆ 2020-03-01   ┆ LFT 6 anos ┆ 2014-01-01 ┆ 2014-06-30 │
        │ LFT      ┆ 2020-09-01   ┆ LFT 6 anos ┆ 2014-07-01 ┆ 2014-12-31 │
        │ LFT      ┆ 2021-03-01   ┆ LFT 6 anos ┆ 2015-01-01 ┆ 2015-04-30 │
        │ LFT      ┆ 2021-09-01   ┆ LFT 6 anos ┆ 2015-05-01 ┆ 2015-12-31 │
        │ LFT      ┆ 2022-03-01   ┆ LFT 6 anos ┆ 2016-01-01 ┆ 2016-06-30 │
        └──────────┴──────────────┴────────────┴────────────┴────────────┘
    """
    dados_api = _buscar_benchmarks_brutos(include_history=include_history)
    tabela = _processar_dados_api(dados_api)

    # Definir a ordenação final com base no caso de uso
    if include_history:
        # Para dados históricos, a ordem cronológica é mais útil
        colunas_ordenacao = ["StartDate", "BondType", "MaturityDate"]
    else:
        # Para dados atuais, agrupar por tipo de título é mais útil
        colunas_ordenacao = ["BondType", "MaturityDate"]
        # Filtrar apenas os dados atuais
        hoje = clock.today()
        tabela = tabela.filter(pl.lit(hoje).is_between("StartDate", "EndDate"))

    if bond_type:
        tabela = tabela.filter(pl.col("BondType") == bond_type.upper())

    return tabela.select(ORDEM_FINAL_COLUNAS).sort(colunas_ordenacao)
