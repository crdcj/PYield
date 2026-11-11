"""Instituições credenciadas como dealers pelo Tesouro Nacional."""

import polars as pl
import requests

from pyield import relogio
from pyield._internal import converters as cv
from pyield._internal.retry import requisitar
from pyield._internal.types import DateLike

URL_API = "https://apiapex.tesouro.gov.br/aria/v1/api-leiloes-pub/custom/dealers"

ESQUEMA_SAIDA = {
    "inicio_periodo": pl.Date,
    "fim_periodo": pl.Date,
    "cnpj": pl.String,
    "instituicao": pl.String,
}


def _buscar_dealers() -> dict:
    """Busca os dados brutos de dealers na API do Tesouro Nacional."""
    resposta = requisitar(requests.get, URL_API, timeout_leitura=5)
    return resposta.json()


def _parsear_dealers(dados: dict) -> pl.DataFrame:
    """Converte os registros da API em uma estrutura tabular inicial."""
    return pl.from_dicts(
        dados.get("registros") or [],
        schema={
            "INICIO_PERIODO": pl.String,
            "FIM_PERIODO": pl.String,
            "CNPJ": pl.String,
            "DEALER": pl.String,
        },
    )


def _processar_dealers(df: pl.DataFrame) -> pl.DataFrame:
    """Renomeia, tipa e ordena os dados de dealers."""
    return df.select(
        inicio_periodo=pl.col("INICIO_PERIODO").str.to_date(),
        fim_periodo=pl.col("FIM_PERIODO").str.to_date(),
        cnpj=pl.col("CNPJ").str.strip_chars(),
        instituicao=pl.col("DEALER").str.strip_chars(),
    ).sort("inicio_periodo", "instituicao", descending=[True, False])


def dealers(data: DateLike | None = None) -> pl.DataFrame:
    """Busca os dealers do Tesouro Nacional vigentes em uma data.

    Dealers são instituições financeiras credenciadas pelo Tesouro Nacional
    para atuar nas emissões primárias e no mercado secundário de títulos
    públicos federais. A composição é revista periodicamente.

    Fonte: API de Leilões da Dívida Pública do Tesouro Nacional.

    Args:
        data: Data de referência do credenciamento. Se ``None``, usa a data
            atual no Brasil.

    Returns:
        DataFrame Polars com as instituições cujo período de credenciamento
        contém a data informada. Retorna DataFrame vazio, com esquema estável,
        se a fonte não possuir registros para a data.

    Output Columns:
        * inicio_periodo (Date): início do período de credenciamento.
        * fim_periodo (Date): fim do período de credenciamento.
        * cnpj (String): CNPJ da instituição credenciada.
        * instituicao (String): nome da instituição credenciada como dealer.

    Notes:
        A função preserva os nomes e períodos publicados pela fonte. O endpoint
        não informa o tipo da instituição, o conglomerado financeiro nem os
        objetos de negociação escolhidos pelo dealer.

    Examples:
        >>> df = yd.tpf.dealers("15-03-2026")
        >>> df.is_empty() or df["inicio_periodo"].unique().to_list()
        [datetime.date(2026, 2, 10)]
    """
    if isinstance(data, str) and not data.strip():
        return pl.DataFrame(schema=ESQUEMA_SAIDA)
    data_referencia = relogio.hoje() if data is None else cv.converter_datas(data)

    # Sem registros, o parse e o processamento já devolvem o esquema de saída.
    return _processar_dealers(_parsear_dealers(_buscar_dealers())).filter(
        pl.lit(data_referencia).is_between("inicio_periodo", "fim_periodo")
    )
