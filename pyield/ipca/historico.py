"""Dados históricos do IPCA via API do IBGE (agregado 6691).

Variáveis disponíveis:
    - 63: IPCA - Variação mensal (%)
    - 2266: IPCA - Número-índice (base dez/1993 = 100)

Exemplo de chamada à API:
    https://servicodados.ibge.gov.br/api/v3/agregados/6691/periodos/202501/variaveis/63?localidades=N1[all]

Exemplo de resposta JSON (simplificado):
    [{"id": "63",
      "variavel": "IPCA - Variação mensal",
      "unidade": "%",
      "resultados": [{"series": [{"serie": {"202501": "0.16"}}]}]}]

O dado utilizado é extraído de resultados[0].series[0].serie, que é um
dicionário {período: valor} (ex.: {"202501": "0.16", "202502": "1.31"}).
"""

import polars as pl
import requests

from pyield._internal.br_numbers import pct_para_decimal
from pyield._internal.cache import ttl_cache
from pyield._internal.converters import converter_datas
from pyield._internal.retry import retry_padrao
from pyield._internal.types import DateLike, any_is_empty

_URL_BASE = "https://servicodados.ibge.gov.br/api/v3/agregados/6691/periodos/"
_SUFIXO_URL = "?localidades=N1[all]"
_VAR_TAXA = 63
_VAR_INDICE = 2266


@ttl_cache()
@retry_padrao
def _buscar_dados_api(url: str) -> dict[str, str]:
    """Busca dados da API do IBGE e retorna o dicionário da série."""
    resposta = requests.get(url, timeout=10)
    resposta.raise_for_status()
    dados = resposta.json()
    if not dados:
        raise ValueError(f"Nenhum dado disponível para a URL: {url}")
    return dados[0]["resultados"][0]["series"][0]["serie"]


def _processar_ipca(dados: dict[str, str]) -> pl.DataFrame:
    """Processa o dicionário de dados do IPCA em DataFrame."""
    return pl.DataFrame(
        {"periodo": dados.keys(), "valor": dados.values()}
    ).with_columns(
        pl.col("periodo").cast(pl.Int64),
        pl.col("valor").cast(pl.Float64),
    )


def _buscar_periodo(
    inicio: DateLike,
    fim: DateLike,
    variavel: int,
) -> pl.DataFrame:
    """Busca dados do IPCA para um intervalo de datas."""
    if any_is_empty(inicio, fim):
        return pl.DataFrame()
    periodo_inicio = converter_datas(inicio).strftime("%Y%m")
    periodo_fim = converter_datas(fim).strftime("%Y%m")
    url = f"{_URL_BASE}{periodo_inicio}-{periodo_fim}/variaveis/{variavel}{_SUFIXO_URL}"
    return _processar_ipca(_buscar_dados_api(url))


def _buscar_ultimos(
    qtd_meses: int,
    variavel: int,
) -> pl.DataFrame:
    """Busca os últimos N meses de dados do IPCA."""
    if qtd_meses <= 0:
        raise ValueError("O número de meses deve ser maior que 0.")
    url = f"{_URL_BASE}-{qtd_meses}/variaveis/{variavel}{_SUFIXO_URL}"
    return _processar_ipca(_buscar_dados_api(url))


def _extrair_escalar(df: pl.DataFrame, coluna: str) -> float:
    if df.is_empty():
        return float("nan")
    return df[coluna].item(0)


def taxas(inicio: DateLike, fim: DateLike) -> pl.DataFrame:
    """Obtém as taxas mensais do IPCA para um intervalo de datas.

    Realiza chamada à API do portal de dados do IBGE no formato:
    https://servicodados.ibge.gov.br/api/v3/agregados/6691/periodos/YYYYMM-YYYYMM/variaveis/63?localidades=N1[all]

    Args:
        inicio: Data de início do intervalo.
        fim: Data de fim do intervalo.

    Returns:
        pl.DataFrame com colunas 'periodo' e 'taxa' (decimal).

    Output Columns:
        * periodo (Int64): período no formato YYYYMM.
        * taxa (Float64): taxa mensal em decimal (ex: 0.0016 = 0,16%).

    Examples:
        >>> from pyield import ipca
        >>> ipca.taxas("01-01-2025", "01-03-2025")  # decimal (0.0016 = 0,16%)
        shape: (3, 2)
        ┌─────────┬────────┐
        │ periodo ┆ taxa   │
        │ ---     ┆ ---    │
        │ i64     ┆ f64    │
        ╞═════════╪════════╡
        │ 202501  ┆ 0.0016 │
        │ 202502  ┆ 0.0131 │
        │ 202503  ┆ 0.0056 │
        └─────────┴────────┘
    """
    return (
        _buscar_periodo(inicio, fim, _VAR_TAXA)
        .with_columns(taxa=pct_para_decimal(pl.col("valor")))
        .select("periodo", "taxa")
    )


def taxa(data: DateLike) -> float:
    """Taxa mensal do IPCA para um mês específico.

    Args:
        data: Qualquer data dentro do mês desejado.

    Returns:
        Taxa mensal do IPCA em decimal ou ``nan`` se não disponível.

    Examples:
        >>> from pyield import ipca
        >>> ipca.taxa("01-01-2025")  # decimal (0.0016 = 0,16%)
        0.0016
    """
    if any_is_empty(data):
        return float("nan")
    return _extrair_escalar(taxas(data, data), "taxa")


def taxas_ultimas(qtd_meses: int = 1) -> pl.DataFrame:
    """Obtém as últimas taxas mensais do IPCA.

    Realiza chamada à API do portal de dados do IBGE no formato:
    https://servicodados.ibge.gov.br/api/v3/agregados/6691/periodos/-N/variaveis/63?localidades=N1[all]

    Args:
        qtd_meses: Número de meses a recuperar. Padrão: 1.

    Returns:
        pl.DataFrame com colunas 'periodo' e 'taxa' (decimal).

    Output Columns:
        * periodo (Int64): período no formato YYYYMM.
        * taxa (Float64): taxa mensal em decimal (ex: 0.0016 = 0,16%).

    Raises:
        ValueError: Se qtd_meses for menor ou igual a 0.

    Examples:
        >>> from pyield import ipca
        >>> # Obter a taxa do IPCA do último mês
        >>> df = ipca.taxas_ultimas(1)
        >>> # Obter as taxas do IPCA dos últimos 3 meses
        >>> df = ipca.taxas_ultimas(3)
    """
    return (
        _buscar_ultimos(qtd_meses, _VAR_TAXA)
        .with_columns(taxa=pct_para_decimal(pl.col("valor")))
        .select("periodo", "taxa")
    )


def indices_ultimos(qtd_meses: int = 1) -> pl.DataFrame:
    """Obtém os últimos valores do número-índice do IPCA.

    Realiza chamada à API do portal de dados do IBGE no formato:
    https://servicodados.ibge.gov.br/api/v3/agregados/6691/periodos/-N/variaveis/2266?localidades=N1[all]

    Args:
        qtd_meses: Número de meses a recuperar. Padrão: 1.

    Returns:
        pl.DataFrame com colunas 'periodo' e 'indice'.

    Output Columns:
        * periodo (Int64): período no formato YYYYMM.
        * indice (Float64): número-índice do IPCA.

    Raises:
        ValueError: Se qtd_meses for menor ou igual a 0.

    Examples:
        >>> from pyield import ipca
        >>> # Obter o número-índice do IPCA do último mês
        >>> df = ipca.indices_ultimos(1)
        >>> # Obter os números-índice do IPCA dos últimos 3 meses
        >>> df = ipca.indices_ultimos(3)
    """
    return _buscar_ultimos(qtd_meses, _VAR_INDICE).rename({"valor": "indice"})


def indices(inicio: DateLike, fim: DateLike) -> pl.DataFrame:
    """Obtém os valores do número-índice do IPCA para um intervalo.

    Realiza chamada à API do portal de dados do IBGE no formato:
    https://servicodados.ibge.gov.br/api/v3/agregados/6691/periodos/YYYYMM-YYYYMM/variaveis/2266?localidades=N1[all]

    Args:
        inicio: Data de início do intervalo.
        fim: Data de fim do intervalo.

    Returns:
        pl.DataFrame com colunas 'periodo' e 'indice'.

    Output Columns:
        * periodo (Int64): período no formato YYYYMM.
        * indice (Float64): número-índice do IPCA.

    Examples:
        >>> from pyield import ipca
        >>> # Obter os números-índice do IPCA para o primeiro trimestre
        >>> ipca.indices(inicio="01-01-2025", fim="01-03-2025")
        shape: (3, 2)
        ┌─────────┬─────────┐
        │ periodo ┆ indice  │
        │ ---     ┆ ---     │
        │ i64     ┆ f64     │
        ╞═════════╪═════════╡
        │ 202501  ┆ 7111.86 │
        │ 202502  ┆ 7205.03 │
        │ 202503  ┆ 7245.38 │
        └─────────┴─────────┘
    """
    return _buscar_periodo(inicio, fim, _VAR_INDICE).rename({"valor": "indice"})


def indice(data: DateLike) -> float:
    """Número-índice do IPCA para um mês específico.

    Args:
        data: Qualquer data dentro do mês desejado.

    Returns:
        Número-índice do IPCA ou ``nan`` se não disponível.

    Examples:
        >>> from pyield import ipca
        >>> ipca.indice("01-01-2025")
        7111.86
    """
    if any_is_empty(data):
        return float("nan")
    return _extrair_escalar(indices(data, data), "indice")
