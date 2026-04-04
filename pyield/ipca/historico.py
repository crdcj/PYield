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


def _processar_ipca(
    dados: dict[str, str],
    em_percentual: bool = False,
) -> pl.DataFrame:
    """Processa o dicionário de dados do IPCA em DataFrame."""
    valor_expr = pl.col("valor").cast(pl.Float64)
    if em_percentual:
        valor_expr = valor_expr.truediv(100).round(4)
    return pl.DataFrame(
        {"periodo": dados.keys(), "valor": dados.values()}
    ).with_columns(
        pl.col("periodo").cast(pl.Int64),
        valor=valor_expr,
    )


def _buscar_periodo(
    data_inicial: DateLike,
    data_final: DateLike,
    variavel: int,
    em_percentual: bool = False,
) -> pl.DataFrame:
    """Busca dados do IPCA para um intervalo de datas."""
    if any_is_empty(data_inicial, data_final):
        return pl.DataFrame()
    inicio = converter_datas(data_inicial).strftime("%Y%m")
    fim = converter_datas(data_final).strftime("%Y%m")
    url = f"{_URL_BASE}{inicio}-{fim}/variaveis/{variavel}{_SUFIXO_URL}"
    return _processar_ipca(_buscar_dados_api(url), em_percentual)


def _buscar_ultimos(
    qtd_meses: int,
    variavel: int,
    em_percentual: bool = False,
) -> pl.DataFrame:
    """Busca os últimos N meses de dados do IPCA."""
    if qtd_meses <= 0:
        raise ValueError("O número de meses deve ser maior que 0.")
    url = f"{_URL_BASE}-{qtd_meses}/variaveis/{variavel}{_SUFIXO_URL}"
    return _processar_ipca(_buscar_dados_api(url), em_percentual)


def taxas(data_inicial: DateLike, data_final: DateLike) -> pl.DataFrame:
    """Obtém as taxas mensais do IPCA para um intervalo de datas.

    Realiza chamada à API do portal de dados do IBGE no formato:
    https://servicodados.ibge.gov.br/api/v3/agregados/6691/periodos/YYYYMM-YYYYMM/variaveis/63?localidades=N1[all]

    Args:
        data_inicial: Data de início do intervalo.
        data_final: Data de fim do intervalo.

    Returns:
        pl.DataFrame com colunas 'periodo' e 'valor'.

    Output Columns:
        * periodo (Int64): período no formato YYYYMM.
        * valor (Float64): taxa mensal em decimal.

    Examples:
        >>> from pyield import ipca
        >>> # Obter as taxas do IPCA para o primeiro trimestre de 2025
        >>> ipca.taxas("01-01-2025", "01-03-2025")
        shape: (3, 2)
        ┌─────────┬────────┐
        │ periodo ┆ valor  │
        │ ---     ┆ ---    │
        │ i64     ┆ f64    │
        ╞═════════╪════════╡
        │ 202501  ┆ 0.0016 │
        │ 202502  ┆ 0.0131 │
        │ 202503  ┆ 0.0056 │
        └─────────┴────────┘
    """
    return _buscar_periodo(data_inicial, data_final, _VAR_TAXA, em_percentual=True)


def taxas_ultimas(qtd_meses: int = 1) -> pl.DataFrame:
    """Obtém as últimas taxas mensais do IPCA.

    Realiza chamada à API do portal de dados do IBGE no formato:
    https://servicodados.ibge.gov.br/api/v3/agregados/6691/periodos/-N/variaveis/63?localidades=N1[all]

    Args:
        qtd_meses: Número de meses a recuperar. Padrão: 1.

    Returns:
        pl.DataFrame com colunas 'periodo' e 'valor'.

    Output Columns:
        * periodo (Int64): período no formato YYYYMM.
        * valor (Float64): taxa mensal em decimal.

    Raises:
        ValueError: Se qtd_meses for menor ou igual a 0.

    Examples:
        >>> from pyield import ipca
        >>> # Obter a taxa do IPCA do último mês
        >>> df = ipca.taxas_ultimas(1)
        >>> # Obter as taxas do IPCA dos últimos 3 meses
        >>> df = ipca.taxas_ultimas(3)
    """
    return _buscar_ultimos(qtd_meses, _VAR_TAXA, em_percentual=True)


def indices_ultimos(qtd_meses: int = 1) -> pl.DataFrame:
    """Obtém os últimos valores do número-índice do IPCA.

    Realiza chamada à API do portal de dados do IBGE no formato:
    https://servicodados.ibge.gov.br/api/v3/agregados/6691/periodos/-N/variaveis/2266?localidades=N1[all]

    Args:
        qtd_meses: Número de meses a recuperar. Padrão: 1.

    Returns:
        pl.DataFrame com colunas 'periodo' e 'valor'.

    Output Columns:
        * periodo (Int64): período no formato YYYYMM.
        * valor (Float64): número-índice do IPCA.

    Raises:
        ValueError: Se qtd_meses for menor ou igual a 0.

    Examples:
        >>> from pyield import ipca
        >>> # Obter o número-índice do IPCA do último mês
        >>> df = ipca.indices_ultimos(1)
        >>> # Obter os números-índice do IPCA dos últimos 3 meses
        >>> df = ipca.indices_ultimos(3)
    """
    return _buscar_ultimos(qtd_meses, _VAR_INDICE)


def indices(data_inicial: DateLike, data_final: DateLike) -> pl.DataFrame:
    """Obtém os valores do número-índice do IPCA para um intervalo.

    Realiza chamada à API do portal de dados do IBGE no formato:
    https://servicodados.ibge.gov.br/api/v3/agregados/6691/periodos/YYYYMM-YYYYMM/variaveis/2266?localidades=N1[all]

    Args:
        data_inicial: Data de início do intervalo.
        data_final: Data de fim do intervalo.

    Returns:
        pl.DataFrame com colunas 'periodo' e 'valor'.

    Output Columns:
        * periodo (Int64): período no formato YYYYMM.
        * valor (Float64): número-índice do IPCA.

    Examples:
        >>> from pyield import ipca
        >>> # Obter os números-índice do IPCA para o primeiro trimestre
        >>> ipca.indices(data_inicial="01-01-2025", data_final="01-03-2025")
        shape: (3, 2)
        ┌─────────┬─────────┐
        │ periodo ┆ valor   │
        │ ---     ┆ ---     │
        │ i64     ┆ f64     │
        ╞═════════╪═════════╡
        │ 202501  ┆ 7111.86 │
        │ 202502  ┆ 7205.03 │
        │ 202503  ┆ 7245.38 │
        └─────────┴─────────┘
    """
    return _buscar_periodo(data_inicial, data_final, _VAR_INDICE)
