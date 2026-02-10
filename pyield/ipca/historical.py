import polars as pl
import requests

from pyield._internal.converters import converter_datas
from pyield._internal.retry import retry_padrao
from pyield._internal.types import DateLike, any_is_empty

IPCA_URL = "https://servicodados.ibge.gov.br/api/v3/agregados/6691/periodos/"


@retry_padrao
def _buscar_dados_api(url: str) -> dict[str, str]:
    """Busca dados da API do IBGE e retorna o dicionário da série temporal."""
    resposta = requests.get(url, timeout=10)
    # Levanta exceção para códigos de erro HTTP
    resposta.raise_for_status()
    dados = resposta.json()
    if not dados:
        raise ValueError(f"Nenhum dado disponível para a URL: {url}")
    return dados[0]["resultados"][0]["series"][0]["serie"]


def _processar_df_ipca(
    dados: dict[str, str], em_percentual: bool = False
) -> pl.DataFrame:
    """Processa o dicionário de dados do IPCA em um DataFrame formatado.

    Args:
        dados: Dicionário contendo os dados brutos do IPCA.
        em_percentual: Se os dados representam taxas em formato percentual
            (True) ou números-índice (False). Padrão: False.

    Returns:
        pl.DataFrame: DataFrame com colunas 'Period' e 'Value'.
    """
    df = pl.DataFrame({"Period": dados.keys(), "Value": dados.values()}).with_columns(
        pl.col("Period").cast(pl.Int64),
        pl.col("Value").cast(pl.Float64),
    )
    if em_percentual:
        df = df.with_columns(pl.col("Value").truediv(100).round(4))
    return df


def rates(start: DateLike, end: DateLike) -> pl.DataFrame:
    """Obtém as taxas mensais do IPCA para um intervalo de datas.

    Realiza chamada à API do portal de dados do IBGE no formato:
    https://servicodados.ibge.gov.br/api/v3/agregados/6691/periodos/YYYYMM-YYYYMM/variaveis/63?localidades=N1[all]

    Exemplo: Para o intervalo "01-01-2024" a "31-03-2024", a URL será:
    https://servicodados.ibge.gov.br/api/v3/agregados/6691/periodos/202401-202403/variaveis/63?localidades=N1[all]

    Args:
        start (DateLike): Data de início do intervalo.
        end (DateLike): Data de fim do intervalo.

    Returns:
        pl.DataFrame: DataFrame com colunas 'Period' e 'Value'.

    Examples:
        >>> from pyield import ipca
        >>> # Obter as taxas do IPCA para o primeiro trimestre de 2025
        >>> ipca.rates("01-01-2025", "01-03-2025")
        shape: (3, 2)
        ┌────────┬────────┐
        │ Period ┆ Value  │
        │ ---    ┆ ---    │
        │ i64    ┆ f64    │
        ╞════════╪════════╡
        │ 202501 ┆ 0.0016 │
        │ 202502 ┆ 0.0131 │
        │ 202503 ┆ 0.0056 │
        └────────┴────────┘
    """
    if any_is_empty(start, end):
        return pl.DataFrame()
    start = converter_datas(start)
    end = converter_datas(end)

    data_inicio = start.strftime("%Y%m")
    data_fim = end.strftime("%Y%m")
    url_api = f"{IPCA_URL}{data_inicio}-{data_fim}/variaveis/63?localidades=N1[all]"
    dados = _buscar_dados_api(url_api)

    return _processar_df_ipca(dados, em_percentual=True)


def last_rates(qtd_meses: int = 1) -> pl.DataFrame:
    """Obtém as últimas taxas mensais do IPCA para um número especificado de meses.

    Realiza chamada à API do portal de dados do IBGE no formato:
    https://servicodados.ibge.gov.br/api/v3/agregados/6691/periodos/-N/variaveis/63?localidades=N1[all]

    Exemplo: Para os últimos 2 meses, a URL será:
    https://servicodados.ibge.gov.br/api/v3/agregados/6691/periodos/-2/variaveis/63?localidades=N1[all]

    Args:
        qtd_meses (int, optional): Número de meses a recuperar. Padrão: 1.

    Returns:
        pl.DataFrame: DataFrame com colunas 'Period' e 'Value'.

    Raises:
        ValueError: Se qtd_meses for menor ou igual a 0.

    Examples:
        >>> from pyield import ipca
        >>> # Obter a taxa do IPCA do último mês
        >>> df = ipca.last_rates(1)
        >>> # Obter as taxas do IPCA dos últimos 3 meses
        >>> df = ipca.last_rates(3)
    """
    if qtd_meses <= 0:
        raise ValueError("O número de meses deve ser maior que 0.")

    url_api = f"{IPCA_URL}-{qtd_meses}/variaveis/63?localidades=N1[all]"
    dados = _buscar_dados_api(url_api)

    return _processar_df_ipca(dados, em_percentual=True)


def last_indexes(qtd_meses: int = 1) -> pl.DataFrame:
    """Obtém os últimos valores do número-índice do IPCA para um número
    especificado de meses.

    Realiza chamada à API do portal de dados do IBGE no formato:
    https://servicodados.ibge.gov.br/api/v3/agregados/6691/periodos/-N/variaveis/2266?localidades=N1[all]

    Exemplo: Para os últimos 2 meses, a URL será:
    https://servicodados.ibge.gov.br/api/v3/agregados/6691/periodos/-2/variaveis/2266?localidades=N1[all]

    Args:
        qtd_meses (int, optional): Número de meses a recuperar. Padrão: 1.

    Returns:
        pl.DataFrame: DataFrame com colunas 'Period' e 'Value'.

    Raises:
        ValueError: Se qtd_meses for menor ou igual a 0.

    Examples:
        >>> from pyield import ipca
        >>> # Obter o número-índice do IPCA do último mês
        >>> df = ipca.last_indexes(1)
        >>> # Obter os números-índice do IPCA dos últimos 3 meses
        >>> df = ipca.last_indexes(3)
    """
    if qtd_meses <= 0:
        raise ValueError("O número de meses deve ser maior que 0.")

    url_api = f"{IPCA_URL}-{qtd_meses}/variaveis/2266?localidades=N1[all]"
    dados = _buscar_dados_api(url_api)

    return _processar_df_ipca(dados)


def indexes(start: DateLike, end: DateLike) -> pl.DataFrame:
    """Obtém os valores do número-índice do IPCA para um intervalo de datas.

    Realiza chamada à API do portal de dados do IBGE no formato:
    https://servicodados.ibge.gov.br/api/v3/agregados/6691/periodos/YYYYMM-YYYYMM/variaveis/2266?localidades=N1[all]

    Exemplo: Para o intervalo "01-01-2024" a "31-03-2024", a URL será:
    https://servicodados.ibge.gov.br/api/v3/agregados/6691/periodos/202401-202403/variaveis/2266?localidades=N1[all]

    Args:
        start (DateLike): Data de início do intervalo.
        end (DateLike): Data de fim do intervalo.

    Returns:
        pl.DataFrame: DataFrame com colunas 'Period' e 'Value'.

    Examples:
        >>> from pyield import ipca
        >>> # Obter os números-índice do IPCA para o primeiro trimestre de 2025
        >>> ipca.indexes(start="01-01-2025", end="01-03-2025")
        shape: (3, 2)
        ┌────────┬─────────┐
        │ Period ┆ Value   │
        │ ---    ┆ ---     │
        │ i64    ┆ f64     │
        ╞════════╪═════════╡
        │ 202501 ┆ 7111.86 │
        │ 202502 ┆ 7205.03 │
        │ 202503 ┆ 7245.38 │
        └────────┴─────────┘
    """
    if any_is_empty(start, end):
        return pl.DataFrame()
    start = converter_datas(start)
    end = converter_datas(end)

    data_inicio = start.strftime("%Y%m")
    data_fim = end.strftime("%Y%m")
    url_api = f"{IPCA_URL}{data_inicio}-{data_fim}/variaveis/2266?localidades=N1[all]"
    dados = _buscar_dados_api(url_api)

    return _processar_df_ipca(dados)
