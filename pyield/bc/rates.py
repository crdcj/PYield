"""Funções para buscar indicadores financeiros da API do Banco Central do Brasil.

Notas de implementação:
    - Valores são obtidos em formato percentual e convertidos para decimal
      (divididos por 100).
    - Cada tipo de taxa é arredondado para manter a mesma precisão fornecida pelo
      Banco Central:
        - SELIC Over e SELIC Meta: 4 casas decimais
        - DI Over: 8 casas decimais para taxas diárias. Para taxas anualizadas,
          o valor é arredondado para 4 casas decimais.
    - Para requisições que abrangem mais de 10 anos, o intervalo de datas é
      automaticamente dividido usando funcionalidades nativas do Polars.
"""

import logging
from enum import Enum

import polars as pl
import requests

from pyield import clock
from pyield._internal.converters import converter_datas
from pyield._internal.retry import retry_padrao
from pyield._internal.types import DateLike, any_is_empty

registro = logging.getLogger(__name__)

URL_BASE = "https://api.bcb.gov.br/dados/serie/bcdata.sgs."
CASAS_DECIMAIS_ANUALIZADA = 4  # 2 casas no formato percentual
CASAS_DECIMAIS_DIARIA = 8  # 6 casas no formato percentual

# Limite de segurança em dias, correspondendo a ~9.5 anos.
# Evita a complexidade do cálculo exato de 10 anos-calendário.
LIMITE_DIAS_SEGURO = 3500  # aprox 365 * 9.5


class SerieBC(Enum):
    """Enum para as séries disponíveis no Banco Central."""

    SELIC_OVER = 1178
    SELIC_TARGET = 432
    DI_OVER = 11


@retry_padrao
def _chamar_api(url_api: str) -> list[dict[str, str]]:
    """Executa uma chamada GET na API do BCB e retorna o JSON.

    A API retorna um json (lista de dicts com chaves 'data' e 'valor', ambas strings):
        [{"data": "29/01/2025", "valor": "12.15"}, ...]
    """
    resposta = requests.get(url_api, timeout=10)
    resposta.raise_for_status()
    return resposta.json()


def _montar_url_download(
    serie: SerieBC, inicio: DateLike, fim: DateLike | None = None
) -> str:
    """Constrói a URL para download de dados das séries do Banco Central.

    Args:
        serie: Valor enum da série a buscar.
        inicio: Data inicial para os dados a buscar.
        fim: Data final para os dados.

    Returns:
        URL formatada para a requisição da API.
    """
    inicio = converter_datas(inicio)
    inicio_str = inicio.strftime("%d/%m/%Y")

    url_api = URL_BASE
    url_api += f"{serie.value}/dados?formato=json"
    url_api += f"&dataInicial={inicio_str}"

    if fim:
        fim = converter_datas(fim)
        fim_str = fim.strftime("%d/%m/%Y")
        url_api += f"&dataFinal={fim_str}"

    return url_api


def _buscar_requisicao(
    serie: SerieBC,
    inicio: DateLike,
    fim: DateLike | None,
) -> pl.DataFrame:
    """Função auxiliar que busca dados da API."""
    # Define o esquema esperado para o DataFrame de retorno.
    # Isso é crucial para os casos em que a API não retorna dados.
    esquema_esperado = {"Date": pl.Date, "Value": pl.Float64}

    url_api = _montar_url_download(serie, inicio, fim)

    try:
        dados = _chamar_api(url_api)
        if not dados:
            registro.warning(f"Sem dados para o período solicitado: {url_api}")
            return pl.DataFrame(schema=esquema_esperado)

        df = pl.from_dicts(dados).select(
            Date=pl.col("data").str.to_date("%d/%m/%Y"),
            Value=pl.col("valor").cast(pl.Float64) / 100,
        )
        return df

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:  # noqa
            registro.warning(
                f"Recurso não encontrado (404), tratado como sem dados: {e.request.url}"
            )
            return pl.DataFrame(schema=esquema_esperado)

        # Qualquer outro erro HTTP final para o programa.
        raise


def _buscar_dados_url(
    serie: SerieBC, inicio: DateLike, fim: DateLike | None = None
) -> pl.DataFrame:
    """Orquestra a busca de dados da API do Banco Central.

    Trata requisições maiores que 10 anos dividindo-as em blocos menores usando
    polars date_range.

    Args:
        serie: Enum da série a buscar.
        inicio: Data inicial para os dados a buscar.
        fim: Data final para os dados.

    Returns:
        DataFrame com os dados requisitados.
    """
    # 1. Converter datas usando a função auxiliar existente
    data_inicio = converter_datas(inicio)
    # Se a data final não for fornecida, usar a data de hoje para o cálculo do período
    data_fim = converter_datas(fim) if fim else clock.today()

    # Verificação simples e pragmática baseada em dias. Se o período for
    # menor que nosso limite de segurança, faz uma chamada única.
    if (data_fim - data_inicio).days < LIMITE_DIAS_SEGURO:
        return _buscar_requisicao(serie, data_inicio, data_fim)

    # 3. Se for maior, quebrar em blocos
    registro.info("Intervalo excede 10 anos. Buscando dados em blocos.")

    duracao_str = "10y"

    inicios_bloco = pl.date_range(
        start=data_inicio, end=data_fim, interval=duracao_str, eager=True
    )

    fins_bloco = inicios_bloco.dt.offset_by(duracao_str)

    blocos_df = pl.DataFrame({"start": inicios_bloco, "end": fins_bloco}).with_columns(
        end=pl.when(pl.col("end") > data_fim).then(data_fim).otherwise("end")
    )

    todos_dfs = [
        _buscar_requisicao(serie, bloco["start"], bloco["end"])
        for bloco in blocos_df.iter_rows(named=True)
    ]

    todos_dfs = [df for df in todos_dfs if not df.is_empty()]

    if not todos_dfs:
        return pl.DataFrame()

    return pl.concat(todos_dfs).unique(subset=["Date"], keep="first").sort("Date")


def selic_over_series(
    start: DateLike,
    end: DateLike | None = None,
) -> pl.DataFrame:
    """Busca a taxa SELIC Over do Banco Central do Brasil.

    A taxa SELIC Over é a taxa de juros média diária efetivamente praticada
    entre bancos no mercado interbancário, usando títulos públicos como garantia.

    Exemplo de URL da API:
        https://api.bcb.gov.br/dados/serie/bcdata.sgs.1178/dados?formato=json&dataInicial=12/04/2024&dataFinal=12/04/2024

    Args:
        start: Data inicial para buscar os dados. Se None, retorna dados desde
            a data mais antiga disponível.
        end: Data final para buscar os dados. Se None, retorna dados até a
            data mais recente disponível.

    Returns:
        DataFrame contendo colunas Date e Value com a taxa SELIC Over, ou
        DataFrame vazio se dados não estiverem disponíveis.

    Examples:
        >>> from pyield import bc
        >>> # Sem dados em 26-01-2025 (domingo). Selic mudou por reunião do Copom.
        >>> bc.selic_over_series("26-01-2025").head(5)  # Primeiras 5 linhas
        shape: (5, 2)
        ┌────────────┬────────┐
        │ Date       ┆ Value  │
        │ ---        ┆ ---    │
        │ date       ┆ f64    │
        ╞════════════╪════════╡
        │ 2025-01-27 ┆ 0.1215 │
        │ 2025-01-28 ┆ 0.1215 │
        │ 2025-01-29 ┆ 0.1215 │
        │ 2025-01-30 ┆ 0.1315 │
        │ 2025-01-31 ┆ 0.1315 │
        └────────────┴────────┘

        >>> # Buscando dados para um intervalo específico
        >>> bc.selic_over_series("14-09-2025", "17-09-2025")
        shape: (3, 2)
        ┌────────────┬───────┐
        │ Date       ┆ Value │
        │ ---        ┆ ---   │
        │ date       ┆ f64   │
        ╞════════════╪═══════╡
        │ 2025-09-15 ┆ 0.149 │
        │ 2025-09-16 ┆ 0.149 │
        │ 2025-09-17 ┆ 0.149 │
        └────────────┴───────┘
    """
    if any_is_empty(start):  # start deve ser fornecido
        return pl.DataFrame()
    df = _buscar_dados_url(SerieBC.SELIC_OVER, start, end)
    return df.with_columns(pl.col("Value").round(CASAS_DECIMAIS_ANUALIZADA))


def selic_over(date: DateLike) -> float:
    """Busca o valor da taxa SELIC Over para uma data específica.

    Função de conveniência que retorna apenas o valor (não o DataFrame) para a
    data especificada.

    Args:
        date: Data de referência para buscar a taxa SELIC Over.

    Returns:
        Taxa SELIC Over como float ou NaN se não disponível.

    Examples:
        >>> from pyield import bc
        >>> bc.selic_over("31-05-2024")
        0.104
    """
    if any_is_empty(date):
        return float("nan")
    df = selic_over_series(date, date)
    if df.is_empty():
        return float("nan")
    return df["Value"].item(0)


def selic_target_series(
    start: DateLike,
    end: DateLike | None = None,
) -> pl.DataFrame:
    """Busca a taxa SELIC Meta do Banco Central do Brasil.

    A taxa SELIC Meta é a taxa de juros oficial definida pelo Comitê de Política
    Monetária (COPOM) do Banco Central do Brasil.

    Exemplo de URL da API:
        https://api.bcb.gov.br/dados/serie/bcdata.sgs.432/dados?formato=json&dataInicial=12/04/2024&dataFinal=12/04/2024

    Args:
        start: Data inicial para buscar os dados.
        end: Data final para buscar os dados. Se None, retorna dados até a
            data mais recente disponível.

    Returns:
        DataFrame contendo colunas Date e Value com a taxa SELIC Meta, ou
        DataFrame vazio se dados não estiverem disponíveis.

    Examples:
        >>> from pyield import bc
        >>> bc.selic_target_series("31-05-2024", "31-05-2024")
        shape: (1, 2)
        ┌────────────┬───────┐
        │ Date       ┆ Value │
        │ ---        ┆ ---   │
        │ date       ┆ f64   │
        ╞════════════╪═══════╡
        │ 2024-05-31 ┆ 0.105 │
        └────────────┴───────┘
    """
    if any_is_empty(start):  # start deve ser fornecido
        return pl.DataFrame()
    df = _buscar_dados_url(SerieBC.SELIC_TARGET, start, end)
    df = df.with_columns(pl.col("Value").round(CASAS_DECIMAIS_ANUALIZADA))
    return df


def selic_target(date: DateLike) -> float:
    """Busca o valor da taxa SELIC Meta para uma data específica.

    Função de conveniência que retorna apenas o valor (não o DataFrame) para a
    data especificada.

    Args:
        date: Data de referência para buscar a taxa SELIC Meta.

    Returns:
        Taxa SELIC Meta como float ou NaN se não disponível.

    Examples:
        >>> from pyield import bc
        >>> bc.selic_target("31-05-2024")
        0.105
    """
    if any_is_empty(date):
        return float("nan")
    df = selic_target_series(date, date)
    if df.is_empty():
        return float("nan")
    return df["Value"].item(0)


def di_over_series(
    start: DateLike,
    end: DateLike | None = None,
    annualized: bool = True,
) -> pl.DataFrame:
    """Busca a taxa DI (Depósito Interbancário) do Banco Central do Brasil.

    A taxa DI representa a taxa de juros média dos empréstimos interbancários.

    Exemplo de URL da API:
        https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados?formato=json&dataInicial=12/04/2024&dataFinal=12/04/2024

    Args:
        start: Data inicial para buscar os dados. Se None, retorna dados desde
            a data mais antiga disponível.
        end: Data final para buscar os dados. Se None, retorna dados até a
            data mais recente disponível.
        annualized: Se True, retorna a taxa anualizada (252 dias úteis por ano),
            caso contrário retorna a taxa diária.

    Returns:
        DataFrame contendo colunas Date e Value com a taxa DI, ou DataFrame
        vazio se dados não estiverem disponíveis.

    Examples:
        >>> from pyield import bc
        >>> # Retorna todos os dados desde 29-01-2025
        >>> bc.di_over_series("29-01-2025").head(5)  # Primeiras 5 linhas
        shape: (5, 2)
        ┌────────────┬────────┐
        │ Date       ┆ Value  │
        │ ---        ┆ ---    │
        │ date       ┆ f64    │
        ╞════════════╪════════╡
        │ 2025-01-29 ┆ 0.1215 │
        │ 2025-01-30 ┆ 0.1315 │
        │ 2025-01-31 ┆ 0.1315 │
        │ 2025-02-03 ┆ 0.1315 │
        │ 2025-02-04 ┆ 0.1315 │
        └────────────┴────────┘
    """
    if any_is_empty(start):
        return pl.DataFrame()
    df = _buscar_dados_url(SerieBC.DI_OVER, start, end)
    if annualized:
        df = df.with_columns(
            (((pl.col("Value") + 1).pow(252)) - 1)
            .round(CASAS_DECIMAIS_ANUALIZADA)
            .alias("Value")
        )

    else:
        df = df.with_columns(pl.col("Value").round(CASAS_DECIMAIS_DIARIA))

    return df


def di_over(date: DateLike, annualized: bool = True) -> float:
    """Busca o valor da taxa DI Over para uma data específica.

    Função de conveniência que retorna apenas o valor (não o DataFrame) para a
    data especificada.

    Args:
        date: Data de referência para buscar a taxa DI Over.
        annualized: Se True, retorna a taxa anualizada (252 dias úteis por ano),
            caso contrário retorna a taxa diária.

    Returns:
        Taxa DI Over como float ou NaN se não disponível.

    Examples:
        >>> from pyield import bc
        >>> bc.di_over("31-05-2024")
        0.104

        >>> bc.di_over("28-01-2025", annualized=False)
        0.00045513
    """
    if any_is_empty(date):
        return float("nan")
    df = di_over_series(date, date, annualized)
    if df.is_empty():
        return float("nan")
    return df["Value"].item(0)
