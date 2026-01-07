"""
Módulo para acessar a API de cotações PTAX do Banco Central do Brasil (BCB)

Exemplo de chamada à API:
    https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?@dataInicial='09-01-2025'&@dataFinalCotacao='09-10-2025'&$format=text/csv

Exemplo de resposta CSV da API do BCB:
cotacaoCompra, cotacaoVenda, dataHoraCotacao
2814         , 2828        , 1984-12-03 11:29:00.0
2814         , 2828        , 1984-12-03 16:38:00.0
2867         , 2881        , 1984-12-04 11:17:00.0
...
"5,4272"     , "5,4278"    , 2025-09-08 13:09:40.608
"5,4272"     , "5,4278"    , 2025-09-09 13:07:27.786
"5,4117"     , "5,4123"    , 2025-09-10 13:06:29.196
"""

import datetime as dt
import io
import logging

import polars as pl
import requests

import pyield.converters as cv
from pyield import clock
from pyield.retry import default_retry
from pyield.types import DateLike

logger = logging.getLogger(__name__)

# Dicionário com o mapeamento das colunas da API do BC para o DataFrame final
COLUMN_MAPPING = {
    "dataHoraCotacao": "DateTime",
    "cotacaoCompra": "BuyRate",
    "cotacaoVenda": "SellRate",
}

PTAX_API_URL = "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?"


def _build_api_url(start: dt.date, end: dt.date) -> str:
    start_str = start.strftime("%m-%d-%Y")
    end_str = end.strftime("%m-%d-%Y")

    # Monta a URL da API com as datas de início e fim
    url = PTAX_API_URL
    url += f"@dataInicial='{start_str}'"
    url += f"&@dataFinalCotacao='{end_str}'"
    url += "&$format=text/csv"  # Adiciona o formato CSV ao final
    return url


@default_retry
def _fetch_text_from_api(url: str) -> str:
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.text


def _parse_csv(csv_text: str) -> pl.DataFrame:
    """Faz o parse seguro do CSV da API PTAX.

    Evita depender de inferência heurística do Polars (que levou a tentar
    ler *i64* em valores decimais como "13,77") definindo dtypes explícitos e
    aplicando `strptime` controlado para a coluna de data/hora.

    Estratégia:
      1. Ler as colunas de taxas como Float64 (ou poderíamos usar Decimal futuramente)
         já aproveitando `decimal_comma=True` para normalizar a vírgula.
      2. Ler `dataHoraCotacao` como string e depois fazer parse explícito para
         Datetime em milissegundos usando o formato conhecido `%Y-%m-%d %H:%M:%S%.3f`.
      3. Forçar unidade de tempo "ms" (a API só tem milissegundos) removendo
         qualquer precisão fantasma.
    """

    # Schema mínimo explícito. Mantemos dataHoraCotacao como Utf8 para parse manual.
    schema = {
        "cotacaoCompra": pl.Float64,
        "cotacaoVenda": pl.Float64,
        "dataHoraCotacao": pl.String,
    }

    df = pl.read_csv(
        io.StringIO(csv_text),
        decimal_comma=True,  # converte "5,4372" para "5.4372" antes do cast
        schema_overrides=schema,
    )
    return df


def _process_df(df: pl.DataFrame) -> pl.DataFrame:
    parsed_dt = pl.col("DateTime").str.strptime(
        pl.Datetime(time_unit="ms"),
        format="%Y-%m-%d %H:%M:%S%.3f",
        strict=True,
    )

    df = (
        df.rename(COLUMN_MAPPING)
        .with_columns(
            parsed_dt.alias("DateTime"),
            parsed_dt.cast(pl.Date).alias("Date"),
            ((pl.col("BuyRate") + pl.col("SellRate")) / 2).round(5).alias("MidRate"),
        )
        .unique(subset=["Date"], keep="last")
        .select(["Date", "DateTime", "BuyRate", "SellRate", "MidRate"])
        .sort("DateTime")
    )
    return df


def ptax_series(
    start: DateLike | None = None,
    end: DateLike | None = None,
) -> pl.DataFrame:
    """Cotações de Dólar PTAX (taxa de câmbio)
    - Fonte: Banco Central do Brasil (BCB)
    - Frequência: Diária
    - Unidade: R$

    Documentação da API do BCB:

        https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/documentacao

    Exemplo de chamada à API:

        https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?@dataInicial='08-01-2025'&@dataFinalCotacao='08-05-2025'&$format=text/csv

    Consultas de Período:

    - Para consultar dados de um intervalo, forneça as datas de `start` e `end`.
    Exemplo:

            `ptax_series(start='20-10-2024', end='27-10-2024')`

    - Se apenas `start` for fornecido, a API do BC retornará dados a partir
    da data de `start` até a data mais recente disponível. Exemplo:

            `ptax_series(start='20-10-2024')`

    - Se apenas `end` for fornecido, a API do BC retornará dados desde a data mais
    antiga disponível até a data de `end`. Exemplo:

        `ptax_series(end='27-10-2024')`

    Série Histórica Completa:

    - Para recuperar a série histórica completa de leilões (desde 28.11.1984
    até o último dia útil), chame a função sem fornecer os parâmetros `start` e `end`.
    Exemplo:

            `ptax_series()`

    Busca dados de cotações de dólar PTAX (taxa de câmbio) para o período:

    - Se `start` for fornecido e `end` não, a função retorna dados de `start` até o fim.
    - Se `end` for fornecido e `start` não, a API retorna dados do início até `end`.
    - Se ambos `start` e `end` forem omitidos, a API retorna a série histórica completa.

    Args:
        start (DateLike, opcional): A data de início para a consulta dos leilões.
            Se `start` for fornecido e `end` for `None`, a API retornará dados de
            leilão a partir de `start` até a data mais recente disponível.
            Se `start` e `end` forem `None`, a série histórica completa será retornada.
            Padrão é `None`.
        end (DateLike, opcional): A data de fim para a consulta de dados de leilão.
            Se `end` for fornecido e `start` for `None`, a API retornará dados de
            leilão desde a data mais antiga disponível até a data de `end`.
            Se `start` e `end` forem `None`, a série histórica completa será retornada.
            Padrão é `None`.

    Returns:
        pl.DataFrame: Um DataFrame contendo os dados de cotações de dólar PTAX.
        Se não houver dados disponíveis para o período especificado, um DataFrame vazio
        será retornado.

    Examples:
        >>> from pyield import bc
        >>> bc.ptax_series(start="20-04-2025", end="25-04-2025")
        shape: (4, 5)
        ┌────────────┬─────────────────────────┬─────────┬──────────┬─────────┐
        │ Date       ┆ DateTime                ┆ BuyRate ┆ SellRate ┆ MidRate │
        │ ---        ┆ ---                     ┆ ---     ┆ ---      ┆ ---     │
        │ date       ┆ datetime[ms]            ┆ f64     ┆ f64      ┆ f64     │
        ╞════════════╪═════════════════════════╪═════════╪══════════╪═════════╡
        │ 2025-04-22 ┆ 2025-04-22 13:09:35.629 ┆ 5.749   ┆ 5.7496   ┆ 5.7493  │
        │ 2025-04-23 ┆ 2025-04-23 13:06:30.443 ┆ 5.6874  ┆ 5.688    ┆ 5.6877  │
        │ 2025-04-24 ┆ 2025-04-24 13:04:29.639 ┆ 5.6732  ┆ 5.6738   ┆ 5.6735  │
        │ 2025-04-25 ┆ 2025-04-25 13:09:26.592 ┆ 5.684   ┆ 5.6846   ┆ 5.6843  │
        └────────────┴─────────────────────────┴─────────┴──────────┴─────────┘

    Notes:
        Disponível desde 28.11.1984, refere-se às taxas administradas até março de 1990
        e às taxas livres a partir de então (Resolução 1690, de 18.3.1990). As taxas
        administradas são aquelas fixadas pelo Banco Central; a partir de março de 1992,
        essa taxa recebeu a denominação de taxa PTAX (fechamento). Até 30 de junho de
        2011, as taxas livres correspondiam à média das taxas efetivas de operações no
        mercado interbancário, ponderada pelo volume de transações do dia. A partir de
        1 de julho de 2011 (Circular 3506, de 23.9.2010), a Ptax passou a corresponder
        à média aritmética das taxas obtidas em quatro consultas diárias aos dealers de
        câmbio e refletem a taxa negociada no momento de abertura da janela de consulta;
        o boletim de fechamento PTAX corresponde à média aritmética das taxas dos
        boletins do dia.

        - Primeira data disponível: 28.11.1984
        - Última data disponível: data atual

        O DataFrame possui as seguintes colunas:

        - Date: Data da cotação.
        - DateTime: Data e hora da cotação.
        - BuyRate: Taxa de compra.
        - SellRate: Taxa de venda.
        - MidRate: Taxa média entre a compra/venda arredondada para 5 casas decimais.
    """
    if start:
        start = cv.convert_dates(start)
    else:
        start = dt.date(1984, 11, 28)  # Primeira data disponível na API

    if end:
        end = cv.convert_dates(end)
    else:
        end = clock.today()

    try:
        url = _build_api_url(start, end)
        text = _fetch_text_from_api(url)
        df = _parse_csv(text)
        if df.is_empty():
            logging.warning("No data found for the specified period.")
            return pl.DataFrame()
        df = _process_df(df)
        return df
    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error occurred: {http_err}")
        return pl.DataFrame()
    except Exception as e:
        logger.exception("Error fetching PTAX data from BC API: %s", e)
        return pl.DataFrame()


def ptax(date: DateLike) -> float:
    """Busca a cotação PTAX média de fechamento para uma data específica.

    Esta função é um wrapper para a função `ptax_series`, otimizada para
    buscar o valor de um único dia.

    Args:
        date (DateLike): A data para a qual a cotação PTAX é desejada.
            Pode ser uma string no formato "dd-mm-aaaa" ou um objeto date/datetime.

    Returns:
        float: O valor da PTAX (taxa média) para a data especificada.
               Retorna None se não houver cotação para a data
               (ex: feriado, fim de semana ou data futura).

    Examples:
        >>> from pyield import bc
        >>> # Busca a PTAX para um dia útil
        >>> bc.ptax("22-08-2025")
        5.4389

        >>> # Busca a PTAX para um fim de semana (sem dados)
        >>> bc.ptax("23-08-2025")
        nan
    """
    # Reutiliza a função ptax_series para buscar os dados para o dia específico.
    # Definir start e end com a mesma data busca a cotação para aquele dia.
    df_ptax = ptax_series(start=date, end=date)

    # Se o DataFrame estiver vazio, significa que não há cotação para a data.
    # Isso ocorre em fins de semana, feriados ou datas futuras.
    if df_ptax.is_empty():
        logger.warning(f"No PTAX data found for date: {date}")
        return float("nan")

    # A API retorna uma única linha para a cotação de fechamento de um dia.
    # A coluna "MidRate" representa a PTAX de fechamento.
    return df_ptax["MidRate"].item(0)
