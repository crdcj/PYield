import datetime as dt
import io
import logging
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from pyield import date_converter as dc
from pyield.date_converter import DateScalar

TIMEZONE_BZ = ZoneInfo("America/Sao_Paulo")

logger = logging.getLogger(__name__)

"""Dicionário com o mapeamento das colunas da API do BC para o DataFrame final
Chaves com comentário serão descartadas ao final do processamento
A ordem das chaves será a ordem das colunas no DataFrame final"""
COLUMN_MAPPING = {
    "dataHoraCotacao": "DateTime",
    "cotacaoCompra": "BuyRate",
    "cotacaoVenda": "SellRate",
}

PTAX_API_URL = "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?"


def _load_from_url(url: str) -> pd.DataFrame:
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return pd.read_csv(
        io.StringIO(response.text),
        dtype_backend="numpy_nullable",
        decimal=",",
        date_format="%Y-%m-%d %H:%M:%S.%f",
        parse_dates=["dataHoraCotacao"],
    )


def _rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Remover colunas que não serão utilizadas
    keep_columns = [col for col in COLUMN_MAPPING.keys() if col in df.columns]
    return df[keep_columns].rename(columns=COLUMN_MAPPING)


def _process_df(df: pd.DataFrame) -> pd.DataFrame:
    df["Date"] = df["DateTime"].dt.normalize()
    df["Time"] = df["DateTime"].dt.time
    df["BuyRate"] = df["BuyRate"].round(4)  # BC API retorna com 4 casas decimais
    df["SellRate"] = df["SellRate"].round(4)  # BC API retorna com 4 casas decimais

    df["MidRate"] = (df["BuyRate"] + df["SellRate"]) / 2
    df["MidRate"] = df["MidRate"].round(5)

    return df


def _reorder_and_sort_columns(df: pd.DataFrame) -> pd.DataFrame:
    column_sequence = ["Date", "Time", "DateTime", "BuyRate", "SellRate", "MidRate"]
    return df[column_sequence].sort_values(by=["DateTime"]).reset_index(drop=True)


def _fetch_df_from_url(url: str) -> pd.DataFrame:
    try:
        df = _load_from_url(url)
        if df.empty:
            logging.warning("No data found for the specified period.")
            return pd.DataFrame()
        df = _rename_columns(df)
        df = _process_df(df)
        df = _reorder_and_sort_columns(df)
        return df
    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error occurred: {http_err}")
        return pd.DataFrame()
    except Exception:
        logger.exception("Error fetching auction data from BC API.")
        return pd.DataFrame()


def ptax_series(
    start: DateScalar | None = None,
    end: DateScalar | None = None,
) -> pd.DataFrame:
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

            `ptax_series(start='2024-10-20', end='2024-10-27')`

    - Se apenas `start` for fornecido, a API do BC retornará dados a partir
    da data de `start` até a data mais recente disponível. Exemplo:

            `ptax_series(start='2024-10-20')`

    - Se apenas `end` for fornecido, a API do BC retornará dados desde a data mais
    antiga disponível até a data de `end`. Exemplo:

        `ptax_series(end='2024-10-27')`

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
        start (DateScalar, opcional): A data de início para a consulta dos leilões.
            Se `start` for fornecido e `end` for `None`, a API retornará dados de
            leilão a partir de `start` até a data mais recente disponível.
            Se `start` e `end` forem `None`, a série histórica completa será retornada.
            Padrão é `None`.
        end (DateScalar, opcional): A data de fim para a consulta de dados de leilão.
            Se `end` for fornecido e `start` for `None`, a API retornará dados de
            leilão desde a data mais antiga disponível até a data de `end`.
            Se `start` e `end` forem `None`, a série histórica completa será retornada.
            Padrão é `None`.

    Returns:
        pd.DataFrame: Um DataFrame contendo os dados de cotações de dólar PTAX.
        Se não houver dados disponíveis para o período especificado, um DataFrame vazio
        será retornado.

    Examples:
        >>> from pyield import bc
        >>> df = yd.bc.ptax_series(start="01-01-2025", end="05-01-2025")
        >>> selected_columns = ["Date", "BuyRate", "SellRate", "MidRate"]
        >>> df[selected_columns]
                Date  BuyRate  SellRate  MidRate
        0 2025-01-02    6.208    6.2086   6.2083
        1 2025-01-03   6.1557    6.1563    6.156

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
        - Time: Hora da cotação.
        - DateTime: Data e hora da cotação.
        - BuyRate: Taxa de compra.
        - SellRate: Taxa de venda.
        - MidRate: Taxa média entre a taxa de compra e venda.


    """
    if start:
        start = dc.convert_input_dates(start)
    else:
        start = pd.Timestamp("1984-11-28")

    if end:
        end = dc.convert_input_dates(end)
    else:
        end = dt.datetime.now(TIMEZONE_BZ).date()

    start_str = start.strftime("%m-%d-%Y")
    end_str = end.strftime("%m-%d-%Y")

    # Monta a URL da API com as datas de início e fim
    url = PTAX_API_URL
    url += f"@dataInicial='{start_str}'"
    url += f"&@dataFinalCotacao='{end_str}'"
    url += "&$format=text/csv"  # Adiciona o formato CSV ao final

    return _fetch_df_from_url(url)
