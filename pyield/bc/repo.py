"""
Documentação da API do BC
    https://olinda.bcb.gov.br/olinda/servico/leiloes_selic/versao/v1/aplicacao#!/recursos/leiloes_compromissadas
Exemplo de chamada:
    https://olinda.bcb.gov.br/olinda/servico/leiloes_selic/versao/v1/odata/leiloes_compromissadas(dataLancamentoInicio=@dataLancamentoInicio,dataLancamentoFim=@dataLancamentoFim,horaInicio=@horaInicio,dataLiquidacao=@dataLiquidacao,dataRetorno=@dataRetorno,publicoPermitidoLeilao=@publicoPermitidoLeilao,nomeTipoOferta=@nomeTipoOferta)?@dataLancamentoInicio='2025-02-13'&@dataLancamentoFim='2025-02-13'&$top=100&$format=text/csv
"""

import io
import logging

import pandas as pd
import requests

from pyield import bday
from pyield import date_converter as dc
from pyield.date_converter import DateScalar
from pyield.retry import default_retry

"""Dicionário com o mapeamento das colunas da API do BC para o DataFrame final
Chaves com comentário serão descartadas ao final do processamento
A ordem das chaves será a ordem das colunas no DataFrame final"""
COLUMN_MAPPING = {
    # "id": "ID",
    "dataMovimento": "Date",
    "horaInicio": "StartTime",
    "publicoPermitidoLeilao": "AllowedParticipants",  # ['SomenteDealer', 'TodoMercado']
    "numeroComunicado": "CommunicationNumber",
    "nomeTipoOferta": "OfferType",
    # "ofertante": "Offerer", # Only Banco Central can offer
    "prazoDiasCorridos": "CDToMat",
    "dataLiquidacao": "Settlement",
    "dataRetorno": "Maturity",
    "volumeAceito": "AcceptedVolume",
    "taxaCorte": "CutRate",
    "percentualCorte": "CutPct",
}

BASE_API_URL = "https://olinda.bcb.gov.br/olinda/servico/leiloes_selic/versao/v1/odata/leiloes_compromissadas(dataLancamentoInicio=@dataLancamentoInicio,dataLancamentoFim=@dataLancamentoFim,horaInicio=@horaInicio,dataLiquidacao=@dataLiquidacao,dataRetorno=@dataRetorno,publicoPermitidoLeilao=@publicoPermitidoLeilao,nomeTipoOferta=@nomeTipoOferta)?"


@default_retry
def _load_from_url(url: str) -> pd.DataFrame:
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return pd.read_csv(
        io.StringIO(response.text),
        dtype_backend="numpy_nullable",
        decimal=",",
        date_format="%Y-%m-%d",
        parse_dates=["dataMovimento", "dataLiquidacao", "dataRetorno"],
    )


def _pre_process_df(df: pd.DataFrame) -> pd.DataFrame:
    # Remover colunas que não serão utilizadas
    keep_columns = [col for col in COLUMN_MAPPING.keys() if col in df.columns]
    return df[keep_columns].rename(columns=COLUMN_MAPPING)


def _process_df(df: pd.DataFrame) -> pd.DataFrame:
    df["StartTime"] = pd.to_datetime(df["StartTime"], format="%H:%M").dt.time
    # AcceptedVolume é em milhares de reais -> converter para reais
    df["AcceptedVolume"] = (1_000 * df["AcceptedVolume"]).round(0).astype("Int64")

    # Remove the percentage sign and round to 6 decimal places (4 decimal places in %)
    df["CutRate"] = (df["CutRate"] / 100).round(6)
    # df["BDtoReturn"] = bday.count(df["Date"], df["ReturnDate"])
    df["BDtoMat"] = bday.count(df["Date"], df["Maturity"])
    return df


def _adjust_null_values(df: pd.DataFrame) -> pd.DataFrame:
    # Onde não há volume aceito, os valores de corte devem ser nulos
    is_accepted = df["AcceptedVolume"] != 0
    df["CutRate"] = df["CutRate"].where(is_accepted, pd.NA)
    df["CutPct"] = df["CutPct"].where(is_accepted, pd.NA)
    return df


def _sort_and_reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    column_sequence = [
        "Date",
        "Settlement",
        "Maturity",
        "CDToMat",
        "BDtoMat",
        "StartTime",
        "AllowedParticipants",
        "CommunicationNumber",
        "OfferType",
        "AcceptedVolume",
        "CutRate",
        "CutPct",
    ]

    primary_sort_keys = ["Date", "StartTime", "OfferType"]
    return df[column_sequence].sort_values(by=primary_sort_keys).reset_index(drop=True)


def _fetch_df_from_url(url: str) -> pd.DataFrame:
    try:
        df = _load_from_url(url)
        if df.empty:
            logging.warning("No auction data found for the specified period.")
            return pd.DataFrame()
        df = _pre_process_df(df)
        df = _adjust_null_values(df)
        df = _process_df(df)
        df = _sort_and_reorder_columns(df)
        return df
    except Exception:
        logging.exception("Error fetching auction data from BC API.")
        return pd.DataFrame()


def repos(
    start: DateScalar | None = None,
    end: DateScalar | None = None,
) -> pd.DataFrame:
    """
    Recupera dados de leilões para um determinado período e tipo de leilão da API do BC.

    **Consultas de Período:**
    - Para consultar dados de um intervalo, forneça as datas de `start` e `end`.
      Exemplo: `auctions(start='2024-10-20', end='2024-10-27')`
    - Se apenas `start` for fornecido, a API do BC retornará dados de leilão a partir
      da data de `start` **até a data mais recente disponível**.
      Exemplo: `auctions(start='2024-10-20')`
    - Se apenas `end` for fornecido, a API do BC retornará dados de leilão **desde a
      data mais antiga disponível até a data de `end`**.
      Exemplo: `auctions(end='2024-10-27')`

    **Série Histórica Completa:**
    - Para recuperar a série histórica completa de leilões (desde 12/11/2012 até o
      último dia útil), chame a função sem fornecer os parâmetros `start` e `end`.
      Exemplo: `auctions()`

    Busca dados de leilões da API do BC para as datas de início e fim especificadas,
    filtrando os resultados diretamente na API pelo tipo de leilão, se especificado.
    O comportamento da função em relação aos parâmetros `start` e `end` segue o padrão
    da API do Banco Central:
    - Se `start` for fornecido e `end` não, a função retorna dados de `start` até o fim.
    - Se `end` for fornecido e `start` não, a API retorna dados do início até `end`.
    - Se ambos `start` e `end` forem omitidos, a API retorna a série histórica completa.

    Os dados podem ser filtrados pelo tipo de leilão especificado ("Sell" ou "Buy").
    Leilões de "Sell" são aqueles em que o Tesouro Nacional vende títulos ao mercado.
    Leilões de "Buy" são aqueles em que o Tesouro Nacional compra títulos do mercado.

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
        pd.DataFrame: Um DataFrame contendo dados de leilões para o período e tipo
            especificados. Em caso de erro ao buscar os dados, um DataFrame vazio
            é retornado e uma mensagem de erro é registrada no log.

    Notes:
        O DataFrame possui as seguintes colunas:
            - Date: Data do leilão.
            - Settlement: Data de liquidação do leilão.
            - Maturity: Data de retorno do leilão.
            - CDToMat: Prazo em dias corridos até o vencimento.
            - BDtoMat: Prazo em dias úteis até o vencimento.
            - StartTime: Hora de início do leilão.
            - AllowedParticipants: Participantes permitidos no leilão.
            - CommunicationNumber: Número do comunicado.
            - OfferType: Tipo de oferta do leilão.
            - AcceptedVolume: Volume aceito no leilão (em R$).
            - CutRate: Taxa de corte do leilão.
            - CutPct: Percentual de corte do leilão.

    """
    url = BASE_API_URL
    if start:
        start = dc.convert_input_dates(start)
        start_str = start.strftime("%Y-%m-%d")
        url += f"@dataLancamentoInicio='{start_str}'"

    if end:
        end = dc.convert_input_dates(end)
        end_str = end.strftime("%Y-%m-%d")
        url += f"&@dataLancamentoFim='{end_str}'"

    url += "&$format=text/csv"  # Adiciona o formato CSV ao final

    return _fetch_df_from_url(url)
