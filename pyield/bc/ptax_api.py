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
import logging

import polars as pl
import requests

import pyield._internal.converters as cv
from pyield import clock
from pyield._internal.retry import retry_padrao
from pyield._internal.types import DateLike

registro = logging.getLogger(__name__)

# Mapeamento unificado: coluna da API → (nome final, dtype)
MAPA_COLUNAS = {
    "cotacaoCompra": ("BuyRate", pl.Float64),
    "cotacaoVenda": ("SellRate", pl.Float64),
    "dataHoraCotacao": ("DateTime", pl.String),
}

ESQUEMA_API = {col: dtype for col, (_, dtype) in MAPA_COLUNAS.items()}
MAPEAMENTO_COLUNAS = {col: alias for col, (alias, _) in MAPA_COLUNAS.items()}

URL_API_PTAX = "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?"


def _montar_url_api(inicio: dt.date, fim: dt.date) -> str:
    inicio_str = inicio.strftime("%m-%d-%Y")
    fim_str = fim.strftime("%m-%d-%Y")

    # Monta a URL da API com as datas de início e fim
    url = URL_API_PTAX
    url += f"@dataInicial='{inicio_str}'"
    url += f"&@dataFinalCotacao='{fim_str}'"
    url += "&$format=text/csv"  # Adiciona o formato CSV ao final
    return url


@retry_padrao
def _buscar_texto_api(url: str) -> bytes:
    resposta = requests.get(url, timeout=10)
    resposta.raise_for_status()
    return resposta.content


def _ler_csv(conteudo_csv: bytes) -> pl.DataFrame:
    """Lê o CSV (texto) da API PTAX em um DataFrame Polars com esquema definido.

    Usa decimal_comma=True para tratar números no formato brasileiro ("5,4372").
    Mantém dataHoraCotacao como String para parse manual posterior em _processar_df.
    """
    return pl.read_csv(
        conteudo_csv,
        decimal_comma=True,
        schema_overrides=ESQUEMA_API,
    )


def _processar_df(df: pl.DataFrame) -> pl.DataFrame:
    df = (
        df.rename(MAPEAMENTO_COLUNAS)
        .with_columns(
            pl.col("DateTime").str.to_datetime(
                format="%Y-%m-%d %H:%M:%S%.3f", strict=False
            )
        )
        .with_columns(
            Date=pl.col("DateTime").cast(pl.Date),
            MidRate=((pl.col("BuyRate") + pl.col("SellRate")) / 2).round(5),
        )
        .unique(subset=["Date"], keep="last")
        .select("Date", "DateTime", "BuyRate", "SellRate", "MidRate")
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
        start = cv.converter_datas(start)
    else:
        start = dt.date(1984, 11, 28)  # Primeira data disponível na API

    if end:
        end = cv.converter_datas(end)
    else:
        end = clock.today()

    try:
        url = _montar_url_api(start, end)
        texto = _buscar_texto_api(url)
        df = _ler_csv(texto)
        if df.is_empty():
            registro.warning("Nenhum dado encontrado para o período informado.")
            return pl.DataFrame()
        df = _processar_df(df)
        return df
    except Exception as e:
        registro.exception(f"Erro ao buscar dados PTAX na API do BC: {e}")
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
               Retorna nan se não houver cotação para a data
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
    # Reutiliza ptax_series para buscar os dados do dia específico.
    # Definir start e end com a mesma data busca a cotação daquele dia.
    dados_ptax = ptax_series(start=date, end=date)

    # Se o DataFrame estiver vazio, não há cotação para a data.
    # Isso ocorre em fins de semana, feriados ou datas futuras.
    if dados_ptax.is_empty():
        registro.warning(f"Sem dados de PTAX para a data: {date}")
        return float("nan")

    # A API retorna uma única linha para a cotação de fechamento do dia.
    # A coluna "MidRate" representa a PTAX de fechamento.
    return dados_ptax["MidRate"].item(0)
