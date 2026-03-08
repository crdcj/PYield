import datetime as dt
import logging
from typing import Literal

import polars as pl
import polars.selectors as cs
import requests
from dateutil.relativedelta import relativedelta

import pyield._internal.converters as cv
import pyield.b3.common as cm
from pyield import bday, clock
from pyield._internal.retry import retry_padrao
from pyield._internal.types import DateLike, any_is_empty
from pyield.b3.price_report import fetch_price_report
from pyield.fwd import forwards

OpcoesContrato = Literal[
    "DI1",
    "DDI",
    "FRC",
    "FRO",
    "DAP",
    "DOL",
    "WDO",
    "IND",
    "WIN",
    "CPM",
]

URL_BASE_INTRADAY = "https://cotacao.b3.com.br/mds/api/v1/DerivativeQuotation"

# Pregão abre às 9:00, porém os dados têm atraso de 15 minutos.
# Esperar 1 minuto adicional para garantir que estejam disponíveis (9:16h).
HORA_INICIO_INTRADAY = dt.time(9, 16)
# Pregão fecha às 18:00h, momento em que os dados consolidados começam a ser preparados.
HORA_FIM_INTRADAY = dt.time(18, 30)

JANELA_DADOS_RECENTES = relativedelta(months=1)

# Lista de contratos que negociam por taxa (juros/cupom).
# Nestes contratos, as colunas OHLC são taxas e precisam ser divididas por 100.
CONTRATOS_TAXA = {"DI1", "DAP", "DDI", "FRC", "FRO"}

CONFIG_COLUNAS_HISTORICO_RECENTE = {
    "Instrumento financeiro": (pl.String, "TickerSymbol"),
    "Código ISIN": (pl.String, "ISINCode"),
    "Segmento": (pl.String, "Segment"),
    # Lemos como "Value" genérico, pois pode ser Taxa ou Preço
    "Preço de abertura": (pl.Float64, "OpenValue"),
    "Preço mínimo": (pl.Float64, "MinValue"),
    "Preço máximo": (pl.Float64, "MaxValue"),
    "Preço médio": (pl.Float64, "AvgValue"),
    "Preço de fechamento": (pl.Float64, "CloseValue"),
    "Última oferta de compra": (pl.Float64, "LastBidValue"),
    "Última oferta de venda": (pl.Float64, "LastAskValue"),
    "Oscilação": (pl.Float64, "Oscillation"),
    "Variação": (pl.Float64, "Variation"),
    "Ajuste": (pl.Float64, "SettlementPrice"),
    "Preço de referência": (pl.Float64, "ReferencePrice"),
    "Ajuste de referência": (pl.Float64, "SettlementRate"),
    "Valor do ajuste por contrato (R$)": (pl.Float64, "AdjustmentValuePerContract"),
    "Quantidade de negócios": (pl.Int64, "TradeCount"),
    "Quantidade de contratos": (pl.Int64, "TradeVolume"),
    "Volume financeiro": (pl.Float64, "FinancialVolume"),
}

ESQUEMA_CSV_HISTORICO_RECENTE = {
    k: v[0] for k, v in CONFIG_COLUNAS_HISTORICO_RECENTE.items()
}
MAPA_RENOMEACAO_HISTORICO_RECENTE = {
    k: v[1] for k, v in CONFIG_COLUNAS_HISTORICO_RECENTE.items()
}

logger = logging.getLogger(__name__)


def _data_intraday_valida(data_verificacao: dt.date) -> bool:
    """Verifica se a data é um dia de negociação intraday."""
    if not cm.data_negociacao_valida(data_verificacao):
        return False

    return data_verificacao == clock.today()


@retry_padrao
def _buscar_json_intraday(codigo_contrato: str) -> list[dict]:
    url = f"{URL_BASE_INTRADAY}/{codigo_contrato}"
    cabecalhos = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"  # noqa: E501
    }
    resposta = requests.get(url, headers=cabecalhos, timeout=10)
    resposta.raise_for_status()
    resposta.encoding = "utf-8"

    if "Quotation not available" in resposta.text or "curPrc" not in resposta.text:
        data_log = clock.now().strftime("%d-%m-%Y %H:%M")
        logger.warning("Sem dados intraday para %s em %s.", codigo_contrato, data_log)
        return []

    return resposta.json()["Scty"]


def _converter_json_intraday(dados_json: list[dict]) -> pl.DataFrame:
    if not dados_json:
        return pl.DataFrame()
    return pl.json_normalize(dados_json)


def _processar_colunas_intraday(df: pl.DataFrame) -> pl.DataFrame:
    df.columns = [
        c.replace("SctyQtn.", "").replace("asset.AsstSummry.", "") for c in df.columns
    ]

    mapa_renomeacao = {
        "symb": "TickerSymbol",
        "bottomLmtPric": "MinLimitRate",
        "prvsDayAdjstmntPric": "PrevSettlementRate",
        "topLmtPric": "MaxLimitRate",
        "opngPric": "OpenRate",
        "minPric": "MinRate",
        "maxPric": "MaxRate",
        "avrgPric": "AvgRate",
        "curPrc": "LastRate",
        "grssAmt": "FinancialVolume",
        "mtrtyCode": "ExpirationDate",
        "opnCtrcts": "OpenContracts",
        "tradQty": "TradeCount",
        "traddCtrctsQty": "TradeVolume",
        "buyOffer.price": "LastAskRate",
        "sellOffer.price": "LastBidRate",
    }
    return df.select(mapa_renomeacao.keys()).rename(mapa_renomeacao, strict=False)


def _preprocessar_df_intraday(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.with_columns(
            pl.col("ExpirationDate").str.to_date(format="%Y-%m-%d", strict=False)
        )
        .drop_nulls(subset=["ExpirationDate"])
        .filter(pl.col("TickerSymbol") != "DI1D")
        .sort("ExpirationDate")
    )


def _processar_df_intraday(df: pl.DataFrame, codigo_contrato: str) -> pl.DataFrame:
    data_negociacao = bday.last_business_day()
    df = df.with_columns(
        cs.contains("Rate").truediv(100).round(5),
        TradeDate=data_negociacao,
        LastUpdate=clock.now() - dt.timedelta(minutes=15),
        DaysToExp=(pl.col("ExpirationDate") - data_negociacao).dt.total_days(),
    )

    df = df.with_columns(BDaysToExp=bday.count_expr(data_negociacao, "ExpirationDate"))

    if codigo_contrato in {"DI1", "DAP"}:
        taxa_fwd = forwards(bdays=df["BDaysToExp"], rates=df["LastRate"])
        anos_uteis = pl.col("BDaysToExp") / 252
        ultimo_preco = 100_000 / ((1 + pl.col("LastRate")) ** anos_uteis)
        df = df.with_columns(LastPrice=ultimo_preco.round(2), ForwardRate=taxa_fwd)

    if codigo_contrato == "DI1":
        df = df.with_columns(DV01=cm.expr_dv01("BDaysToExp", "LastRate", "LastPrice"))

    return df.filter(pl.col("DaysToExp") > 0)


def _selecionar_e_reordenar_colunas_intraday(df: pl.DataFrame) -> pl.DataFrame:
    todas_colunas = [
        "TradeDate",
        "LastUpdate",
        "TickerSymbol",
        "ExpirationDate",
        "BDaysToExp",
        "DaysToExp",
        "OpenContracts",
        "TradeCount",
        "TradeVolume",
        "FinancialVolume",
        "DV01",
        "LastPrice",
        "PrevSettlementRate",
        "MinLimitRate",
        "MaxLimitRate",
        "OpenRate",
        "MinRate",
        "AvgRate",
        "MaxRate",
        "LastAskRate",
        "LastBidRate",
        "LastRate",
        "ForwardRate",
    ]
    colunas_reordenadas = [coluna for coluna in todas_colunas if coluna in df.columns]
    return df.select(colunas_reordenadas)


def fetch_intraday_df(codigo_contrato: str) -> pl.DataFrame:
    """Busca os dados intraday mais recentes da B3."""
    try:
        dados_json = _buscar_json_intraday(codigo_contrato)
        if not dados_json:
            return pl.DataFrame()

        return (
            _converter_json_intraday(dados_json)
            .pipe(_processar_colunas_intraday)
            .pipe(_preprocessar_df_intraday)
            .pipe(_processar_df_intraday, codigo_contrato)
            .pipe(_selecionar_e_reordenar_colunas_intraday)
        )
    except Exception as erro:
        logger.exception(
            "CRITICAL: Pipeline intraday falhou para %s. Erro: %s",
            codigo_contrato,
            erro,
        )
        return pl.DataFrame()


@retry_padrao
def _buscar_csv_historico_recente(data: dt.date) -> bytes:
    """Busca o CSV diário de derivativos consolidados na B3."""
    url = "https://arquivos.b3.com.br/bdi/table/export/csv"
    parametros = {"lang": "pt-BR"}
    data_str = data.strftime("%Y-%m-%d")
    carga = {
        "Name": "ConsolidatedTradesDerivatives",
        "Date": data_str,
        "FinalDate": data_str,
        "ClientId": "",
        "Filters": {},
    }

    cabecalhos = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",  # noqa: E501
        "Accept": "application/json, text/plain, */*",
    }

    resposta = requests.post(
        url, params=parametros, json=carga, headers=cabecalhos, timeout=(5, 30)
    )
    resposta.raise_for_status()
    return resposta.content


def _parsear_df_bruto(csv_bytes: bytes) -> pl.DataFrame:
    """Lê o CSV bruto em um DataFrame Polars."""
    return pl.read_csv(
        csv_bytes.replace(b".", b""),
        separator=";",
        skip_lines=2,
        null_values=["-"],
        decimal_comma=True,
        schema_overrides=ESQUEMA_CSV_HISTORICO_RECENTE,
        encoding="utf-8-sig",
    )


def _preprocessar_df_historico_recente(
    df: pl.DataFrame, codigo_contrato: str
) -> pl.DataFrame:
    return df.rename(MAPA_RENOMEACAO_HISTORICO_RECENTE, strict=False).filter(
        pl.col("TickerSymbol").str.starts_with(codigo_contrato),
        pl.col("TickerSymbol").str.len_chars().is_in([6, 13]),
    )


def _processar_df_historico_recente(
    df: pl.DataFrame, data_referencia: dt.date, codigo_contrato: str
) -> pl.DataFrame:
    df = df.with_columns(
        BDaysToExp=bday.count_expr(data_referencia, "ExpirationDate"),
        DaysToExp=(df["ExpirationDate"] - pl.lit(data_referencia)).dt.total_days(),
        TradeDate=data_referencia,
    ).filter(pl.col("DaysToExp") > 0)

    eh_taxa = codigo_contrato in CONTRATOS_TAXA
    sufixo_destino = "Rate" if eh_taxa else "Price"

    colunas_renomear = [c for c in df.columns if c.endswith("Value")]
    mapa_renomeacao = {c: c.replace("Value", sufixo_destino) for c in colunas_renomear}
    df = df.rename(mapa_renomeacao)

    if eh_taxa:
        colunas_taxa = [c for c in df.columns if c.endswith("Rate")]
        df = df.with_columns(pl.col(colunas_taxa).truediv(100).round(6))

    if (
        codigo_contrato == "DI1"
        and "SettlementPrice" in df.columns
        and "SettlementRate" in df.columns
    ):
        df = df.with_columns(
            DV01=cm.expr_dv01("BDaysToExp", "SettlementRate", "SettlementPrice")
        )

    if codigo_contrato in {"DI1", "DAP"} and "SettlementRate" in df.columns:
        df = df.with_columns(
            ForwardRate=forwards(bdays=df["BDaysToExp"], rates=df["SettlementRate"])
        )

    return df


def _selecionar_e_reordenar_colunas_historico_recente(df: pl.DataFrame) -> pl.DataFrame:
    ordem_preferida = [
        "TradeDate",
        "ISINCode",
        "TickerSymbol",
        "ExpirationDate",
        "BDaysToExp",
        "DaysToExp",
        "DV01",
        "TradeCount",
        "TradeVolume",
        "FinancialVolume",
        "AdjustmentValuePerContract",
        "OpenPrice",
        "MinPrice",
        "MaxPrice",
        "AvgPrice",
        "ClosePrice",
        "LastBidPrice",
        "LastAskPrice",
        "Oscillation",
        "Variation",
        "SettlementPrice",
        "OpenRate",
        "MinRate",
        "MaxRate",
        "AvgRate",
        "CloseRate",
        "LastBidRate",
        "LastAskRate",
        "SettlementRate",
        "ForwardRate",
    ]
    colunas_existentes = [c for c in ordem_preferida if c in df.columns]
    return df.select(colunas_existentes)


def _buscar_df_historico_recente(data: dt.date, codigo_contrato: str) -> pl.DataFrame:
    """Busca o histórico recente de futuros na B3 para a data informada."""
    try:
        csv_texto = _buscar_csv_historico_recente(data)
        if not csv_texto:
            return pl.DataFrame()

        df = _parsear_df_bruto(csv_texto)
        df = _preprocessar_df_historico_recente(df, codigo_contrato)
        if df.is_empty():
            return pl.DataFrame()

        df = cm.adicionar_vencimento(df, codigo_contrato, coluna_ticker="TickerSymbol")
        df = _processar_df_historico_recente(df, data, codigo_contrato)
        df = _selecionar_e_reordenar_colunas_historico_recente(df)

        return df.sort("ExpirationDate")
    except Exception as erro:
        logger.exception(
            "CRITICAL: Falha ao processar histórico recente do contrato %s para %s. Erro: %s",
            codigo_contrato,
            data,
            erro,
        )
        return pl.DataFrame()


def buscar_df_historico(data: dt.date, codigo_contrato: str) -> pl.DataFrame:
    """Busca o histórico com priorização de fonte por recência da data."""
    data_limite_recente = clock.today() - JANELA_DADOS_RECENTES

    if data <= data_limite_recente:
        try:
            return fetch_price_report(
                date=data, contract_code=codigo_contrato, source_type="SPR"
            )
        except Exception:
            return pl.DataFrame()

    df_recente = _buscar_df_historico_recente(data, codigo_contrato)
    if not df_recente.is_empty():
        return df_recente

    try:
        return fetch_price_report(
            date=data, contract_code=codigo_contrato, source_type="SPR"
        )
    except Exception:
        return pl.DataFrame()


def futures(
    date: DateLike,
    contract_code: OpcoesContrato | str,
) -> pl.DataFrame:
    """
    Fetches data for a specified futures contract based on type and reference date.

    Args:
        contract_code (str): The B3 futures contract code identifying the derivative.
            Supported contract codes are:
            - "DI1": One-day Interbank Deposit Futures (Futuro de DI) from B3.
            - "DDI": DI x U.S. Dollar Spread Futures (Futuro de Cupom Cambial) from B3.
            - "FRC": Forward Rate Agreement (FRA).
            - "FRO": FRA de Cupom Cambial (OC1).
            - "DAP": DI x IPCA Spread Futures.
            - "DOL": U.S. Dollar Futures from B3.
            - "WDO": Mini U.S. Dollar Futures from B3.
            - "IND": Ibovespa Futures from B3.
            - "WIN": Mini Ibovespa Futures from B3.
            - "CPM": COPOM Rate Expectation Futures.
        date (DateLike): The reference date for fetching the data.

    Returns:
        pl.DataFrame: DataFrame containing the fetched data for the specified futures
            contract.

    Raises:
        ValueError: If the futures contract code is not recognized or supported.

    Examples:
        >>> df = futures("31-05-2024", "DI1")
        >>> {"TradeDate", "TickerSymbol", "ExpirationDate", "SettlementRate"}.issubset(
        ...     set(df.columns)
        ... )
        True
        >>> df.shape[0] > 0
        True

        >>> df = futures("31-05-2024", "DAP")
        >>> {"TradeDate", "TickerSymbol", "ExpirationDate", "SettlementRate"}.issubset(
        ...     set(df.columns)
        ... )
        True
        >>> df.shape[0] > 0
        True

    """
    if any_is_empty(date, contract_code):
        return pl.DataFrame()
    data_negociacao = cv.converter_datas(date)

    if not cm.data_negociacao_valida(data_negociacao):
        logger.warning(
            "A data %s não é válida. Retornando DataFrame vazio.",
            data_negociacao,
        )
        return pl.DataFrame()

    contrato_selecionado = str(contract_code).upper()

    if _data_intraday_valida(data_negociacao):
        horario_atual = clock.now().time()
        if horario_atual < HORA_INICIO_INTRADAY:
            logger.warning("Mercado ainda não abriu. Retornando DataFrame vazio.")
            return pl.DataFrame()

        if horario_atual >= HORA_FIM_INTRADAY:
            df_hist = buscar_df_historico(data_negociacao, contrato_selecionado)
            if not df_hist.is_empty():
                logger.info("Dados consolidados disponíveis. Usando histórico.")
                return df_hist

        return fetch_intraday_df(contrato_selecionado)

    return buscar_df_historico(data_negociacao, contrato_selecionado)
