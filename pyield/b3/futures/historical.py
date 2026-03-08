import datetime as dt
import logging

import polars as pl
import requests
from dateutil.relativedelta import relativedelta

import pyield.b3.common as cm
from pyield import bday, clock
from pyield._internal.retry import retry_padrao
from pyield.b3.price_report import fetch_price_report
from pyield.fwd import forwards

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


def historical(data: dt.date, codigo_contrato: str) -> pl.DataFrame:
    """Busca histórico de futuros com priorização de fonte por recência da data."""
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
