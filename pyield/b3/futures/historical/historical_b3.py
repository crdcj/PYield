import datetime as dt
import logging

import polars as pl
import requests

import pyield.b3.common as cm
from pyield import bday
from pyield.fwd import forwards
from pyield.retry import retry_padrao

# Lista de contratos que negociam por taxa (juros/cupom).
# Nestes contratos, as colunas OHLC são taxas e precisam ser divididas por 100.
CONTRATOS_TAXA = {"DI1", "DAP", "DDI", "FRC", "FRO"}
CONFIG_COLUNAS = {
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

ESQUEMA_CSV = {k: v[0] for k, v in CONFIG_COLUNAS.items()}
MAPA_RENOMEACAO = {k: v[1] for k, v in CONFIG_COLUNAS.items()}

logger = logging.getLogger(__name__)


@retry_padrao
def _buscar_csv(data: dt.date) -> bytes:
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

    # 3. Cabeçalhos (Headers)
    # O User-Agent é essencial para simular um navegador e evitar bloqueios
    cabecalhos = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",  # noqa
        "Accept": "application/json, text/plain, */*",
    }

    resposta = requests.post(
        url, params=parametros, json=carga, headers=cabecalhos, timeout=(5, 30)
    )
    resposta.raise_for_status()
    resposta.encoding = "utf-8-sig"
    return resposta.content


def _parsear_df_bruto(csv_bytes: bytes) -> pl.DataFrame:
    """Lê o CSV bruto em um DataFrame Polars."""
    df = pl.read_csv(
        csv_bytes.replace(b".", b""),
        separator=";",
        skip_lines=2,
        null_values=["-"],
        decimal_comma=True,
        schema_overrides=ESQUEMA_CSV,
        encoding="utf-8-sig",
    )
    return df


def _preprocessar_df(df: pl.DataFrame, codigo_contrato: str) -> pl.DataFrame:
    """Renomeia e filtra o DataFrame para o contrato desejado."""
    df = df.rename(MAPA_RENOMEACAO, strict=False).filter(
        pl.col("TickerSymbol").str.contains(codigo_contrato),
        pl.col("TickerSymbol").str.len_chars() == 6,  # noqa
    )

    return df


def _processar_df(
    df: pl.DataFrame, data_referencia: dt.date, codigo_contrato: str
) -> pl.DataFrame:
    # 1. Datas de Vencimento
    df = df.with_columns(
        BDaysToExp=bday.count_expr(data_referencia, "ExpirationDate"),
        DaysToExp=(df["ExpirationDate"] - pl.lit(data_referencia)).dt.total_days(),
        TradeDate=data_referencia,
    ).filter(pl.col("DaysToExp") > 0)

    # 2. Renomeação Dinâmica (Rate vs Price)
    # Se for contrato de taxa, as colunas "Value" viram "Rate"
    # Se for contrato de preço, as colunas "Value" viram "Price"
    eh_taxa = codigo_contrato in CONTRATOS_TAXA
    sufixo_destino = "Rate" if eh_taxa else "Price"

    colunas_renomear = [c for c in df.columns if c.endswith("Value")]
    mapa_renomeacao = {c: c.replace("Value", sufixo_destino) for c in colunas_renomear}
    df = df.rename(mapa_renomeacao)

    # 3. Tratamento Específico de Taxas
    if eh_taxa:
        # Pega todas as colunas que agora terminam em "Rate" (incluindo SettlementRate)
        colunas_taxa = [c for c in df.columns if c.endswith("Rate")]

        # Divide por 100 para transformar percentual em decimal (14.50 -> 0.1450)
        df = df.with_columns((pl.col(colunas_taxa) / 100).round(6))

    # 4. Cálculo do DV01 (Apenas para DI1 e se tivermos as colunas necessárias)
    # SettlementPrice aqui já é o PU vindo do CSV (Ex: 99.000)
    # SettlementRate aqui já é a taxa decimal (Ex: 0.14)
    if (
        codigo_contrato == "DI1"
        and "SettlementPrice" in df.columns
        and "SettlementRate" in df.columns
    ):
        # DV01 = (Duration / (1 + Taxa)) * PU * 0.0001
        # Duration Modificada * PU * 1bp
        duracao = pl.col("BDaysToExp") / 252
        duracao_mod = duracao / (1 + pl.col("SettlementRate"))
        df = df.with_columns(DV01=duracao_mod * pl.col("SettlementPrice") * 0.0001)

    # 5. Forward Rates (Para DI1 e DAP)
    if codigo_contrato in {"DI1", "DAP"} and "SettlementRate" in df.columns:
        # Assume que forwards aceita taxa decimal e dias úteis
        df = df.with_columns(
            ForwardRate=forwards(bdays=df["BDaysToExp"], rates=df["SettlementRate"])
        )

    return df


def _selecionar_e_reordenar_colunas(df: pl.DataFrame) -> pl.DataFrame:
    """Seleciona e ordena colunas conforme a preferência."""
    # Define a ordem preferida, mas só seleciona o que existe no DF
    ordem_preferida = [
        "TradeDate",
        "ISINCode",
        "TickerSymbol",
        "ExpirationDate",
        "BDaysToExp",
        "DaysToExp",
        # "Segment",
        "DV01",
        "TradeCount",
        "TradeVolume",
        "FinancialVolume",
        "AdjustmentValuePerContract",
        # "ReferencePrice",
        # Colunas de Preço (vão existir para DOL, IND, etc)
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
        # Colunas de Taxa (vão existir para DI1, DAP, etc)
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


def _buscar_df_historico_b3(data: dt.date, codigo_contrato: str) -> pl.DataFrame:
    """Busca o histórico de futuros na B3 para a data informada."""
    try:
        # Tenta baixar os dados
        csv_texto = _buscar_csv(data)

        # Se veio vazio ou nulo, retorna vazio
        if not csv_texto:
            return pl.DataFrame()

        # Tenta fazer o parse e processamento
        df = _parsear_df_bruto(csv_texto)
        df = _preprocessar_df(df, codigo_contrato)

        if df.is_empty():
            return pl.DataFrame()

        df = cm._adicionar_vencimento(df, codigo_contrato, coluna_ticker="TickerSymbol")

        df = _processar_df(df, data, codigo_contrato)
        df = _selecionar_e_reordenar_colunas(df)

        return df.sort("ExpirationDate")

    except Exception as e:
        logger.exception(
            "CRITICAL: Falha ao processar o contrato %s para %s. Erro: %s",
            codigo_contrato,
            data,
            e,
        )
        return pl.DataFrame()
