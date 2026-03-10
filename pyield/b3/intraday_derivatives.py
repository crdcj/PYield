import logging

import polars as pl
import requests

from pyield._internal.retry import retry_padrao

URL_BASE_INTRADAY = "https://cotacao.b3.com.br/mds/api/v1/DerivativeQuotation"

logger = logging.getLogger(__name__)

# --- Mapeamento de Colunas ---

# Estrutura: (nome_json_normalize, nome_canonico, tipo_polars)
# Colunas numéricas de cotação/limites usam sufixo "Value" no output bruto.
# Semântica de contrato (Rate/Price) é responsabilidade do módulo consumidor.
# Colunas opcionais (ofertas, opções) ficam no final; são incluídas somente
# quando presentes no payload.
COLUNAS_INTRADAY: list[tuple[str, str, type[pl.DataType]]] = [
    ("symb", "TickerSymbol", pl.String),
    ("desc", "Description", pl.String),
    ("asset.code", "AssetCode", pl.String),
    ("mkt.cd", "MarketCode", pl.String),
    ("asset.AsstSummry.mtrtyCode", "ExpirationDate", pl.Date),
    ("SctyQtn.prvsDayAdjstmntPric", "PrevSettlementValue", pl.Float64),
    ("SctyQtn.bottomLmtPric", "MinLimitValue", pl.Float64),
    ("SctyQtn.topLmtPric", "MaxLimitValue", pl.Float64),
    ("SctyQtn.opngPric", "OpenValue", pl.Float64),
    ("SctyQtn.minPric", "MinValue", pl.Float64),
    ("SctyQtn.maxPric", "MaxValue", pl.Float64),
    ("SctyQtn.avrgPric", "AvgValue", pl.Float64),
    ("SctyQtn.curPrc", "LastValue", pl.Float64),
    ("SctyQtn.exrcPric", "ExerciseValue", pl.Float64),
    ("asset.AsstSummry.opnCtrcts", "OpenContracts", pl.Int64),
    ("asset.AsstSummry.grssAmt", "FinancialVolume", pl.Float64),
    ("asset.AsstSummry.tradQty", "TradeCount", pl.Int64),
    ("asset.AsstSummry.traddCtrctsQty", "TradeVolume", pl.Int64),
    ("buyOffer.price", "BuyOfferValue", pl.Float64),
    ("sellOffer.price", "SellOfferValue", pl.Float64),
    ("asset.SdTpCd.desc", "SideTypeDescription", pl.String),
]

# Mapa de tipos para cast inicial usando os nomes do json_normalize.
TIPOS_INTRADAY = pl.Schema({nome_orig: tipo for nome_orig, _, tipo in COLUNAS_INTRADAY})


def _mapa_renomeacao_intraday() -> dict[str, str]:
    """Constrói dicionário {nome_json_normalize: nome_canonico}."""
    return {nome_orig: nome_novo for nome_orig, nome_novo, _ in COLUNAS_INTRADAY}


@retry_padrao
def _buscar_json_intraday(codigo_contrato: str) -> list[dict]:
    url = f"{URL_BASE_INTRADAY}/{codigo_contrato}"
    cabecalhos = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"  # noqa: E501
    }
    resposta = requests.get(url, headers=cabecalhos, timeout=10)
    resposta.raise_for_status()
    resposta.encoding = "utf-8"

    if "Quotation not available" in resposta.text:
        return []

    return resposta.json()["Scty"]


def _converter_json_intraday(dados_json: list[dict]) -> pl.DataFrame:
    if not dados_json:
        return pl.DataFrame()
    return pl.json_normalize(dados_json)


def _processar_colunas_intraday(df: pl.DataFrame) -> pl.DataFrame:
    mapa_renomeacao = _mapa_renomeacao_intraday()
    colunas_disponiveis = [col for col in mapa_renomeacao if col in df.columns]
    tipos_disponiveis = pl.Schema(
        {col: TIPOS_INTRADAY[col] for col in colunas_disponiveis}
    )
    return (
        df.select(colunas_disponiveis)
        .cast(tipos_disponiveis, strict=False)
        .rename(mapa_renomeacao, strict=False)
    )


def fetch_intraday_derivatives(contract_code: str) -> pl.DataFrame:
    """Busca cotações intraday brutas de derivativos da B3.

    Faz a chamada ao endpoint ``DerivativeQuotation`` e devolve um DataFrame
    padronizado, sem enriquecimento de regra de negócio.

    As colunas de cotação e limites são retornadas com sufixo ``Value``.
    A fonte intraday da B3 opera com atraso aproximado de 15 minutos.
    Filtros por mercado (ex.: apenas ``FUT``), normalização semântica
    (``Rate``/``Price``) e cálculos derivados devem ser feitos no módulo
    consumidor.

    Args:
        contract_code: Código base do derivativo na B3.

    Returns:
        DataFrame Polars com o payload normalizado da API.
    """
    dados_json = _buscar_json_intraday(contract_code)
    if not dados_json:
        return pl.DataFrame()

    return (
        _converter_json_intraday(dados_json)
        .pipe(_processar_colunas_intraday)
        .drop_nulls(subset=["ExpirationDate"])
        .sort("MarketCode", "TickerSymbol")
    )
