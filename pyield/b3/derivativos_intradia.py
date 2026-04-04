"""Cotações intradia de derivativos da B3 (endpoint DerivativeQuotation).

Exemplo de chamada à API:
    https://cotacao.b3.com.br/mds/api/v1/DerivativeQuotation/DI1

Exemplo de resposta JSON (resumido):
    {"Scty": [
        {"symb": "DI1J30",
         "desc": "DI DE 1 DIA",
         "asset": {"code": "DI1",
                   "AsstSummry": {"mtrtyCode": "2030-04-01",
                                  "opnCtrcts": 64037,
                                  "grssAmt": 5272867.79,
                                  "tradQty": 36,
                                  "traddCtrctsQty": 89}},
         "mkt": {"cd": "FUT"},
         "SctyQtn": {"curPrc": 14.105, "opngPric": 14.22,
                     "minPric": 14.02, "maxPric": 14.22,
                     "avrgPric": 14.1013,
                     "prvsDayAdjstmntPric": 14.127,
                     "bottomLmtPric": 13.16,
                     "topLmtPric": 15.385},
         "buyOffer": {"price": 14.105},
         "sellOffer": {"price": 14.13}},
        ...
    ]}
"""

import datetime as dt
import logging

import polars as pl
import requests

from pyield import relogio
from pyield._internal.cache import ttl_cache
from pyield._internal.retry import retry_padrao

URL_BASE_INTRADIA = "https://cotacao.b3.com.br/mds/api/v1/DerivativeQuotation"

logger = logging.getLogger(__name__)

# --- Mapeamento de Colunas ---

# Estrutura: (nome_json_normalize, nome_canonico, tipo_polars)
# Colunas numéricas de cotação/limites usam prefixo "preco_" no output bruto.
# Para contratos cotados por taxa, a conversão para "taxa_" é responsabilidade
# do módulo consumidor.
# Colunas opcionais (ofertas, opções) ficam no final; são incluídas somente
# quando presentes no payload.
COLUNAS_INTRADIA: list[tuple[str, str, type[pl.DataType]]] = [
    ("symb", "codigo_negociacao", pl.String),
    ("desc", "descricao", pl.String),
    ("asset.code", "codigo_ativo", pl.String),
    ("mkt.cd", "codigo_mercado", pl.String),
    ("asset.AsstSummry.mtrtyCode", "data_vencimento", pl.Date),
    ("SctyQtn.prvsDayAdjstmntPric", "preco_ajuste_anterior", pl.Float64),
    ("SctyQtn.bottomLmtPric", "preco_limite_minimo", pl.Float64),
    ("SctyQtn.topLmtPric", "preco_limite_maximo", pl.Float64),
    ("SctyQtn.opngPric", "preco_abertura", pl.Float64),
    ("SctyQtn.minPric", "preco_minimo", pl.Float64),
    ("SctyQtn.maxPric", "preco_maximo", pl.Float64),
    ("SctyQtn.avrgPric", "preco_medio", pl.Float64),
    ("SctyQtn.curPrc", "preco_ultimo", pl.Float64),
    ("SctyQtn.exrcPric", "preco_exercicio", pl.Float64),
    ("asset.AsstSummry.opnCtrcts", "contratos_abertos", pl.Int64),
    ("asset.AsstSummry.grssAmt", "volume_financeiro", pl.Float64),
    ("asset.AsstSummry.tradQty", "numero_negocios", pl.Int64),
    ("asset.AsstSummry.traddCtrctsQty", "volume_negociado", pl.Int64),
    ("buyOffer.price", "preco_oferta_compra", pl.Float64),
    ("sellOffer.price", "preco_oferta_venda", pl.Float64),
    ("asset.SdTpCd.desc", "tipo_lado", pl.String),
]

# Mapa de tipos para cast inicial usando os nomes do json_normalize.
MAPEAMENTO = {orig: novo for orig, novo, _ in COLUNAS_INTRADIA}
TIPOS = {orig: tipo for orig, _, tipo in COLUNAS_INTRADIA}


@ttl_cache(ttl=10)
@retry_padrao
def _buscar_json_intradia(codigo_contrato: str) -> list[dict]:
    url = f"{URL_BASE_INTRADIA}/{codigo_contrato}"
    cabecalhos = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"  # noqa: E501
    }
    resposta = requests.get(url, headers=cabecalhos, timeout=10)
    resposta.raise_for_status()
    resposta.encoding = "utf-8"

    if "Quotation not available" in resposta.text:
        return []

    return resposta.json()["Scty"]


def _converter_json_intradia(dados_json: list[dict]) -> pl.DataFrame:
    if not dados_json:
        return pl.DataFrame()
    return pl.json_normalize(dados_json)


def _processar_colunas_intradia(df: pl.DataFrame) -> pl.DataFrame:
    colunas_disponiveis = [col for col in MAPEAMENTO if col in df.columns]
    tipos_disponiveis = pl.Schema({col: TIPOS[col] for col in colunas_disponiveis})
    return (
        df.select(colunas_disponiveis)
        .cast(tipos_disponiveis, strict=False)
        .rename(MAPEAMENTO, strict=False)
    )


def derivativo_intradia(codigo_contrato: str) -> pl.DataFrame:
    """Busca cotações intradia brutas de derivativos da B3.

    Faz a chamada ao endpoint ``DerivativeQuotation`` e devolve um DataFrame
    padronizado, sem enriquecimento de regra de negócio.

    As colunas de cotação e limites são retornadas com prefixo ``preco_``.
    A fonte intradia da B3 opera com atraso aproximado de 15 minutos.
    Para contratos cotados por taxa, a conversão para ``taxa_`` e cálculos
    derivados devem ser feitos no módulo consumidor.

    Args:
        codigo_contrato: Código base do derivativo na B3
            (ex.: ``DI1``, ``DOL``, ``DAP``, ``DDI``, ``FRC``, ``FRO``, ``IND``).

    Returns:
        DataFrame Polars com o payload normalizado da API.

    Output Columns:
        * codigo_negociacao (String): código de negociação na B3.
        * descricao (String): descrição do instrumento.
        * codigo_ativo (String): código do ativo base.
        * codigo_mercado (String): código do mercado (ex.: FUT, OPTEXER, SOPT, SPOT).
        * data_vencimento (Date): data de vencimento do contrato.
        * preco_ajuste_anterior (Float64): preço de ajuste do dia anterior.
        * preco_limite_minimo (Float64): limite mínimo de variação.
        * preco_limite_maximo (Float64): limite máximo de variação.
        * preco_abertura (Float64): preço de abertura.
        * preco_minimo (Float64): preço mínimo negociado.
        * preco_maximo (Float64): preço máximo negociado.
        * preco_medio (Float64): preço médio negociado.
        * preco_ultimo (Float64): último preço negociado.
        * preco_exercicio (Float64): preço de exercício (opções).
        * contratos_abertos (Int64): contratos em aberto.
        * volume_financeiro (Float64): volume financeiro bruto.
        * numero_negocios (Int64): número de negócios.
        * volume_negociado (Int64): quantidade de contratos negociados.
        * preco_oferta_compra (Float64): melhor oferta de compra (opcional).
        * preco_oferta_venda (Float64): melhor oferta de venda (opcional).
        * tipo_lado (String): tipo de lado (opcional).
        * horario_referencia (Time): horário aproximado a que os
          dados se referem. A fonte intradia da B3 possui atraso de
          ~15 min; este valor é calculado subtraindo esse atraso do
          horário da consulta.
    """
    dados_json = _buscar_json_intradia(codigo_contrato)
    if not dados_json:
        return pl.DataFrame()

    df = _converter_json_intradia(dados_json)
    df = _processar_colunas_intradia(df)
    horario = (relogio.agora() - dt.timedelta(minutes=15)).time()
    df = df.with_columns(horario_referencia=horario)
    return df.sort("codigo_negociacao")
