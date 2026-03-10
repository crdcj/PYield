import datetime as dt
import importlib
import math
from pathlib import Path

import polars as pl

derivatives_mod = importlib.import_module("pyield.b3.intraday_derivatives")
futures_intraday_mod = importlib.import_module("pyield.b3.futures.intraday")

DIRETORIO_DADOS = Path(__file__).parent / "data"
CAMINHO_CSV = DIRETORIO_DADOS / "derivatives_intraday_20260309.csv"
DATA_REFERENCIA = dt.date(2026, 3, 9)
HORARIO_REFERENCIA = dt.datetime(2026, 3, 9, 12, 0)
TOTAL_LINHAS_DOL = 4643
TOTAL_LINHAS_FUTURO_SIMPLES = 48
TAMANHO_TICKER_FUTURO = 6


def _carregar_csv_bruto() -> pl.DataFrame:
    return pl.read_csv(CAMINHO_CSV)


def _valor_nulo(valor) -> bool:
    return valor is None or (isinstance(valor, float) and math.isnan(valor))


def _payload_api_mock(codigo_contrato: str) -> list[dict]:
    df = _carregar_csv_bruto().filter(pl.col("query_code") == codigo_contrato)
    registros = []
    for linha in df.iter_rows(named=True):
        payload = {
            "symb": linha["symb"],
            "desc": linha["desc"],
            "asset": {
                "code": linha["asset.code"],
                "AsstSummry": {
                    "mtrtyCode": linha["asset.AsstSummry.mtrtyCode"],
                },
            },
            "mkt": {"cd": linha["mkt.cd"]},
            "SctyQtn": {},
        }

        mapa_cotacao = {
            "SctyQtn.bottomLmtPric": "bottomLmtPric",
            "SctyQtn.prvsDayAdjstmntPric": "prvsDayAdjstmntPric",
            "SctyQtn.topLmtPric": "topLmtPric",
            "SctyQtn.opngPric": "opngPric",
            "SctyQtn.minPric": "minPric",
            "SctyQtn.maxPric": "maxPric",
            "SctyQtn.avrgPric": "avrgPric",
            "SctyQtn.curPrc": "curPrc",
            "SctyQtn.exrcPric": "exrcPric",
        }
        mapa_resumo = {
            "asset.AsstSummry.grssAmt": "grssAmt",
            "asset.AsstSummry.opnCtrcts": "opnCtrcts",
            "asset.AsstSummry.tradQty": "tradQty",
            "asset.AsstSummry.traddCtrctsQty": "traddCtrctsQty",
        }
        for coluna_csv, chave_api in mapa_cotacao.items():
            valor = linha.get(coluna_csv)
            if not _valor_nulo(valor):
                payload["SctyQtn"][chave_api] = valor

        for coluna_csv, chave_api in mapa_resumo.items():
            valor = linha.get(coluna_csv)
            if not _valor_nulo(valor):
                payload["asset"]["AsstSummry"][chave_api] = valor

        for coluna_csv, caminho in {
            "buyOffer.price": ("buyOffer", "price"),
            "sellOffer.price": ("sellOffer", "price"),
        }.items():
            valor = linha.get(coluna_csv)
            if _valor_nulo(valor):
                continue
            bloco, chave = caminho
            payload[bloco] = {chave: valor}

        valor_lado = linha.get("asset.SdTpCd.desc")
        if not _valor_nulo(valor_lado):
            payload["asset"]["SdTpCd"] = {"desc": valor_lado}

        registros.append(payload)

    return registros


def _df_bruto_normalizado(codigo_contrato: str) -> pl.DataFrame:
    monkeypatched = _payload_api_mock(codigo_contrato)
    return (
        derivatives_mod._converter_json_intraday(monkeypatched)
        .pipe(derivatives_mod._processar_colunas_intraday)
        .drop_nulls(subset=["ExpirationDate"])
        .sort(["MarketCode", "TickerSymbol"])
    )


def _buscar_json_intraday_mock(codigo_contrato: str) -> list[dict]:
    return _payload_api_mock(codigo_contrato)


def _fetch_intraday_derivatives_mock(codigo_contrato: str) -> pl.DataFrame:
    return _df_bruto_normalizado(codigo_contrato)


def _data_referencia_mock() -> dt.date:
    return DATA_REFERENCIA


def _horario_referencia_mock() -> dt.datetime:
    return HORARIO_REFERENCIA


def test_fetch_intraday_derivatives_preserva_payload_misto(monkeypatch):
    """Módulo bruto deve preservar mercados mistos do payload do dia."""
    monkeypatch.setattr(
        derivatives_mod,
        "_buscar_json_intraday",
        _buscar_json_intraday_mock,
    )

    resultado = derivatives_mod.fetch_intraday_derivatives("DOL")

    assert resultado.height == TOTAL_LINHAS_DOL
    assert resultado["MarketCode"].unique().sort().to_list() == [
        "FUT",
        "OPTEXER",
        "SOPT",
        "SPOT",
    ]
    assert resultado["ExpirationDate"].null_count() == 0


def test_fetch_intraday_derivatives_suporta_colunas_opcionais_ausentes(monkeypatch):
    """Módulo bruto não deve quebrar quando o payload não tem book de ofertas."""
    monkeypatch.setattr(
        derivatives_mod,
        "_buscar_json_intraday",
        _buscar_json_intraday_mock,
    )

    resultado = derivatives_mod.fetch_intraday_derivatives("DDI")

    assert resultado.height == TOTAL_LINHAS_FUTURO_SIMPLES
    assert "BuyOfferValue" not in resultado.columns
    assert "SellOfferValue" not in resultado.columns


def test_fetch_intraday_derivatives_nao_descarta_fro_sem_curprc(monkeypatch):
    """FRO deve continuar válido mesmo sem coluna curPrc no payload."""
    monkeypatch.setattr(
        derivatives_mod,
        "_buscar_json_intraday",
        _buscar_json_intraday_mock,
    )

    resultado = derivatives_mod.fetch_intraday_derivatives("FRO")

    assert resultado.height == TOTAL_LINHAS_FUTURO_SIMPLES
    assert "LastValue" not in resultado.columns
    assert resultado["MarketCode"].unique().to_list() == ["FUT"]


def test_futures_intraday_filtra_apenas_futuros(monkeypatch):
    """Camada de futures deve consumir o bruto e manter apenas FUT."""
    monkeypatch.setattr(
        futures_intraday_mod,
        "fetch_intraday_derivatives",
        _fetch_intraday_derivatives_mock,
    )
    monkeypatch.setattr(
        futures_intraday_mod.bday, "last_business_day", _data_referencia_mock
    )
    monkeypatch.setattr(futures_intraday_mod.clock, "now", _horario_referencia_mock)

    resultado = futures_intraday_mod.intraday("DOL")

    esperados = (
        _carregar_csv_bruto()
        .filter(pl.col("query_code") == "DOL")
        .filter(pl.col("mkt.cd") == "FUT")
        .get_column("symb")
        .sort()
        .to_list()
    )

    assert resultado.height == len(esperados)
    assert resultado["TickerSymbol"].sort().to_list() == esperados
    assert all(
        len(ticker) == TAMANHO_TICKER_FUTURO
        for ticker in resultado["TickerSymbol"].to_list()
    )
